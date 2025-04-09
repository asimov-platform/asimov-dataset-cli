// This is free and unencumbered software released into the public domain.

#![deny(unsafe_code)]

mod feature;

use std::{collections::VecDeque, os::unix::fs::MetadataExt, path::PathBuf};

use asimov_dataset_cli::{
    context,
    prepare::PrepareStatsReport,
    publish::{self, PublishStatsReport},
    ui,
};
use clientele::{
    StandardOptions,
    SysexitsError::*,
    crates::clap::{Parser, Subcommand},
    exit,
};
use near_api::AccountId;
use ratatui::TerminalOptions;
use tokio::task::JoinSet;
use tracing::debug;

/// ASIMOV Dataset Command-Line Interface (CLI)
#[derive(Debug, Parser)]
#[command(name = "asimov-dataset", about, long_about)]
struct Options {
    #[clap(flatten)]
    flags: StandardOptions,

    #[clap(subcommand)]
    command: Command,
}

const PUBLISH_USAGE: &str = "asimov-dataset publish [OPTIONS] <REPOSITORY> <FILES>...\n       \
                             asimov-dataset publish your-repo.near ./data.ttl\n       \
                             asimov-dataset publish --network testnet your-repo.testnet ./data1.ttl ./data2.nt\n       \
                             asimov-dataset publish --signer other.testnet your-repo.testnet ./data.rdfb\n       \
                             asimov-dataset publish your-repo.near ./prepared/*.rdfb ./raw/*.ttl";

const PREPARE_USAGE: &str = "asimov-dataset prepare [OPTIONS] <FILES>...\n       \
                             asimov-dataset prepare data.ttl\n       \
                             asimov-dataset prepare ./data1.ttl ./data2.nt ./data3.n3\n       \
                             asimov-dataset prepare ./dataset/*.ttl";

/// Commands for the ASIMOV CLI
#[derive(Debug, Subcommand)]
enum Command {
    /// Publish dataset files to an on-chain repository contract.
    ///
    /// This command can publish both raw RDF data files and pre-prepared RDFB files.
    /// Raw RDF files will be automatically prepared before publishing.
    #[command(override_usage = PUBLISH_USAGE)]
    Publish(PublishCommand),

    /// Prepare dataset files without publishing.
    ///
    /// This command processes RDF data files and converts them into a format
    /// ready for publishing to the ASIMOV network.
    #[command(override_usage = PREPARE_USAGE)]
    Prepare(PrepareCommand),
}

/// Options for the prepare command
#[derive(Debug, Parser)]
struct PrepareCommand {
    /// Files to prepare. Supported formats: n3, nt, nq, rdf, ttl, trig.
    ///
    /// Each file should contain valid RDF data in one of the supported formats.
    /// The format is determined by the file extension.
    #[arg(required = true)]
    files: Vec<String>,
}

/// Options for the publish command
#[derive(Debug, Parser)]
struct PublishCommand {
    /// Network on which to publish. Either `mainnet` or `testnet`.
    ///
    /// If not provided, the network will be inferred from the repository name
    /// (`.near` suffix for mainnet, `.testnet` suffix for testnet).
    #[arg(long)]
    network: Option<String>,

    /// Account that signs batches sent to the repository.
    ///
    /// By default, the repository account is used for signing.
    /// Can also be set via the NEAR_SIGNER environment variable.
    #[arg(long)]
    signer: Option<AccountId>,

    /// Optional dataset name in the repository.
    #[arg(long)]
    dataset: Option<String>,

    /// Repository is the on-chain account address to which the data is published.
    #[arg(required = true)]
    repository: AccountId,

    /// Files to publish.
    ///
    /// Supports both:
    ///
    /// - Raw RDF files (formats: n3, nt, nq, rdf, ttl, trig) which will be prepared automatically
    ///
    /// - Pre-prepared RDFB files from previous 'prepare' command runs
    #[arg(required = true)]
    files: Vec<String>,
}

#[tokio::main]
pub async fn main() {
    // Load environment variables from `.env`:
    let _ = clientele::dotenv();

    // tracing_subscriber::fmt::init();

    // Expand wildcards and @argfiles:
    let Ok(args) = clientele::args_os() else {
        exit(EX_USAGE);
    };

    // Parse command-line options:
    let options = Options::parse_from(&args);

    // Print the version, if requested:
    if options.flags.version {
        println!("ASIMOV {}", env!("CARGO_PKG_VERSION"));
        exit(EX_OK);
    }

    // Print the license, if requested:
    if options.flags.license {
        print!("{}", include_str!("../UNLICENSE"));
        exit(EX_OK);
    }

    match options.command {
        Command::Prepare(cmd) => cmd.run(options.flags.verbose > 0).await,
        Command::Publish(cmd) => cmd.run(options.flags.verbose > 0).await,
    };

    println!("\n");
}

impl PrepareCommand {
    async fn run(self, verbose: bool) {
        let start = std::time::Instant::now();

        let (ui_event_tx, ui_event_rx) = crossbeam::channel::unbounded();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        let files: Vec<PathBuf> = self
            .files
            .iter()
            .map(PathBuf::from)
            .filter(|file| std::fs::exists(file).unwrap_or(false))
            .collect();
        let queued_files: VecDeque<(PathBuf, usize)> = files
            .iter()
            .cloned()
            .map(|file| {
                let size = std::fs::metadata(&file).unwrap().size() as usize;
                (file, size)
            })
            .collect();

        let total_bytes = queued_files.iter().map(|(_, size)| size).sum();

        let ui_state = ui::Prepare {
            total_bytes,
            queued_files,
            ..Default::default()
        };

        let mut set = JoinSet::new();

        let (ctx, cancel) = context::new_cancel_context();

        set.spawn_blocking(move || ui::listen_input(&ui_event_tx));

        let report = Some(asimov_dataset_cli::prepare::PrepareStatsReport {
            tx: event_tx.clone(),
        });

        let mut terminal = ratatui::init_with_options(TerminalOptions {
            viewport: if !verbose {
                ratatui::Viewport::Inline(2)
            } else {
                ratatui::Viewport::Inline(15)
            },
        });

        let (files_tx, files_rx) = crossbeam::channel::unbounded();

        set.spawn({
            let ctx = ctx.clone();
            async move {
                asimov_dataset_cli::prepare::prepare_datasets(
                    ctx,
                    files.into_iter(),
                    files_tx,
                    report,
                )
                .await
                .expect("`prepare` failed");
            }
        });

        drop(event_tx);

        ui::run_prepare(
            &mut terminal,
            verbose,
            ui_state,
            ui_event_rx,
            event_rx,
            || cancel.cancel(),
        )
        .unwrap();

        drop(files_rx);

        let _ = set.join_all().await;

        ratatui::restore();

        debug!(
            duration = ?std::time::Instant::now().duration_since(start),
            "Prepare finished"
        );
    }
}

impl PublishCommand {
    async fn run(self, verbose: bool) {
        let files: Vec<PathBuf> = self
            .files
            .iter()
            .map(PathBuf::from)
            .filter(|file| std::fs::exists(file).unwrap_or(false))
            .collect();

        let network_config = match self.network.as_deref() {
            Some("mainnet") => near_api::NetworkConfig::mainnet(),
            Some("testnet") => near_api::NetworkConfig::testnet(),
            None => {
                // infer from repository accountid
                match self.repository.as_str().split('.').next_back() {
                    Some("near") => near_api::NetworkConfig::mainnet(),
                    Some("testnet") => near_api::NetworkConfig::testnet(),
                    _ => {
                        eprintln!("Unable to infer network, please provide --network");
                        exit(EX_CONFIG);
                    }
                }
            }
            Some(network) => {
                eprintln!("Unknown network name: {}", network);
                exit(EX_CONFIG);
            }
        };

        let near_signer = {
            if let Some(signer) = self.signer {
                signer
            } else {
                match std::env::var("NEAR_SIGNER") {
                    Ok(signer) => signer
                        .parse()
                        .expect("invalid account address in NEAR_SIGNER"),
                    Err(std::env::VarError::NotPresent) => self.repository.clone(),
                    Err(err) => {
                        eprintln!("{err}");
                        exit(EX_CONFIG);
                    }
                }
            }
        };

        let signer = near_api::signer::keystore::KeystoreSigner::search_for_keys(
            near_signer,
            &network_config,
        )
        .await
        .expect("failed to get key in keystore");
        let signer = near_api::Signer::new(signer).unwrap();

        let mut set = JoinSet::new();

        let (ctx, cancel) = context::new_cancel_context();

        let (prepared_files, unprepared_files) = publish::split_prepared_files(&files);

        let prepared_files: VecDeque<(PathBuf, usize)> = prepared_files
            .iter()
            .cloned()
            .map(|file| {
                let size = std::fs::metadata(&file).unwrap().size() as usize;
                (file, size)
            })
            .collect();

        let (event_tx, event_rx) = crossbeam::channel::unbounded();
        let (files_tx, files_rx) = crossbeam::channel::unbounded();

        if !unprepared_files.is_empty() {
            set.spawn({
                let ctx = ctx.clone();
                let tx = event_tx.clone();
                let unprepared_files = unprepared_files.clone().into_iter();
                let report = Some(PrepareStatsReport { tx });
                async move {
                    asimov_dataset_cli::prepare::prepare_datasets(
                        ctx,
                        unprepared_files,
                        files_tx,
                        report,
                    )
                    .await
                    .unwrap();
                }
            });
        } else {
            drop(files_tx);
        }

        let unprepared_files: VecDeque<(PathBuf, usize)> = unprepared_files
            .iter()
            .cloned()
            .map(|file| {
                let size = std::fs::metadata(&file).unwrap().size() as usize;
                (file, size)
            })
            .collect();

        let (ui_event_tx, ui_event_rx) = crossbeam::channel::unbounded();

        set.spawn_blocking(move || ui::listen_input(&ui_event_tx));

        let mut terminal = ratatui::init_with_options(TerminalOptions {
            viewport: if !verbose {
                ratatui::Viewport::Inline(4)
            } else {
                ratatui::Viewport::Inline(20)
            },
        });
        let prepare_state = if unprepared_files.is_empty() {
            None
        } else {
            let total_bytes = unprepared_files.iter().map(|(_, size)| size).sum();
            Some(ui::Prepare {
                total_bytes,
                queued_files: unprepared_files,
                ..Default::default()
            })
        };
        let total_bytes = prepared_files.iter().map(|(_, size)| size).sum();
        let ui_state = ui::Publish {
            queued_files: prepared_files.clone(),
            total_bytes,
            prepare: prepare_state,
            ..Default::default()
        };

        set.spawn({
            let tx = event_tx.clone();
            async move {
                asimov_dataset_cli::publish::publish_datasets(
                    ctx,
                    self.repository,
                    self.dataset,
                    signer,
                    &network_config,
                    prepared_files.into_iter().chain(files_rx.iter()),
                    Some(PublishStatsReport { tx }),
                )
                .await
                .unwrap();
            }
        });

        drop(event_tx);

        ui::run_publish(
            &mut terminal,
            verbose,
            ui_state,
            ui_event_rx,
            event_rx,
            || cancel.cancel(),
        )
        .unwrap();

        let _ = set.join_all().await;

        ratatui::restore();
    }
}
