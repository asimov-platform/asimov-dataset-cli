// This is free and unencumbered software released into the public domain.

#![deny(unsafe_code)]

mod feature;

use std::{collections::VecDeque, os::unix::fs::MetadataExt, path::PathBuf};

use asimov_dataset_cli::{
    prepare::PrepareStatsReport,
    publish::{self, PublishStatsReport},
    ui,
};
use clientele::{
    StandardOptions,
    SysexitsError::*,
    crates::clap::{Args, CommandFactory, Parser, Subcommand},
    exit,
};
use near_api::AccountId;
use ratatui::TerminalOptions;
use tokio::task::JoinSet;
use tracing::debug;

/// ASIMOV Dataset Command-Line Interface (CLI)
#[derive(Debug, Parser)]
#[command(name = "asimov-dataset", long_about)]
#[command(allow_external_subcommands = true)]
#[command(arg_required_else_help = true)]
#[command(disable_help_flag = true)]
#[command(disable_help_subcommand = true)]
struct Options {
    #[clap(flatten)]
    flags: StandardOptions,

    #[clap(short = 'h', long, help = "Print help (see more with '--help')")]
    help: bool,

    #[clap(subcommand)]
    command: Option<Command>,
}

/// Commands for the ASIMOV CLI
#[derive(Debug, Subcommand)]
enum Command {
    /// Prepare dataset files
    Prepare(PrepareCommand),
    /// Publish dataset files
    Publish(PublishCommand),
}

/// Options for the prepare command
#[derive(Debug, Args)]
struct PrepareCommand {
    /// Files to prepare
    #[arg(required = true)]
    files: Vec<String>,
}

/// Options for the publish command
#[derive(Debug, Args)]
struct PublishCommand {
    /// Network on which to publish. Either `mainnet` or `testnet`
    #[arg(long)]
    network: String,

    /// Repository where to publish the dataset files
    #[arg(required = true)]
    repository: String,
    /// Files to publish
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

    // Print help, if requested:
    if options.help {
        Options::command().print_long_help().unwrap();
        exit(EX_OK);
    }

    match options.command {
        Some(Command::Prepare(cmd)) => cmd.run(options.flags.verbose > 0).await,
        Some(Command::Publish(cmd)) => cmd.run(options.flags.verbose > 0).await,
        None => {}
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

        set.spawn(async move {
            asimov_dataset_cli::prepare::prepare_datasets(files.into_iter(), None, report)
                .await
                .expect("`prepare` failed");
        });

        drop(event_tx);

        ui::run_prepare(&mut terminal, verbose, ui_state, ui_event_rx, event_rx).unwrap();

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

        let repository: AccountId = self.repository.parse().expect("invalid repository");

        let network_config = match self.network.as_str() {
            "mainnet" => near_api::NetworkConfig::mainnet(),
            "testnet" => near_api::NetworkConfig::testnet(),
            _ => {
                print!("Unknown network name: {}", self.network);
                exit(EX_CONFIG);
            }
        };

        let near_signer: AccountId = std::env::var("NEAR_SIGNER")
            .expect("need NEAR_SIGNER")
            .parse()
            .expect("invalid account name in NEAR_SIGNER");

        let signer = near_api::signer::keystore::KeystoreSigner::search_for_keys(
            near_signer,
            &network_config,
        )
        .await
        .expect("failed to get key in keystore");
        let signer = near_api::Signer::new(signer).unwrap();

        let mut set = JoinSet::new();

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
                let tx = event_tx.clone();
                let unprepared_files = unprepared_files.clone().into_iter();
                async move {
                    asimov_dataset_cli::prepare::prepare_datasets(
                        unprepared_files,
                        Some(files_tx),
                        Some(PrepareStatsReport { tx }),
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
                    repository,
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

        ui::run_publish(&mut terminal, verbose, ui_state, ui_event_rx, event_rx).unwrap();

        let _ = set.join_all().await;

        ratatui::restore();
    }
}
