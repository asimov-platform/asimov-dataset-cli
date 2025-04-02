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
    crates::clap::{CommandFactory, Parser, Subcommand},
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
#[derive(Debug, Parser)]
struct PrepareCommand {
    /// Files to prepare
    #[arg(required = true)]
    files: Vec<String>,
}

/// Options for the publish command
#[derive(Debug, Parser)]
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
        Some(Command::Prepare(cmd)) => cmd.run().await,
        Some(Command::Publish(cmd)) => cmd.run().await,
        None => todo!(),
    }
    .unwrap();
}

impl PrepareCommand {
    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        let mut terminal = ratatui::init_with_options(TerminalOptions {
            viewport: ratatui::Viewport::Inline(30),
        });

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

        let total_bytes = queued_files.iter().fold(0, |acc, (_, size)| acc + size);

        let ui_state = ui::Prepare {
            total_bytes,
            queued_files,
            ..Default::default()
        };

        let mut set = JoinSet::new();

        set.spawn_blocking({
            let tx = event_tx.clone();
            move || ui::listen_input(&tx)
        });

        let report = Some(asimov_dataset_cli::prepare::PrepareStatsReport { tx: event_tx });

        set.spawn(async move {
            asimov_dataset_cli::prepare::prepare_datasets(files.into_iter(), None, report)
                .await
                .expect("`prepare` failed");
        });

        ui::run_prepare(&mut terminal, ui_state, event_rx).unwrap();

        // let _ = set.join_all().await;

        ratatui::try_restore().unwrap();

        debug!(
            duration = ?std::time::Instant::now().duration_since(start),
            "Prepare finished"
        );

        Ok(())
    }
}

impl PublishCommand {
    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
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
                exit(EX_OK);
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

        let (event_tx, event_rx) = crossbeam::channel::unbounded();

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

        let (files_tx, files_rx) = crossbeam::channel::unbounded();

        if !unprepared_files.is_empty() {
            set.spawn({
                let tx = event_tx.clone();
                let unprepared_files = unprepared_files.clone().into_iter();
                async move {
                    asimov_dataset_cli::prepare::prepare_datasets(
                        unprepared_files,
                        Some(files_tx),
                        Some(PrepareStatsReport { tx: tx.clone() }),
                    )
                    .await
                    .unwrap();
                }
            });
        } else {
            drop(files_tx);
        }

        set.spawn({
            let tx = event_tx.clone();
            let prepared_files = prepared_files.clone().into_iter();
            async move {
                asimov_dataset_cli::publish::publish_datasets(
                    repository,
                    signer,
                    &network_config,
                    prepared_files.chain(files_rx.iter()),
                    Some(PublishStatsReport { tx }),
                )
                .await
                .unwrap();
            }
        });

        let unprepared_files: VecDeque<(PathBuf, usize)> = unprepared_files
            .iter()
            .cloned()
            .map(|file| {
                let size = std::fs::metadata(&file).unwrap().size() as usize;
                (file, size)
            })
            .collect();

        set.spawn_blocking({
            let tx = event_tx.clone();
            move || ui::listen_input(&tx)
        });

        let mut terminal = ratatui::init_with_options(TerminalOptions {
            viewport: ratatui::Viewport::Inline(30),
        });
        let prepare_state = if unprepared_files.is_empty() {
            None
        } else {
            let total_bytes = unprepared_files.iter().fold(0, |acc, (_, size)| acc + size);
            Some(ui::Prepare {
                total_bytes,
                queued_files: unprepared_files,
                ..Default::default()
            })
        };
        let total_bytes = prepared_files.iter().fold(0, |acc, (_, size)| acc + size);
        let ui_state = ui::Publish {
            queued_files: prepared_files,
            total_bytes,
            prepare: prepare_state,
            ..Default::default()
        };

        ui::run_publish(&mut terminal, ui_state, event_rx).unwrap();

        // let _ = set.join_all().await;

        ratatui::try_restore().unwrap();

        Ok(())
    }
}
