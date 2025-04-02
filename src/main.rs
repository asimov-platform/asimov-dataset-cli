// This is free and unencumbered software released into the public domain.

#![deny(unsafe_code)]

mod feature;

use std::{
    collections::VecDeque,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    time::{Duration, Instant},
};

use asimov_dataset_cli::{prepare::PrepareStatsReport, publish::PublishStatsReport};
use clientele::{
    StandardOptions,
    SysexitsError::*,
    crates::clap::{CommandFactory, Parser, Subcommand},
    exit,
};
use crossterm::event;
use near_api::AccountId;
use ratatui::TerminalOptions;
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
        Some(Command::Prepare(PrepareCommand { files })) => {
            let start = std::time::Instant::now();

            let (tx, rx) = crossbeam::channel::unbounded();

            let mut terminal = ratatui::init_with_options(TerminalOptions {
                viewport: ratatui::Viewport::Inline(30),
            });

            std::thread::scope(|s| {
                let files: Vec<PathBuf> = files
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

                use asimov_dataset_cli::ui;
                let ui_state = ui::Prepare {
                    total_bytes,
                    queued_files,
                    ..Default::default()
                };

                s.spawn({
                    let tx = tx.clone();
                    move || {
                        let tick_rate = Duration::from_millis(200);
                        let mut last_tick = Instant::now();
                        loop {
                            // poll for tick rate duration, if no events, sent tick event.
                            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
                            if event::poll(timeout).unwrap() {
                                match event::read().unwrap() {
                                    event::Event::Key(key) => {
                                        tx.send(ui::Event::Input(key)).unwrap()
                                    }
                                    event::Event::Resize(_, _) => {
                                        tx.send(ui::Event::Resize).unwrap()
                                    }
                                    _ => {}
                                };
                            }
                            if last_tick.elapsed() >= tick_rate {
                                tx.send(ui::Event::Tick).unwrap();
                                last_tick = Instant::now();
                            }
                        }
                    }
                });

                s.spawn(move || ui::run_prepare(&mut terminal, ui_state, rx));

                let report = Some(asimov_dataset_cli::prepare::PrepareStatsReport { tx });

                let _files = asimov_dataset_cli::prepare::prepare_datasets(&files, report)
                    .expect("`prepare` failed");
            });

            ratatui::restore();

            debug!(
                duration = ?std::time::Instant::now().duration_since(start),
                "Prepare finished"
            );
        }
        Some(Command::Publish(PublishCommand {
            repository,
            files,
            network,
        })) => {
            let files: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();

            let repository: AccountId = repository.parse().expect("invalid repository");

            let network_config = match network.as_str() {
                "mainnet" => near_api::NetworkConfig::mainnet(),
                "testnet" => near_api::NetworkConfig::testnet(),
                _ => {
                    print!("Unknown network name: {}", network);
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

            // let (tx, rx) = crossbeam::channel::unbounded();

            // asimov_dataset_cli::publish::publish_datasets(
            //     repository,
            //     signer,
            //     &network_config,
            //     &files,
            //     ,
            // )
            // .await
            // .expect("`publish` failed");

            let (tx, rx) = crossbeam::channel::unbounded();

            let mut terminal = ratatui::init_with_options(TerminalOptions {
                viewport: ratatui::Viewport::Inline(30),
            });

            let report = Some((
                PrepareStatsReport { tx: tx.clone() },
                PublishStatsReport { tx: tx.clone() },
            ));

            let publish_task = tokio::task::spawn({
                let files = files.clone();
                async move {
                    asimov_dataset_cli::publish::publish_datasets(
                        repository,
                        signer,
                        &network_config,
                        &files,
                        report,
                    )
                    .await
                    .map_err(|_err| ())
                }
            });

            std::thread::scope(|s| {
                let files: Vec<PathBuf> = files
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

                use asimov_dataset_cli::ui;
                let ui_state = ui::Publish {
                    queued_files: queued_files.iter().map(|f| f.0.clone()).collect(),
                    prepare: Some(ui::Prepare {
                        total_bytes,
                        queued_files,
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                s.spawn({
                    let tx = tx.clone();
                    move || {
                        let tick_rate = Duration::from_millis(200);
                        let mut last_tick = Instant::now();
                        loop {
                            // poll for tick rate duration, if no events, sent tick event.
                            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
                            if event::poll(timeout).unwrap() {
                                match event::read().unwrap() {
                                    event::Event::Key(key) => {
                                        tx.send(ui::Event::Input(key)).unwrap()
                                    }
                                    event::Event::Resize(_, _) => {
                                        tx.send(ui::Event::Resize).unwrap()
                                    }
                                    _ => {}
                                };
                            }
                            if last_tick.elapsed() >= tick_rate {
                                tx.send(ui::Event::Tick).unwrap();
                                last_tick = Instant::now();
                            }
                        }
                    }
                });

                s.spawn(move || ui::run_publish(&mut terminal, ui_state, rx));
            });

            let _res = publish_task.await.expect("`prepare` failed");

            ratatui::restore();
        }
        None => todo!(),
    }
}
