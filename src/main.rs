// This is free and unencumbered software released into the public domain.

#![deny(unsafe_code)]

mod feature;

use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use asimov_dataset_cli::{
    context,
    prepare::PrepareStatsReport,
    publish::{self, PublishStatsReport},
    ui,
};
use clap::builder::{styling::AnsiColor, Styles};
use clientele::{
    crates::clap::{CommandFactory, Parser, Subcommand},
    exit, StandardOptions,
    SysexitsError::*,
};
use color_eyre::Section;
use eyre::{bail, eyre, Context, Result};
use near_api::{AccountId, NetworkConfig, Signer};
use tokio::task::JoinSet;
use tracing::debug;

/// ASIMOV Dataset Command-Line Interface (CLI)
#[derive(Debug, Parser)]
#[command(name = "asimov-dataset", about, long_about, styles=get_cli_styles())]
struct Options {
    #[clap(flatten)]
    flags: StandardOptions,

    #[clap(subcommand)]
    command: Option<Command>,
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
    /// This command can publish both raw RDF data files and pre-prepared RDF/Borsh files.
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
    /// Directory where prepared RDF/Borsh files will be stored.
    ///
    /// If not specified, a temporary directory will be created in the system's
    /// temp directory (e.g., /tmp/asimov-dataset/<pid>/).
    #[arg(short = 'o', long)]
    output_dir: Option<PathBuf>,

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
    #[arg(long, env = "NEAR_SIGNER")]
    signer: Option<AccountId>,

    /// Optional dataset name in the repository.
    #[arg(long)]
    dataset: Option<String>,

    /// Repository is the on-chain account address to which the data is published.
    #[arg(required = true)]
    repository: AccountId,

    /// Upload a simple contract at the repository address before uploading RDF data.
    #[arg(long)]
    upload_contract: bool,

    /// Files to publish.
    ///
    /// Supports both:
    ///
    /// - Raw RDF files (formats: n3, nt, nq, rdf, ttl, trig) which will be prepared automatically
    ///
    /// - Pre-prepared RDF/Borsh files from previous 'prepare' command runs
    #[arg(required = true)]
    files: Vec<String>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    color_eyre::install()?;

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

    let Some(command) = options.command else {
        Options::command().color(options.flags.color).print_help()?;
        exit(EX_USAGE);
    };

    match command {
        Command::Prepare(cmd) => cmd.run(options.flags.verbose).await,
        Command::Publish(cmd) => cmd.run(options.flags.verbose).await,
    }
}

impl PrepareCommand {
    async fn run(self, verbosity: u8) -> Result<()> {
        let start = std::time::Instant::now();

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
            .map(|file| (file.clone(), file_size(&file)))
            .collect();

        let total_bytes = queued_files.iter().map(|(_, size)| size).sum();

        let ui_state = ui::PrepareState {
            total_bytes,
            queued_files,
            ..Default::default()
        };

        let (files_tx, files_rx) = crossbeam::channel::unbounded();

        let dir = match self.output_dir {
            Some(dir) => dir,
            None => create_tmp_dir().wrap_err("Failed to create a temporary output directory")?,
        };
        assert!(
            std::fs::metadata(&dir)
                .unwrap_or_else(|err| {
                    eprintln!("Invalid output directory {:?}: {}", dir.display(), err);
                    exit(EX_IOERR);
                })
                .is_dir(),
            "{:?} is not a directory",
            dir.display()
        );

        let params = asimov_dataset_cli::prepare::ParamsBuilder::default()
            .files(files.into_iter())
            .files_tx(files_tx)
            .output_dir(dir.clone())
            .report(asimov_dataset_cli::prepare::PrepareStatsReport { tx: event_tx })
            .build()?;

        let mut set: JoinSet<Result<()>> = JoinSet::new();

        let (ctx, _cancel) = context::new_cancel_context();

        set.spawn({
            let ctx = ctx.clone();
            asimov_dataset_cli::prepare::prepare_datasets(ctx, params)
        });

        ui::run_prepare(verbosity, ui_state, event_rx)?;

        drop(files_rx); // for now we do nothing with these

        while let Some(join_result) = set.join_next().await {
            match join_result {
                Err(err) if err.is_cancelled() => (),
                Err(err) => panic!("{err}"),
                Ok(task_result) => task_result?,
            }
        }

        println!("Prepared RDF/Borsh files are in {}", dir.display());

        debug!(
            duration = ?std::time::Instant::now().duration_since(start),
            "Prepare finished"
        );

        Ok(())
    }
}

impl PublishCommand {
    async fn run(self, verbosity: u8) -> Result<()> {
        let network_config = match self.network.as_deref() {
            Some("mainnet") => near_api::NetworkConfig::mainnet(),
            Some("testnet") => near_api::NetworkConfig::testnet(),
            None => {
                // infer from repository accountid
                match self.repository.as_str().split('.').next_back() {
                    Some("near") => near_api::NetworkConfig::mainnet(),
                    Some("testnet") => near_api::NetworkConfig::testnet(),
                    _ => {
                        bail!("Unable to infer network, please provide --network");
                    }
                }
            }
            Some(network) => {
                bail!("Unknown network name: {}", network);
            }
        };

        let signer_id = if let Some(signer) = self.signer {
            signer
        } else {
            self.repository.clone()
        };

        let signer = get_signer(&signer_id, &network_config).await?;

        if self.upload_contract {
            asimov_dataset_cli::publish::upload_repository_contract(
                self.repository.clone(),
                signer_id.clone(),
                signer.clone(),
                &network_config,
            )
            .await
            .context("Failed uploading contract")?;
        }

        let files: Vec<PathBuf> = self
            .files
            .iter()
            .map(PathBuf::from)
            .filter(|file| std::fs::exists(file).unwrap_or(false))
            .collect();

        let (prepared_files, unprepared_files) = publish::split_prepared_files(&files);

        let prepared_files: VecDeque<(PathBuf, usize)> = prepared_files
            .iter()
            .cloned()
            .map(|file| (file.clone(), file_size(&file)))
            .collect();

        let (event_tx, event_rx) = crossbeam::channel::unbounded();
        let (files_tx, files_rx) = crossbeam::channel::unbounded();

        let mut set: JoinSet<Result<()>> = JoinSet::new();

        let (ctx, _cancel) = context::new_cancel_context();

        if !unprepared_files.is_empty() {
            let dir = create_tmp_dir().context("Failed to create directory for prepared files")?;

            set.spawn({
                let ctx = ctx.clone();
                let tx = event_tx.clone();
                let unprepared_files = unprepared_files.clone().into_iter();
                let report = PrepareStatsReport { tx };

                let params = asimov_dataset_cli::prepare::ParamsBuilder::default()
                    .files(unprepared_files)
                    .files_tx(files_tx)
                    .output_dir(dir.clone())
                    .report(report)
                    .build()?;
                asimov_dataset_cli::prepare::prepare_datasets(ctx, params)
            });
        } else {
            drop(files_tx);
        }

        let unprepared_files: VecDeque<(PathBuf, usize)> = unprepared_files
            .iter()
            .cloned()
            .map(|file| (file.clone(), file_size(&file)))
            .collect();

        let prepare_state = if unprepared_files.is_empty() {
            None
        } else {
            let total_bytes = unprepared_files.iter().map(|(_, size)| size).sum();
            Some(ui::PrepareState {
                total_bytes,
                queued_files: unprepared_files,
                ..Default::default()
            })
        };

        let params = asimov_dataset_cli::publish::ParamsBuilder::default()
            .signer_id(signer_id)
            .signer(signer)
            .repository(self.repository)
            .dataset(self.dataset)
            .network(network_config)
            .files(
                prepared_files
                    .clone()
                    .into_iter()
                    .chain(files_rx.into_iter()),
            )
            .report(PublishStatsReport { tx: event_tx })
            .build()?;

        set.spawn({
            async move { asimov_dataset_cli::publish::publish_datasets(ctx, params).await }
        });

        let total_bytes = prepared_files.iter().map(|(_, size)| size).sum();
        let ui_state = ui::PublishState {
            queued_files: prepared_files,
            total_bytes,
            prepare: prepare_state,
            ..Default::default()
        };

        ui::run_publish(verbosity, ui_state, event_rx)?;

        while let Some(join_result) = set.join_next().await {
            match join_result {
                Err(err) if err.is_cancelled() => (),
                Err(err) => panic!("{err}"),
                Ok(task_result) => task_result?,
            }
        }

        Ok(())
    }
}

async fn get_signer(account: &AccountId, network: &NetworkConfig) -> Result<Arc<Signer>> {
    let keystore_result = Signer::from_keystore_with_search_for_keys(account.clone(), network)
        .await
        .with_context(|| format!("Failed to get signer from keychain for \"{}\"", account))
        .and_then(|keystore| Signer::new(keystore).context("Failed to create keychain signer"));

    let keystore_err = match keystore_result {
        Ok(keystore) => return Ok(keystore),
        Err(err) => err,
    };

    let secret_key_result = std::env::var("NEAR_PRIVATE_KEY")
        .map_err(|err| match err {
            std::env::VarError::NotPresent => {
                eyre!("Environment variable NEAR_PRIVATE_KEY is not present")
            }
            std::env::VarError::NotUnicode(_os_string) => {
                eyre!("Environment variable NEAR_PRIVATE_KEY has invalid data",)
            }
        })
        .and_then(|key_bytes| key_bytes.parse().context("Invalid NEAR private key format"))
        .map(Signer::from_secret_key)
        .and_then(|secret_key| {
            Signer::new(secret_key).context("Failed to create signer from private key")
        });

    let secret_key_err = match secret_key_result {
        Ok(secret_key) => return Ok(secret_key),
        Err(err) => err,
    };

    Err(eyre::eyre!(
        "Unable to find credentials for NEAR account \"{}\"",
        account
    )
    .with_note(|| {
        format!(
            "\nThe CLI tried two methods to find your credentials:\n\
             1. Searching the system keychain for account \"{}\"\n\
             2. Looking for a private key in the NEAR_PRIVATE_KEY environment variable\n",
            account
        )
    })
    .with_section(|| format!("Keychain error: {:#}", keystore_err))
    .with_section(|| format!("Private key error: {:#}", secret_key_err))
    .with_suggestion(|| {
        "\nYou can:\n\
             • Import your account into the keychain:\n\t $ near account import-account\n\
             • Set the NEAR_PRIVATE_KEY environment variable with your private key (\"ed25519:...\")\n\
             • Use the --signer option to specify a different account that has access to the repository contract"
    }))
}

fn create_tmp_dir() -> std::io::Result<PathBuf> {
    let mut temp_dir = std::env::temp_dir();
    temp_dir.push("asimov-dataset");
    temp_dir.push(std::process::id().to_string());
    std::fs::create_dir_all(&temp_dir)?;
    Ok(temp_dir)
}

fn file_size(file: &PathBuf) -> usize {
    std::fs::metadata(file).map(|f| f.len()).unwrap() as usize
}

fn get_cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::Green.on_default().bold())
        .usage(AnsiColor::Green.on_default())
        .literal(AnsiColor::Cyan.on_default().bold())
        .placeholder(AnsiColor::Cyan.on_default())
}
