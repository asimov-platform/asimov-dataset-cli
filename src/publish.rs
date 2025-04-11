// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use crossbeam::channel::Sender;
use eyre::{Context as _, Result, eyre};
use near_api::{
    AccountId, NearGas, NetworkConfig, Transaction,
    near_primitives::action::{Action, DeployContractAction, FunctionCallAction},
};
use std::{io::Read, path::PathBuf, sync::Arc};

use crate::context::Context;

#[derive(Clone, Debug)]
pub struct PublishStatsReport {
    pub tx: Sender<crate::ui::Event>,
}

/// Splits the files into (prepared, unprepared) according to their file extension.
pub fn split_prepared_files(files: &[PathBuf]) -> (Vec<PathBuf>, Vec<PathBuf>) {
    files
        .iter()
        .cloned()
        .partition(|file| file.extension().is_some_and(|ext| ext == "rdfb"))
}

pub async fn upload_repository_contract(
    repository: AccountId,
    signer_id: AccountId,
    signer: Arc<near_api::Signer>,
    network: &NetworkConfig,
) -> Result<()> {
    let code = include_bytes!("../assets/log_vault.wasm").to_vec();
    let tx_outcome = Transaction::construct(signer_id.clone(), repository.clone())
        .add_action(Action::DeployContract(DeployContractAction { code }))
        .with_signer(signer)
        .send_to(network)
        .await
        .context("Failed to send DeployContract tx to RPC")?;

    use near_api::near_primitives::views::FinalExecutionStatus;
    match tx_outcome.status {
        FinalExecutionStatus::NotStarted => todo!(),
        FinalExecutionStatus::Started => todo!(),
        FinalExecutionStatus::SuccessValue(_items) => Ok(()),
        FinalExecutionStatus::Failure(error) => Err(eyre!(error)),
    }
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Params<I> {
    signer_id: AccountId,
    signer: Arc<near_api::Signer>,
    repository: AccountId,
    #[builder(setter(into), default)]
    dataset: Option<String>,
    network: NetworkConfig,
    files: I,
    #[builder(setter(into, strip_option), default)]
    report: Option<PublishStatsReport>,
}

impl<I> Params<I> {
    pub fn new(
        repository: AccountId,
        signer_id: AccountId,
        dataset: Option<String>,
        signer: Arc<near_api::Signer>,
        network: NetworkConfig,
        files: I,
        report: Option<PublishStatsReport>,
    ) -> Self {
        Self {
            repository,
            signer_id,
            dataset,
            signer,
            network,
            files,
            report,
        }
    }
}

pub async fn publish_datasets<I>(ctx: Context, params: Params<I>) -> Result<()>
where
    I: Iterator<Item = (PathBuf, usize)>,
{
    let dataset = params.dataset.unwrap_or(String::from(""));
    for (filename, statement_count) in params.files {
        if ctx.is_cancelled() {
            break;
        }
        let mut args = Vec::new();
        1_u8.serialize(&mut args)?; // version 1
        dataset.serialize(&mut args)?;
        1_u8.serialize(&mut args)?; // RDF/Borsh dataset encoding

        let bytes = std::fs::File::open(&filename)?.read_to_end(&mut args)?;

        let _tx_outcome = Transaction::construct(params.signer_id.clone(), params.repository.clone())
            .add_action(Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "rdf_insert".into(),
                args,
                gas: NearGas::from_tgas(300).as_gas(),
                deposit: 0,
            })))
            .with_signer(params.signer.clone())
            .send_to(&params.network)
            .await
            .inspect(
                |outcome| tracing::info!(?filename, status = ?outcome.transaction_outcome.outcome.status, "uploaded dataset"),
            )?;

        std::fs::remove_file(&filename).ok();

        if let Some(ref report) = params.report {
            report
                .tx
                .send(crate::ui::Event::Publish(crate::ui::PublishProgress {
                    filename,
                    bytes,
                    statement_count,
                }))
                .ok();
        }
    }
    Ok(())
}
