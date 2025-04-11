// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use crossbeam::channel::Sender;
use eyre::{Context as _, Result, bail, eyre};
use near_api::{
    AccountId, Contract, NearGas, NetworkConfig, Transaction,
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
    signer: Arc<near_api::Signer>,
    network: &NetworkConfig,
) -> Result<()> {
    let code = include_bytes!("../assets/log_vault.wasm").to_vec();
    let tx_outcome = Transaction::construct(repository.clone(), repository.clone())
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

pub async fn publish_datasets<I>(
    ctx: Context,
    repository: AccountId,
    dataset: Option<String>,
    signer: Arc<near_api::Signer>,
    network: &NetworkConfig,
    files: I,
    report: Option<PublishStatsReport>,
) -> Result<()>
where
    I: Iterator<Item = (PathBuf, usize)>,
{
    let dataset = dataset.unwrap_or(String::from(""));
    for (filename, statement_count) in files {
        if ctx.is_cancelled() {
            break;
        }
        let mut args = Vec::new();
        1_u8.serialize(&mut args)?; // version 1
        dataset.serialize(&mut args)?;
        1_u8.serialize(&mut args)?; // RDF/Borsh dataset encoding

        let bytes = std::fs::File::open(&filename)?.read_to_end(&mut args)?;

        let _tx_outcome = Transaction::construct(repository.clone(), repository.clone())
            .add_action(Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "rdf_insert".into(),
                args,
                gas: NearGas::from_tgas(300).as_gas(),
                deposit: 0,
            })))
            .with_signer(signer.clone())
            .send_to(network)
            .await
            .inspect(
                |outcome| tracing::info!(?filename, status = ?outcome.transaction_outcome.outcome.status, "uploaded dataset"),
            )?;

        std::fs::remove_file(&filename).ok();

        if let Some(ref report) = report {
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
