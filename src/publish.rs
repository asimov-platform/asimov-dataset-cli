// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use crossbeam::channel::Sender;
use near_api::{
    AccountId, NearGas, NetworkConfig, Transaction,
    near_primitives::action::{Action, FunctionCallAction},
};
use std::{error::Error, io::Read, path::PathBuf, sync::Arc, time::Duration};

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

pub async fn publish_datasets<I>(
    repository: AccountId,
    signer: Arc<near_api::Signer>,
    network: &NetworkConfig,
    files: I,
    report: Option<PublishStatsReport>,
) -> Result<(), Box<dyn Error>>
where
    I: Iterator<Item = (PathBuf, usize)>,
{
    for (filename, statement_count) in files {
        let mut args = Vec::new();
        1_u8.serialize(&mut args)?; // version 1
        "".serialize(&mut args)?;
        1_u8.serialize(&mut args)?; // RDF/Borsh dataset encoding

        let bytes = std::fs::File::open(&filename)?.read_to_end(&mut args)?;

        // let _tx_outcome = Transaction::construct(repository.clone(), repository.clone())
        //     .add_action(Action::FunctionCall(Box::new(FunctionCallAction {
        //         method_name: "rdf_insert".into(),
        //         args,
        //         gas: NearGas::from_tgas(300).as_gas(),
        //         deposit: 0,
        //     })))
        //     .with_signer(signer.clone())
        //     .send_to(network)
        //     .await
        //     .inspect(
        //         |outcome| tracing::info!(?file, status = ?outcome.transaction_outcome.outcome.status, "uploaded dataset"),
        //     )?;

        std::thread::sleep(Duration::from_millis(500));

        if let Some(ref report) = report {
            report
                .tx
                .send(crate::ui::Event::Publish(crate::ui::PublishProgress {
                    filename,
                    bytes,
                    statement_count,
                }))
                .unwrap();
        }
    }
    Ok(())
}
