// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use near_api::{
    AccountId, NearGas, NetworkConfig, Transaction,
    near_primitives::action::{Action, FunctionCallAction},
};
use rdf_writer::Writer;
use std::{error::Error, io::Read, sync::Arc};

pub fn prepare_datasets(files: &[String]) -> Result<(), Box<dyn Error>> {
    let reader = files
        .iter()
        .flat_map(|file| rdf_reader::open_path(file, None))
        .flatten();

    let mut file_idx = 1_usize;
    let sink = std::fs::File::create(format!("prepared.{:06}.rdfb", file_idx))?;
    let mut writer = rdf_borsh::BorshWriter::new(Box::new(sink))?;

    for stmt in reader {
        let stmt = &*stmt?;

        match writer.write_statement(stmt) {
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::Other => {
                // the error type isn't very useful but it's possibly a term dict overflow
                tracing::debug!(?err, "writer.write_statement failed");
                // finish current writer
                writer.finish()?;

                // open next
                file_idx += 1;
                let sink = std::fs::File::create(format!("prepared.{:06}.rdfb", file_idx))?;
                writer = rdf_borsh::BorshWriter::new(Box::new(sink))?;

                // retry writing the statement to the new writer
                writer.write_statement(stmt)?;
            }
            Err(err) => return Err(err.into()),
        }
    }

    writer.finish()?;

    Ok(())
}

pub async fn publish_datasets(
    repository: AccountId,
    signer: Arc<near_api::Signer>,
    network: &NetworkConfig,
    files: &[String],
) -> Result<(), Box<dyn Error>> {
    for file in files {
        let mut args = Vec::new();
        1_u8.serialize(&mut args)?; // version 1
        "".serialize(&mut args)?;
        1_u8.serialize(&mut args)?; // RDF/Borsh dataset encoding

        std::fs::File::open(file)?.read_to_end(&mut args)?;

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
                |outcome| tracing::info!(?file, status = ?outcome.transaction_outcome.outcome.status, "uploaded dataset"),
            )?;
    }
    Ok(())
}
