// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use near_api::{
    AccountId, NearGas, NetworkConfig, Transaction,
    near_primitives::action::{Action, FunctionCallAction},
};
use std::{error::Error, io::Read, sync::Arc};

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
