// This is free and unencumbered software released into the public domain.

use rdf_writer::Writer;
use std::{error::Error, sync::Arc};

pub fn prepare_datasets(files: &[String]) -> Result<(), Box<dyn Error>> {
    let mut reader = files
        .iter()
        .flat_map(|file| rdf_reader::open_path(file, None))
        .flatten();

    let mut file_idx = 1_usize;
    let sink = std::fs::File::create(format!("prepared.{:06}.rdfb", file_idx))?;
    let mut writer = rdf_borsh::BorshWriter::new(Box::new(sink))?;

    loop {
        let Some(stmt) = reader.next() else { break };
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
    _repository: &str,
    _signer: Arc<near_api::Signer>,
    _files: &[String],
) -> Result<(), Box<dyn Error>> {
    Ok(())
}
