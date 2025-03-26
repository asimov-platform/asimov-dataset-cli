// This is free and unencumbered software released into the public domain.

use borsh::BorshSerialize;
use near_api::{
    AccountId, NearGas, NetworkConfig, Transaction,
    near_primitives::action::{Action, FunctionCallAction},
};
use rdf_rs::model::Statement;
use rdf_writer::Writer;
use std::{
    cell::RefCell,
    collections::VecDeque,
    error::Error,
    io::{Read, Write},
    rc::Rc,
    sync::Arc,
};
use tracing::{info, trace, warn};

/// Max bytes for serialized result, leaving some room for rdf_insert header.
const MAX_FILE_SIZE: usize = 1_572_864 - 1024;

/// Controls how close we want the serialized result to be to MAX_FILE_SIZE.
const ACCEPTABLE_RATIO: f64 = 0.9;

pub fn prepare_datasets(files: &[String]) -> Result<(), Box<dyn Error>> {
    let mut reader = files
        .iter()
        .flat_map(|file| rdf_reader::open_path(file, None))
        .flatten();

    // The index for output file. Used as `prepared.{:06d}.rdfb`.
    let mut file_idx: usize = 1;
    // Buffer for storing statements that need to be retried
    let mut statement_buffer: VecDeque<Box<dyn Statement>> = VecDeque::new();
    // write_count is how many we're trying to serialize each iteration
    let mut write_count: usize = 1;
    // write_count_delta controls how we update write_count if the resulting data is either too
    // large or too small
    let mut write_count_delta: usize = 1;
    // lowest_overflow is the lowest known write_count where result data is too large
    let mut lowest_overflow: usize = usize::MAX;
    // have_more states whether the iterator has more items
    let mut have_more = true;
    // best_ratio contains the best known (non-overflowing) size ratio for each iteration.
    // It's used to quit early in the case where adding one more statement overflows but current
    // write_count doesn't meet ACCEPTABLE_RATIO.
    let mut best_ratio: f64 = 0.0;

    let mut total_written: usize = 0;

    loop {
        while have_more && (statement_buffer.len() < write_count) {
            match reader.next() {
                Some(stmt) => {
                    statement_buffer.push_back(stmt?);
                }
                None => {
                    have_more = false;
                }
            }
        }

        if statement_buffer.is_empty() {
            break;
        }

        let data = match serialize_statements(statement_buffer.iter().take(write_count)) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::Other => {
                trace!(
                    statement_count = write_count.min(statement_buffer.len()),
                    write_count,
                    write_count_delta,
                    best_ratio,
                    ?err,
                    "failed to serialize"
                );

                lowest_overflow = lowest_overflow.min(write_count);

                // backtrack
                write_count -= write_count_delta;

                if write_count_delta == 1 {
                    // this helps get unstuck
                    write_count = lowest_overflow - 2;
                } else {
                    // the last delta was too large so pull back
                    write_count_delta >>= 1;
                }

                if write_count_delta == 0 {
                    write_count_delta = 1
                };

                write_count += write_count_delta;

                continue;
            }
            Err(err) => panic!("{err}"),
        };

        let ratio = data.len() as f64 / MAX_FILE_SIZE as f64;

        trace!(
            data = data.len(),
            statement_buffer = statement_buffer.len(),
            ratio,
            best_ratio,
            lowest_overflow,
            write_count,
            write_count_delta
        );

        if 1.0 > ratio
            && (ratio > ACCEPTABLE_RATIO
                || ratio == best_ratio
                || (statement_buffer.len() < write_count && !have_more))
        {
            let written = write_count.min(statement_buffer.len());
            total_written += written;

            // write to a file
            let filename = format!("prepared.{:06}.rdfb", file_idx);
            info!(
                data.len = data.len(),
                batch_statement_count = written,
                total_statement_count = total_written,
                ratio,
                filename,
                "Writing file"
            );
            std::fs::File::create(&filename)?.write_all(&data)?;
            file_idx += 1;

            statement_buffer.drain(..written);
            // reset these:
            write_count = 1;
            best_ratio = 0.0;
            lowest_overflow = usize::MAX;

            continue;
        }

        if ratio > 1.0 {
            // current size is larger than max
            if write_count == 1 {
                let stmt = statement_buffer.pop_front();
                warn!(?stmt, "statement is too large to be published even alone");
                continue;
            }

            lowest_overflow = lowest_overflow.min(write_count);

            // backtrack
            write_count -= write_count_delta;

            if write_count_delta == 1 {
                // this helps get unstuck
                write_count = lowest_overflow - 2;
            } else {
                // the last delta was too large so pull back
                write_count_delta >>= 1;
            }
        } else {
            // current size is smaller than max

            best_ratio = best_ratio.max(ratio);

            write_count_delta <<= 1;

            let diff = lowest_overflow - write_count;
            while write_count_delta >= diff {
                write_count_delta >>= 1;
            }
        }

        if write_count_delta == 0 {
            write_count_delta = 1
        };

        if (write_count + 1) >= lowest_overflow {
            // If we end up here it means that the best_ratio was somewhere on N-1, N-2, ...
            // Just accept current ratio and on next iteration this will write the file.
            best_ratio = ratio;
            continue;
        }

        write_count += write_count_delta;
    }

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

struct SharedBufferWriter {
    buffer: Rc<std::cell::RefCell<Vec<u8>>>,
}

impl Default for SharedBufferWriter {
    fn default() -> Self {
        let buffer = Rc::new(RefCell::new(Vec::with_capacity(MAX_FILE_SIZE)));
        Self { buffer }
    }
}

impl SharedBufferWriter {
    fn buffer(&self) -> Rc<RefCell<Vec<u8>>> {
        self.buffer.clone()
    }
}

impl std::io::Write for SharedBufferWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_statements<T, I>(statements: I) -> Result<Vec<u8>, std::io::Error>
where
    T: AsRef<dyn Statement>,
    I: Iterator<Item = T>,
{
    let w = SharedBufferWriter::default();
    let buf = w.buffer();
    let mut writer = rdf_borsh::BorshWriter::new(Box::new(w))?;

    for statement in statements {
        writer.write_statement(statement.as_ref())?;
    }
    writer.finish()?;

    Ok(buf.take())
}
