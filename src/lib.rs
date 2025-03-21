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
use tracing::{error, trace, warn};

const MAX_FILE_SIZE: usize = 1_572_864; // bytes
//const MAX_FILE_SIZE: usize = 3 * (1 << 10); // bytes

pub fn prepare_datasets(files: &[String]) -> Result<(), Box<dyn Error>> {
    let mut reader = files
        .iter()
        .flat_map(|file| rdf_reader::open_path(file, None))
        .flatten()
        .fuse();

    let mut file_idx = 1_usize;

    // Buffer for storing statements that need to be retried
    let mut statement_buffer: VecDeque<Rc<dyn Statement>> = VecDeque::new();

    let mut write_count: usize = 1;
    let mut write_count_delta: isize = 1;
    let mut have_more = true;
    let mut best_ratio: f64 = 0.0;

    'outer: loop {
        while have_more {
            match reader.next() {
                Some(stmt) => {
                    statement_buffer.push_back(stmt?.into());
                    if statement_buffer.len() >= write_count {
                        break;
                    }
                }
                None => {
                    if statement_buffer.is_empty() {
                        // no more statements left
                        break 'outer;
                    } else {
                        // we have leftovers in the buffer, write those out
                        have_more = false;
                        break;
                    }
                }
            }
        }

        if statement_buffer.is_empty() {
            break;
        }

        let stmts = statement_buffer
            .iter()
            .take(write_count)
            .collect::<Vec<_>>();

        let data = match serialize_statements(&stmts) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::Other => {
                error!(?err, "failed to serialize");

                write_count_delta = if write_count_delta > 1 {
                    -1
                } else {
                    -write_count_delta.abs() << 1
                }
                .min(-1);
                write_count = write_count.checked_add_signed(write_count_delta).unwrap();

                continue;
            }
            Err(err) => panic!("{err}"),
        };

        // target=4
        // start write_count=1
        // deltas=+1,+2
        // 1 -> 2 -> 4

        // target=5
        // start write_count=1
        // deltas=+1,+2+,+4,-2,-1
        // 1 -> 2 -> 4 -> 8 -> 6 -> 5

        // target=6
        // start write_count=1
        // deltas=+1,+2+,+4,-2
        // 1 -> 2 -> 4 -> 8 -> 6

        // target=7
        // start write_count=1
        // deltas=+1,+2,+4,-2,+1
        // 1 -> 2 -> 4 -> 8 -> 6 -> 7

        // target=8
        // start write_count=1
        // deltas=+1,+2,+4
        // 1 -> 2 -> 4 -> 8

        // target=9
        // start write_count=1
        // deltas=+1,+2,+4,+8,-4,-2,-1
        // 1 -> 2 -> 4 -> 8 -> 16 -> 12 -> 10 -> 9

        let ratio = data.len() as f64 / MAX_FILE_SIZE as f64;

        trace!(
            data = data.len(),
            statement_buffer = statement_buffer.len(),
            have_more,
            ratio,
            best_ratio,
            write_count,
            write_count_delta
        );

        if 1.0 > ratio
            && (ratio > 0.9
                || ratio == best_ratio
                || (statement_buffer.len() < write_count && !have_more))
        {
            statement_buffer.drain(..write_count.min(statement_buffer.len()));
            write_count = 1;
            write_count_delta = 1;
            best_ratio = 0.0;

            // write to a file
            let filename = format!("prepared.{:06}.rdfb", file_idx);
            trace!(data = data.len(), ratio, filename, "WRITING FILE");
            std::fs::File::create(&filename)?.write_all(&data)?;
            file_idx += 1;

            continue;
        }

        if ratio > 1.0 {
            // target is smaller than current size
            if write_count == 1 {
                let stmt = statement_buffer.pop_front();
                warn!(?stmt, "statement is too large to be published even alone");
                continue;
            }

            write_count_delta = if write_count_delta > 1 {
                -1
            } else {
                -write_count_delta.abs() << 1
            }
            .min(-1);
        } else {
            // target is larger than current size
            best_ratio = best_ratio.max(ratio);

            //write_count_delta = (write_count_delta << 1).max(1);
            write_count_delta = if write_count_delta < 0 {
                1
            } else {
                write_count_delta << 1
            }
            .max(1);
        }

        write_count = write_count.checked_add_signed(write_count_delta).unwrap();
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

#[derive(Default)]
struct SharedBufferWriter {
    buffer: Rc<std::cell::RefCell<Vec<u8>>>,
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

fn serialize_statements<T: AsRef<dyn Statement>>(
    statements: &[T],
) -> Result<Vec<u8>, std::io::Error> {
    let w = SharedBufferWriter::default();
    let buf = w.buffer();
    let mut writer = rdf_borsh::BorshWriter::new(Box::new(w))?;

    for statement in statements {
        writer.write_statement(statement.as_ref())?;
    }
    writer.finish()?;

    Ok(buf.take())
}
