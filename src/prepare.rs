// This is free and unencumbered software released into the public domain.

use rdf_rs::model::Statement;
use rdf_writer::Writer;
use std::{
    cell::RefCell,
    collections::VecDeque,
    error::Error,
    fs::File,
    io::{BufReader, Write},
    path::PathBuf,
    rc::Rc,
    sync::mpsc,
};
use tracing::info;

/// Max bytes for serialized result, leaving some room for rdf_insert header.
const MAX_FILE_SIZE: usize = 1_572_864 - 1024;

/// Controls how close we want the serialized result to be to MAX_FILE_SIZE.
const ACCEPTABLE_RATIO: f64 = 0.95;

pub fn prepare_datasets(files: &[String]) -> Result<(), Box<dyn Error>> {
    std::thread::scope(|s| {
        let (batch_req_send, batch_req_recv) = mpsc::sync_channel(10);
        let (dataset_send, dataset_recv) = mpsc::sync_channel(10);

        let files: Vec<String> = files.to_vec();
        let producer = files
            .into_iter()
            .map(|file| {
                let file = PathBuf::from(file);
                let format = file
                    .extension()
                    .and_then(std::ffi::OsStr::to_str)
                    .and_then(oxrdfio::RdfFormat::from_extension);
                (file, format)
            })
            .flat_map(|(file, format)| {
                let reader = File::open(file).map(BufReader::new).unwrap();
                oxrdfio::RdfParser::from_format(format.unwrap()).for_reader(reader)
            })
            .flatten();

        s.spawn(|| read_worker_loop(producer, batch_req_recv));

        for _ in 0..4 {
            let batch_req_send = batch_req_send.clone();
            let dataset_send = dataset_send.clone();
            s.spawn(|| prepare_worker_loop(batch_req_send, dataset_send));
        }

        s.spawn(|| write_worker_loop(dataset_recv));
    });

    Ok(())
}

struct StatementBatchRequest {
    amount: usize,
    response_chan: oneshot::Sender<StatementBatch>,
}

struct StatementBatch {
    quads: Vec<oxrdf::Quad>,
}

#[derive(Default)]
struct RDFBDataset {
    data: Vec<u8>,
    statement_count: usize,
}

fn read_worker_loop<I>(mut producer: I, requests: mpsc::Receiver<StatementBatchRequest>)
where
    I: Iterator<Item = oxrdf::Quad>,
{
    while let Ok(req) = requests.recv() {
        let mut quads: Vec<oxrdf::Quad> = Vec::with_capacity(req.amount);

        while quads.len() < req.amount {
            if let Some(quad) = producer.next() {
                quads.push(quad);
            } else {
                req.response_chan.send(StatementBatch { quads }).unwrap();
                return;
            }
        }

        req.response_chan.send(StatementBatch { quads }).unwrap();
    }
}

fn prepare_worker_loop(
    // producer: Arc<Mutex<std::sync::mpsc::Receiver<StatementBatch>>>,
    batch_requests: mpsc::SyncSender<StatementBatchRequest>,
    sink: mpsc::SyncSender<RDFBDataset>,
) {
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

    loop {
        while have_more && (statement_buffer.len() < write_count) {
            let (batch_send, batch_recv) = oneshot::channel();
            if batch_requests
                .send(StatementBatchRequest {
                    amount: write_count - statement_buffer.len(),
                    response_chan: batch_send,
                })
                .is_err()
            {
                have_more = false;
                break;
            }
            let Ok(batch) = batch_recv.recv() else {
                have_more = false;
                break;
            };
            for statement in batch.quads {
                statement_buffer.push_back(statement.into());
            }
        }

        if statement_buffer.is_empty() {
            break;
        }

        let try_write_count = write_count.min(statement_buffer.len());
        let ser_result = serialize_statements(statement_buffer.range(..try_write_count));

        let too_large = match ser_result {
            Ok(ref data) => data.len() > MAX_FILE_SIZE,
            Err(ref err) => err.kind() == std::io::ErrorKind::Other,
        };

        if too_large {
            // current size is larger than max

            if write_count == 1 {
                let _stmt = statement_buffer.pop_front();
                // let statement_number = total_written + 1;
                // warn!(
                //     ?statement_number,
                //     "statement is too large to be published even alone"
                // );
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

            write_count_delta = write_count_delta.max(1);

            write_count += write_count_delta;
            continue;
        }

        let data = match ser_result {
            Ok(data) => data,
            Err(err) => panic!("{err}"), // TODO
        };

        let ratio = data.len() as f64 / MAX_FILE_SIZE as f64;

        if (ratio < ACCEPTABLE_RATIO)
            && (ratio != best_ratio)
            && (statement_buffer.len() > write_count || have_more)
        {
            // we're under the target
            // ... and the best ratio is something else (anti-loop measure)
            // ... and there are more statements that could be included

            best_ratio = best_ratio.max(ratio);

            write_count_delta <<= 1;

            let diff = lowest_overflow - write_count;
            while write_count_delta >= diff {
                write_count_delta >>= 1;
            }

            write_count_delta = write_count_delta.max(1);

            write_count += write_count_delta;

            if (write_count + 1) >= lowest_overflow {
                // It is possible that the final serialization of a dataset with *more* statements
                // ends up being *smaller* after compression.
                // If we end up here it means that the best_ratio was somewhere on N-1, N-2, ...
                // Just accept current ratio and on next iteration this will write the file.
            } else {
                continue;
            }
        }

        sink.send(RDFBDataset {
            data,
            statement_count: try_write_count,
        })
        .unwrap();

        statement_buffer.drain(..try_write_count);

        // reset these:
        write_count = 1;
        best_ratio = 0.0;
        lowest_overflow = usize::MAX;
    }
}

fn write_worker_loop(producer: mpsc::Receiver<RDFBDataset>) {
    // The index for output file. Used as `prepared.{:06d}.rdfb`.
    let mut file_idx: usize = 1;
    let mut total_written: usize = 0;
    while let Ok(prepared) = producer.recv() {
        let filename = format!("prepared.{:06}.rdfb", file_idx);
        std::fs::File::create(&filename)
            .unwrap()
            .write_all(&prepared.data)
            .unwrap();
        total_written += prepared.statement_count;
        let ratio = prepared.data.len() as f64 / MAX_FILE_SIZE as f64;
        info!(
            batch_byte_size = prepared.data.len(),
            batch_statement_count = prepared.statement_count,
            total_statement_count = total_written,
            ratio,
            filename,
            "Writing file"
        );
        file_idx += 1;
    }
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
