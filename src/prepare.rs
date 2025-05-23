// This is free and unencumbered software released into the public domain.

use crossbeam::channel::{Receiver, Sender};
use eyre::{Context as _, OptionExt, Result};
use rdf_rs::model::Statement;
use rdf_writer::Writer;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::File,
    io::{BufReader, Write},
    path::PathBuf,
    rc::Rc,
};
use tokio::task::JoinSet;
use tracing::info;

use crate::context::Context;

/// Max bytes for serialized result, leaving some room for rdf_insert header.
const MAX_FILE_SIZE: usize = 1_572_864 - 1024;

/// Controls how close we want the serialized result to be to MAX_FILE_SIZE.
const ACCEPTABLE_RATIO: f64 = 0.95;

#[derive(Clone, Debug)]
pub struct PrepareStatsReport {
    pub tx: Sender<crate::ui::Event>,
}

#[derive(derive_builder::Builder, Debug)]
#[builder(pattern = "owned")]
pub struct Params<I> {
    files: I,
    files_tx: Sender<(PathBuf, usize)>,
    output_dir: PathBuf,
    #[builder(setter(into, strip_option), default)]
    report: Option<PrepareStatsReport>,
}

impl<I> Params<I> {
    pub fn new(
        files: I,
        files_tx: Sender<(PathBuf, usize)>,
        report: Option<PrepareStatsReport>,
        output_dir: PathBuf,
    ) -> Self {
        Self {
            files,
            files_tx,
            report,
            output_dir,
        }
    }
}

pub async fn prepare_datasets<I>(ctx: Context, params: Params<I>) -> Result<()>
where
    I: Iterator<Item = PathBuf>,
{
    let (batch_tx, batch_rx) = crossbeam::channel::bounded(100);

    let mut set = JoinSet::new();

    set.spawn_blocking({
        let ctx = ctx.clone();
        let files: Vec<PathBuf> = params.files.collect();
        let report = params.report.clone();
        move || read_worker_loop(ctx, &files, batch_tx, report)
    });

    let (dataset_tx, dataset_rx) = crossbeam::channel::bounded(10);

    for _ in 0..6 {
        let batch_rx = batch_rx.clone();
        let dataset_tx = dataset_tx.clone();
        let ctx = ctx.clone();
        set.spawn_blocking(|| prepare_worker_loop(ctx, batch_rx, dataset_tx));
    }
    drop(dataset_tx);

    set.spawn_blocking(|| {
        write_worker_loop(
            ctx,
            dataset_rx,
            params.files_tx,
            params.report,
            params.output_dir,
        )
    });

    while let Some(handle) = set.join_next().await {
        handle??;
    }
    Ok(())
}

struct StatementBatch {
    quads: Vec<(usize, oxrdf::Quad)>,
}

#[derive(Default)]
struct RDFBDataset {
    data: Vec<u8>,
    statement_count: usize,
    skipped_statements: usize,
}

fn read_worker_loop(
    ctx: Context,
    files: &[PathBuf],
    batch_tx: Sender<StatementBatch>,
    report: Option<PrepareStatsReport>,
) -> Result<()> {
    struct CountingBufReader<R> {
        inner: BufReader<R>,
        count: Rc<RefCell<usize>>,
    }

    impl<R> CountingBufReader<R> {
        fn new(inner: BufReader<R>, count: Rc<RefCell<usize>>) -> Self {
            Self { inner, count }
        }
    }

    impl<R: std::io::Read> std::io::Read for CountingBufReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let count = self.inner.read(buf)?;
            *self.count.borrow_mut() += count;
            Ok(count)
        }
    }

    let batch_size = 100_000;
    let mut statement_index: usize = 0;

    for file in files {
        let format = file
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .and_then(oxrdfio::RdfFormat::from_extension)
            .ok_or_eyre("Unknown file format")?;
        let reader = File::open(file).context("Failed to open input file")?;
        let reader = BufReader::with_capacity(1 << 20, reader);
        let count = Rc::new(RefCell::new(0));
        let reader = CountingBufReader::new(reader, count.clone());
        let mut reader = oxrdfio::RdfParser::from_format(format).for_reader(reader);

        while !ctx.is_cancelled() {
            let mut quads = Vec::with_capacity(batch_size);

            let finished = loop {
                let Some(quad) = reader.next() else {
                    break true;
                };
                let quad = quad?;
                quads.push((statement_index, quad));
                statement_index += 1;
                if quads.len() >= batch_size {
                    break false;
                }
            };

            if finished && quads.is_empty() && *count.borrow() == 0 {
                break;
            }

            if let Some(ref report) = report {
                let mut bytes = count.borrow_mut();
                report
                    .tx
                    .send(crate::ui::Event::Reader(crate::ui::ReaderProgress {
                        filename: PathBuf::from(file),
                        bytes: *bytes,
                        statement_count: quads.len(),
                        finished,
                    }))
                    .ok();
                *bytes = 0;
            }

            if batch_tx.send(StatementBatch { quads }).is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

fn prepare_worker_loop(
    ctx: Context,
    batch_rx: Receiver<StatementBatch>,
    dataset_tx: Sender<RDFBDataset>,
) -> Result<()> {
    // Buffer for storing statements that need to be retried
    let mut statement_buffer: VecDeque<(usize, Box<dyn Statement>)> = VecDeque::new();
    // write_count is how many we're trying to serialize each iteration
    let mut write_count: usize = 1;
    // write_count_delta controls how we update write_count if the resulting data is either too
    // large or too small
    let mut write_count_delta: usize = 1;
    // lowest_overflow is the lowest known write_count where result data is too large
    let mut lowest_overflow: usize = usize::MAX;
    // have_more states whether the producer has more items
    let mut have_more = true;
    // best_ratio contains the best known (non-overflowing) size ratio for each iteration.
    // It's used to quit early in the case where adding one more statement overflows but current
    // write_count doesn't meet ACCEPTABLE_RATIO.
    let mut best_ratio: f64 = 0.0;

    let mut skipped_statements: usize = 0;

    while !ctx.is_cancelled() {
        while have_more && (statement_buffer.len() < write_count) {
            let Ok(batch) = batch_rx.recv() else {
                have_more = false;
                break;
            };
            statement_buffer.extend(batch.quads.into_iter().map(|(i, stmt)| (i, stmt.into())));
        }

        if statement_buffer.is_empty() {
            break;
        }

        let try_write_count = write_count.min(statement_buffer.len());
        let ser_result =
            serialize_statements(statement_buffer.range(..try_write_count).map(|(_, x)| x));

        let too_large = match ser_result {
            Ok(ref data) => data.len() > MAX_FILE_SIZE,
            Err(ref err) => err.kind() == std::io::ErrorKind::Other,
        };

        if too_large {
            // current size is larger than max

            if write_count == 1 {
                if let Some((index, _)) = statement_buffer.pop_front() {
                    tracing::warn!(?index, "statement is too large to be published even alone");
                    skipped_statements += 1;
                    continue;
                }
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

        if dataset_tx
            .send(RDFBDataset {
                data,
                statement_count: try_write_count,
                skipped_statements,
            })
            .is_err()
        {
            return Ok(());
        }

        statement_buffer.drain(..try_write_count);

        // reset these:
        write_count = 1;
        best_ratio = 0.0;
        lowest_overflow = usize::MAX;
        skipped_statements = 0;
    }

    Ok(())
}

fn write_worker_loop(
    ctx: crate::context::Context,
    dataset_rx: Receiver<RDFBDataset>,
    files_tx: Sender<(PathBuf, usize)>,
    report: Option<PrepareStatsReport>,
    output_dir: PathBuf,
) -> Result<()> {
    // The index for output file. Used as `prepared.{:06d}.rdfb`.
    let mut file_idx: usize = 1;
    let mut total_written: usize = 0;

    while !ctx.is_cancelled() {
        let Ok(prepared) = dataset_rx.recv() else {
            break;
        };
        let filename = output_dir.join(format!("prepared.{:06}.rdfb", file_idx));

        let mut file =
            std::fs::File::create(&filename).context("Failed to create output file for RDFB")?;
        file.write_all(&prepared.data)
            .context("Failed to write RDFB data")?;

        if files_tx
            .send((filename.clone(), prepared.statement_count))
            .is_err()
        {
            return Ok(());
        }

        if let Some(ref report) = report {
            let filename = filename.clone();
            report
                .tx
                .send(crate::ui::Event::Prepare(crate::ui::PrepareProgress {
                    filename,
                    bytes: prepared.data.len(),
                    statement_count: prepared.statement_count,
                    skipped_statements: prepared.skipped_statements,
                }))
                .ok();
        }

        total_written += prepared.statement_count;
        let ratio = prepared.data.len() as f64 / MAX_FILE_SIZE as f64;
        info!(
            batch_byte_size = prepared.data.len(),
            batch_statement_count = prepared.statement_count,
            total_statement_count = total_written,
            ratio,
            ?filename,
            "Writing file"
        );
        file_idx += 1;
    }

    Ok(())
}

struct SharedBufferWriter {
    buffer: Rc<RefCell<Vec<u8>>>,
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
