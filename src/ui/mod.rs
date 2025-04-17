// This is free and unencumbered software released into the public domain.

use crossbeam::channel::Receiver;
use eyre::Result;

mod prepare;
mod publish;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
pub use prepare::{PrepareProgress, PrepareState, ReaderProgress};
pub use publish::{PublishProgress, PublishState};

pub enum UIEvent {
    Resize,
}

#[derive(Debug)]
pub enum Event {
    Reader(ReaderProgress),
    Prepare(PrepareProgress),
    Publish(PublishProgress),
}

pub fn run_prepare(
    verbosity: u8,
    mut state: PrepareState,
    progress_rx: Receiver<Event>,
) -> Result<()> {
    let parsing_style =
        ProgressStyle::with_template("{msg:10} [{bar:40}] {binary_bytes} / {binary_total_bytes}")
            .unwrap()
            .progress_chars("##-");

    let prepare_style =
        ProgressStyle::with_template("{msg:10} [{bar:40}] {human_pos} / {human_len}")
            .unwrap()
            .progress_chars("##-");

    let multi = MultiProgress::new();
    if verbosity < 1 {
        // only show bars for `-v`
        multi.set_draw_target(ProgressDrawTarget::hidden());
    }
    let reader_bar = ProgressBar::new(state.total_bytes as u64)
        .with_message("Parsing")
        .with_style(parsing_style);
    let prepare_bar = ProgressBar::new(0)
        .with_message("Batching")
        .with_style(prepare_style);

    multi.add(reader_bar.clone());
    multi.add(prepare_bar.clone());

    while let Ok(event) = progress_rx.recv() {
        tracing::debug!(?event);

        match event {
            Event::Reader(progress) => {
                reader_bar.inc(progress.bytes as u64);
                prepare_bar.inc_length(progress.statement_count as u64);
                if progress.finished && verbosity > 1 {
                    multi.println(format!(
                        "✅ Finished reading file {}",
                        progress.filename.display()
                    ))?;
                }
                state.update_reader_state(progress);
            }
            Event::Prepare(progress) => {
                prepare_bar.inc(progress.statement_count as u64);
                if verbosity > 1 {
                    if let Some(filename) = progress
                        .filename
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                    {
                        multi.println(format!("✅ Prepared batch {}", filename))?;
                    }
                }
                state.update_prepare_state(progress);
            }
            Event::Publish(_) => unreachable!(),
        }
    }

    reader_bar.finish();
    prepare_bar.finish();

    Ok(())
}

pub fn run_publish(
    verbosity: u8,
    mut state: PublishState,
    progress_rx: Receiver<Event>,
) -> Result<()> {
    let parsing_style =
        ProgressStyle::with_template("{msg:10} [{bar:40}] {binary_bytes} / {binary_total_bytes}")
            .unwrap()
            .progress_chars("##-");

    let prepare_style =
        ProgressStyle::with_template("{msg:10} [{bar:40}] {human_pos} / {human_len}")
            .unwrap()
            .progress_chars("##-");

    let upload_style =
        ProgressStyle::with_template("{msg:10} [{bar:40}] {human_pos} / {human_len}")
            .unwrap()
            .progress_chars("##-");

    let multi = MultiProgress::new();
    if verbosity < 1 {
        // only show bars for `-v`
        multi.set_draw_target(ProgressDrawTarget::hidden());
    }

    let reader_bar = multi.add(
        ProgressBar::new(
            state
                .prepare
                .as_ref()
                .map(|state| state.total_bytes)
                .unwrap_or_default() as u64,
        )
        .with_message("Parsing")
        .with_style(parsing_style),
    );
    let prepare_bar = multi.add(
        ProgressBar::new(0)
            .with_message("Batching")
            .with_style(prepare_style),
    );
    let upload_bar = multi.add(
        ProgressBar::new(0)
            .with_message("Upload")
            .with_style(upload_style),
    );

    while let Ok(event) = progress_rx.recv() {
        tracing::debug!(?event);

        match event {
            Event::Reader(progress) => {
                reader_bar.inc(progress.bytes as u64);
                prepare_bar.inc_length(progress.statement_count as u64);
                if progress.finished && verbosity > 1 {
                    multi.println(format!(
                        "✅ Finished reading file {}",
                        progress.filename.display()
                    ))?;
                }
                if let Some(ref mut state) = state.prepare {
                    state.update_reader_state(progress);
                }
            }
            Event::Prepare(progress) => {
                prepare_bar.inc(progress.statement_count as u64);
                upload_bar.inc_length(1);
                if verbosity > 1 {
                    if let Some(filename) = progress
                        .filename
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                    {
                        multi.println(format!("✅ Prepared batch {}", filename))?;
                    }
                }
                if let Some(ref mut state) = state.prepare {
                    state.update_prepare_state(progress);
                }
            }
            Event::Publish(progress) => {
                upload_bar.inc(1);
                if verbosity > 1 {
                    if let Some(filename) = progress
                        .filename
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                    {
                        multi.println(format!("✅ Uploaded batch {}", filename))?;
                    }
                }
                state.update_publish_state(progress);
            }
        }
    }

    reader_bar.finish();
    prepare_bar.finish();
    upload_bar.finish();

    Ok(())
}
