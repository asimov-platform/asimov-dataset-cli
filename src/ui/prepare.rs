// This is free and unencumbered software released into the public domain.

use std::{collections::VecDeque, path::PathBuf};

/// Prepare contains the UI state of preparation progress.
#[derive(Debug, Default)]
pub struct PrepareState {
    pub current_file: Option<PathBuf>,
    pub current_file_size: usize,
    pub current_read_bytes: usize,

    pub queued_files: VecDeque<(PathBuf, usize)>,
    pub total_bytes: usize,

    pub read_bytes: usize,
    pub read_files: Vec<PathBuf>,
    pub read_statements: usize,

    pub prepared_bytes: usize,
    pub prepared_files: Vec<PathBuf>,
    pub prepared_statements: usize,
    pub skipped_statemets: usize,
}

impl PrepareState {
    pub fn update_reader_state(&mut self, progress: ReaderProgress) {
        match self.current_file {
            Some(ref curr) if *curr == progress.filename => {
                self.current_read_bytes += progress.bytes;
            }
            _ => {
                let size = self
                    .queued_files
                    .iter()
                    .find(|(name, _size)| *name == progress.filename)
                    .unwrap()
                    .1;
                self.current_file = Some(progress.filename.clone());
                self.current_file_size = size;
                self.current_read_bytes = progress.bytes;
            }
        }

        self.read_bytes += progress.bytes;
        self.read_statements += progress.statement_count;

        if progress.finished {
            self.queued_files
                .retain(|(name, _size)| *name != progress.filename);
            self.read_files.push(progress.filename);
            self.current_file = None;
        }
    }

    pub fn update_prepare_state(&mut self, progress: PrepareProgress) {
        self.prepared_bytes += progress.bytes;
        self.prepared_statements += progress.statement_count;
        self.skipped_statemets += progress.skipped_statements;
        self.prepared_files.push(progress.filename);
    }
}

#[derive(Debug, Default)]
pub struct ReaderProgress {
    pub filename: PathBuf,
    pub bytes: usize,
    pub statement_count: usize,
    pub finished: bool,
}

#[derive(Debug, Default)]
pub struct PrepareProgress {
    pub filename: PathBuf,
    pub bytes: usize,
    pub statement_count: usize,
    pub skipped_statements: usize,
}
