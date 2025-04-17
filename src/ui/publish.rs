// This is free and unencumbered software released into the public domain.

use std::{collections::VecDeque, path::PathBuf};

/// Publish contains the UI state of publishing progress.
#[derive(Debug, Default)]
pub struct PublishState {
    pub prepare: Option<super::prepare::PrepareState>,

    pub queued_files: VecDeque<(PathBuf, usize)>,
    pub total_bytes: usize,

    pub published_bytes: usize,
    pub published_files: Vec<PathBuf>,
    pub published_statements: usize,
}

impl PublishState {
    pub fn update_publish_state(&mut self, progress: PublishProgress) {
        self.published_bytes += progress.bytes;
        self.published_statements += progress.statement_count;
        self.queued_files.retain(|(f, _)| *f != progress.filename);
        self.published_files.push(progress.filename);
    }
}

#[derive(Debug, Default)]
pub struct PublishProgress {
    pub filename: PathBuf,
    pub bytes: usize,
    pub statement_count: usize,
}
