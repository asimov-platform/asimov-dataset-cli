// This is free and unencumbered software released into the public domain.

use std::{collections::VecDeque, path::PathBuf};

use super::format::{format_bytes, format_number};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Text},
    widgets::{Block, Borders, Gauge, LineGauge, List},
};

pub fn draw_prepare(frame: &mut Frame, area: Rect, state: &PrepareState, verbose: bool) {
    if !verbose {
        let [_padding, area] =
            Layout::horizontal([Constraint::Length(2), Constraint::Fill(1)]).areas(area);
        let ratio = if state.total_bytes > 0 {
            state.read_bytes as f64 / state.total_bytes as f64
        } else {
            0.0
        };
        let gauge = LineGauge::default()
            .filled_style(Style::default().fg(Color::Blue))
            .label(format!(
                "Prepared {} / {} ({:>2.0}%) to {} batches ({})",
                format_bytes(state.read_bytes),
                format_bytes(state.total_bytes),
                ratio * 100.0,
                format_number(state.prepared_files.len()),
                format_bytes(state.prepared_bytes),
            ))
            .ratio(ratio);
        frame.render_widget(gauge, area);
        return;
    }

    let [title_area, stats_area, current_file_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(7),
        Constraint::Length(1),
    ])
    .spacing(1)
    .areas(area);

    let [_padding, stats_area] =
        Layout::horizontal([Constraint::Length(3), Constraint::Min(0)]).areas(stats_area);

    let block = Block::new()
        .title(Line::from("Prepare Progress").left_aligned())
        .borders(Borders::TOP);
    frame.render_widget(block, title_area);

    {
        let list = List::new([
            Text::from(format!(
                "Queued files: {}",
                format_number(state.queued_files.len())
            )),
            Text::from(format!(
                "Read data: {} / {} total ({:>2.0}%)",
                format_bytes(state.read_bytes),
                format_bytes(state.total_bytes),
                (state.read_bytes as f32 / state.total_bytes as f32 * 100.0)
            )),
            Text::from(format!(
                "Read statements: {}",
                format_number(state.read_statements)
            )),
            Text::from(format!(
                "Prepared statements: {} / {} ({:>2.0}%)",
                format_number(state.prepared_statements),
                format_number(state.read_statements),
                (state.prepared_statements as f32 / state.read_statements as f32 * 100.0)
            )),
            Text::from(format!(
                "Skipped statements: {}",
                format_number(state.skipped_statemets),
            )),
            Text::from(format!(
                "Prepared batches: {}",
                format_number(state.prepared_files.len())
            )),
            Text::from(format!(
                "Total size of batches: {}",
                format_bytes(state.prepared_bytes)
            )),
        ]);

        frame.render_widget(list, stats_area);
    }

    if let Some(ref current_file) = state.current_file {
        let [text_area, gauge_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(current_file_area);
        let text = Text::from(format!("Processing file {}", current_file.display()));
        frame.render_widget(text, text_area);
        let gauge = Gauge::default()
            .gauge_style(Style::default().fg(Color::LightGreen))
            .ratio(state.current_read_bytes as f64 / state.current_file_size as f64);
        frame.render_widget(
            gauge,
            Rect {
                x: gauge_area.left(),
                y: gauge_area.top(),
                width: gauge_area.width,
                height: 1,
            },
        );
    }
}

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
