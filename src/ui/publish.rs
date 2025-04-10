// This is free and unencumbered software released into the public domain.

use std::{collections::VecDeque, path::PathBuf};

use super::format::{format_bytes, format_number};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Text},
    widgets::{Block, Borders, LineGauge, List},
};

pub fn draw_publish(frame: &mut Frame, area: Rect, state: &PublishState, verbose: bool) {
    if !verbose {
        let [_padding, area] =
            Layout::horizontal([Constraint::Length(2), Constraint::Fill(1)]).areas(area);
        let ratio = if state.total_bytes > 0 {
            state.published_bytes as f64 / state.total_bytes as f64
        } else {
            0.0
        };
        let gauge = LineGauge::default()
            .filled_style(Style::default().fg(Color::Blue))
            .label(format!(
                "Published {} / {} ({:>2.0}%), {} batches",
                format_bytes(state.published_bytes),
                format_bytes(state.total_bytes),
                ratio * 100.0,
                format_number(state.published_files.len()),
            ))
            .ratio(ratio);
        frame.render_widget(gauge, area);
        return;
    }

    let [title_area, stats_area, current_batch_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(4),
        Constraint::Length(1),
    ])
    .spacing(1)
    .areas(area);

    let [_padding, stats_area] =
        Layout::horizontal([Constraint::Length(3), Constraint::Min(0)]).areas(stats_area);

    let block = Block::new()
        .title(Line::from("Publish Progress").left_aligned())
        .borders(Borders::TOP);
    frame.render_widget(block, title_area);

    {
        let total_statements = if let Some(ref prepare) = state.prepare {
            prepare.prepared_statements
        } else {
            state.published_statements.max(1)
        };

        let list = List::new([
            Text::from(format!(
                "Queued batches: {}",
                format_number(state.queued_files.len())
            )),
            Text::from(format!(
                "Published data: {} / {} total ({:>2.0}%)",
                format_bytes(state.published_bytes),
                format_bytes(state.total_bytes),
                if state.total_bytes > 0 {
                    state.published_bytes as f32 / state.total_bytes as f32 * 100.0
                } else {
                    0.0
                }
            )),
            Text::from(format!(
                "Published statements: {} / {} ({:>2.0}%)",
                format_number(state.published_statements),
                format_number(total_statements),
                (state.published_statements as f32 / total_statements as f32 * 100.0)
            )),
            Text::from(format!(
                "Published batches: {}",
                format_number(state.published_files.len())
            )),
        ]);

        frame.render_widget(list, stats_area);
    }

    if let Some((batch, _)) = state.queued_files.iter().next() {
        let text = Text::from(format!(
            "Next batch: {}",
            batch.file_name().and_then(std::ffi::OsStr::to_str).unwrap()
        ));
        frame.render_widget(text, current_batch_area);
    }
}

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
