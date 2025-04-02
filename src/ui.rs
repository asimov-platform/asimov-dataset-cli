// This is free and unencumbered software released into the public domain.

use std::{collections::VecDeque, path::PathBuf};

use color_eyre::Result;
use crossbeam::channel::Receiver;
use crossterm::event;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Text},
    widgets::{Block, Borders, Gauge, List},
};

#[derive(Debug, Default)]
pub struct Prepare {
    pub current_file: PathBuf,
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
}

#[derive(Debug, Default)]
pub struct Publish {
    pub prepare: Option<Prepare>,

    pub queued_files: VecDeque<PathBuf>,
    pub total_bytes: usize,

    pub published_bytes: usize,
    pub published_files: Vec<PathBuf>,
    pub published_statements: usize,
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
}

#[derive(Debug, Default)]
pub struct PublishProgress {
    pub filename: PathBuf,
    pub bytes: usize,
    pub statement_count: usize,
}

pub enum Event {
    Resize,
    Tick,
    Input(event::KeyEvent),
    Reader(ReaderProgress),
    Prepare(PrepareProgress),
    Publish(PublishProgress),
}

pub fn run_prepare(
    terminal: &mut DefaultTerminal,
    mut state: Prepare,
    rx: Receiver<Event>,
) -> Result<()> {
    loop {
        terminal.draw(|frame| draw_prepare(frame, frame.area(), &state))?;

        match rx.recv() {
            Err(_) => return Ok(()),
            Ok(event) => match event {
                Event::Input(event) => {
                    if event.code == event::KeyCode::Char('q') {
                        break Ok(());
                    }
                }
                Event::Tick => {}
                Event::Resize => terminal.autoresize()?,
                Event::Reader(progress) => {
                    if state.current_file != progress.filename {
                        let size = state
                            .queued_files
                            .iter()
                            .find(|(name, _size)| *name == progress.filename)
                            .unwrap()
                            .1;
                        state.current_file = progress.filename.clone();
                        state.current_file_size = size;
                        state.current_read_bytes = progress.bytes;
                    } else {
                        state.current_read_bytes += progress.bytes;
                    }

                    state.read_bytes += progress.bytes;
                    state.read_statements = progress.statement_count;

                    if progress.finished {
                        state
                            .queued_files
                            .retain(|(name, _size)| *name != progress.filename);
                        state.read_files.push(progress.filename);
                    }
                }
                Event::Prepare(progress) => {
                    state.prepared_bytes += progress.bytes;
                    state.prepared_statements += progress.statement_count;
                    state.prepared_files.push(progress.filename);
                }
                Event::Publish(_) => unreachable!(),
            },
        }
    }
}

pub fn run_publish(
    terminal: &mut DefaultTerminal,
    mut state: Publish,
    rx: Receiver<Event>,
) -> Result<()> {
    loop {
        terminal.draw(|frame| {
            if let Some(ref prepare) = state.prepare {
                let [prepare_area, publish_area] =
                    Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)])
                        .margin(2)
                        .areas(frame.area());

                draw_prepare(frame, prepare_area, prepare);
                draw_publish(frame, publish_area, &state);
            } else {
                draw_publish(frame, frame.area(), &state);
            }
        })?;

        match rx.recv() {
            Err(_) => return Ok(()),
            Ok(event) => match event {
                Event::Input(event) => {
                    if event.code == event::KeyCode::Char('q') {
                        break Ok(());
                    }
                }
                Event::Tick => {}
                Event::Resize => terminal.autoresize()?,
                Event::Reader(progress) => {
                    let prepare = state.prepare.as_mut().unwrap();
                    if prepare.current_file != progress.filename {
                        let (_file, size) = prepare
                            .queued_files
                            .iter()
                            .find(|name| name.0 == progress.filename)
                            .unwrap();
                        prepare.current_file = progress.filename.clone();
                        prepare.current_file_size = *size;
                        prepare.current_read_bytes = progress.bytes;
                    } else {
                        prepare.current_read_bytes += progress.bytes;
                    }
                    prepare.read_bytes += progress.bytes;
                    prepare.read_statements += progress.statement_count;
                    if progress.finished {
                        prepare.queued_files.pop_front();
                        prepare.read_files.push(progress.filename);
                    }
                }
                Event::Prepare(progress) => {
                    let prepare = state.prepare.as_mut().unwrap();
                    prepare.prepared_bytes += progress.bytes;
                    prepare.prepared_statements += progress.statement_count;
                    prepare.prepared_files.push(progress.filename);
                }
                Event::Publish(progress) => {
                    state.published_bytes += progress.bytes;
                    state.published_statements += progress.statement_count;
                    state.published_files.push(progress.filename);
                }
            },
        }
    }
}

fn draw_prepare(frame: &mut Frame, area: Rect, state: &Prepare) {
    let [title_area, stats_area, current_file_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(6),
        Constraint::Length(1),
    ])
    .margin(1)
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
            Text::from(format!("Queued files: {}", state.queued_files.len())),
            Text::from(format!(
                "Read bytes: {} / {} total ({:>2.0}%)",
                state.read_bytes,
                state.total_bytes,
                (state.read_bytes as f32 / state.total_bytes as f32 * 100.0)
            )),
            Text::from(format!("Read statements: {}", state.read_statements)),
            Text::from(format!(
                "Prepared statements: {} / {} ({:>2.0}%)",
                state.prepared_statements,
                state.read_statements,
                (state.prepared_statements as f32 / state.read_statements as f32 * 100.0)
            )),
            Text::from(format!("Created batches: {}", state.prepared_files.len())),
            Text::from(format!("Size of batches: {} bytes", state.prepared_bytes)),
        ]);

        frame.render_widget(list, stats_area);
    }

    if state.current_read_bytes > 0 {
        let [text_area, gauge_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(current_file_area);
        let text = Text::from(format!("Processing file {}", state.current_file.display()));
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

fn draw_publish(frame: &mut Frame, area: Rect, state: &Publish) {
    let [title_area, stats_area] = Layout::vertical([Constraint::Length(1), Constraint::Length(6)])
        .margin(1)
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
            Text::from(format!("Queued files: {}", state.queued_files.len())),
            Text::from(format!(
                "Published bytes: {} / {} total ({:>2}%)",
                state.published_bytes,
                state.total_bytes,
                if state.total_bytes > 0 {
                    state.published_bytes as f32 / state.total_bytes as f32 * 100.0
                } else {
                    0.0
                }
            )),
            Text::from(format!(
                "Published statements: {} / {} ({:>2}%)",
                state.published_statements,
                total_statements,
                (state.published_statements as f32 / total_statements as f32 * 100.0)
            )),
            Text::from(format!("Published files: {}", state.published_files.len())),
        ]);

        frame.render_widget(list, stats_area);
    }
}
