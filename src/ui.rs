// This is free and unencumbered software released into the public domain.

use std::{
    collections::VecDeque,
    fmt::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

use color_eyre::Result;
use crossbeam::channel::{Receiver, Sender};
use crossterm::event;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Text},
    widgets::{Block, Borders, Gauge, List},
};

/// Prepare contains the UI state of preparation progress.
#[derive(Debug, Default)]
pub struct Prepare {
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
}

/// Publish contains the UI state of preparation progress.
#[derive(Debug, Default)]
pub struct Publish {
    pub prepare: Option<Prepare>,

    pub queued_files: VecDeque<(PathBuf, usize)>,
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

pub fn listen_input(tx: &Sender<Event>) {
    let tick_rate = Duration::from_millis(200);
    let mut last_tick = Instant::now();
    loop {
        // poll for tick rate duration, if no events, sent tick event.
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).unwrap() {
            let Some(event) = event::read()
                .map(|event| match event {
                    event::Event::Key(key) => Some(Event::Input(key)),
                    event::Event::Resize(_, _) => Some(Event::Resize),
                    _ => None,
                })
                .unwrap()
            else {
                continue;
            };
            if tx.send(event).is_err() {
                break;
            }
        }
        if last_tick.elapsed() >= tick_rate {
            tx.send(Event::Tick).ok();
            last_tick = Instant::now();
        }
    }
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
                        return Ok(());
                    }
                }
                Event::Tick => {}
                Event::Resize => terminal.autoresize()?,
                Event::Reader(progress) => {
                    match state.current_file {
                        Some(ref curr) if *curr == progress.filename => {
                            state.current_read_bytes += progress.bytes;
                        }
                        _ => {
                            let size = state
                                .queued_files
                                .iter()
                                .find(|(name, _size)| *name == progress.filename)
                                .unwrap()
                                .1;
                            state.current_file = Some(progress.filename.clone());
                            state.current_file_size = size;
                            state.current_read_bytes = progress.bytes;
                        }
                    }

                    state.read_bytes += progress.bytes;
                    state.read_statements = progress.statement_count;

                    if progress.finished {
                        state
                            .queued_files
                            .retain(|(name, _size)| *name != progress.filename);
                        state.read_files.push(progress.filename);
                        state.current_file = None;
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
                        return Ok(());
                    }
                }
                Event::Tick => {}
                Event::Resize => terminal.autoresize()?,
                Event::Reader(progress) => {
                    let prepare = state.prepare.as_mut().unwrap();

                    match prepare.current_file {
                        Some(ref curr) if *curr == progress.filename => {
                            prepare.current_read_bytes += progress.bytes;
                        }
                        _ => {
                            let size = prepare
                                .queued_files
                                .iter()
                                .find(|(name, _size)| *name == progress.filename)
                                .unwrap()
                                .1;
                            prepare.current_file = Some(progress.filename.clone());
                            prepare.current_file_size = size;
                            prepare.current_read_bytes = progress.bytes;
                        }
                    }

                    prepare.read_bytes += progress.bytes;
                    prepare.read_statements = progress.statement_count;

                    if progress.finished {
                        prepare
                            .queued_files
                            .retain(|(name, _size)| *name != progress.filename);
                        prepare.read_files.push(progress.filename);
                        prepare.current_file = None;
                    }
                }
                Event::Prepare(progress) => {
                    let prepare = state.prepare.as_mut().unwrap();
                    prepare.prepared_bytes += progress.bytes;
                    prepare.prepared_statements += progress.statement_count;
                    prepare.prepared_files.push(progress.filename.clone());
                    state.total_bytes += progress.bytes;
                    state
                        .queued_files
                        .push_back((progress.filename, progress.statement_count));
                }
                Event::Publish(progress) => {
                    state.published_bytes += progress.bytes;
                    state.published_statements += progress.statement_count;
                    state.queued_files.retain(|(f, _)| *f != progress.filename);
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

fn draw_publish(frame: &mut Frame, area: Rect, state: &Publish) {
    let [title_area, stats_area, current_batch_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(4),
        Constraint::Length(1),
    ])
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
            Text::from(format!(
                "Queued batches: {}",
                format_number(state.queued_files.len())
            )),
            Text::from(format!(
                "Publish data: {} / {} total ({:>2.0}%)",
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
        let text = Text::from(format!("Next batch: {}", batch.display()));
        frame.render_widget(text, current_batch_area);
    }
}

///
/// ```
/// # use asimov_dataset_cli::ui::format_bytes;
/// assert_eq!("256 B", format_bytes(256).as_str());
/// assert_eq!("999 B", format_bytes(999).as_str());
/// assert_eq!("1.0 KB", format_bytes(1024).as_str());
/// assert_eq!("4.1 KB", format_bytes(1<<12).as_str());
/// assert_eq!("524.3 KB", format_bytes(1<<19).as_str());
/// assert_eq!("2.1 MB", format_bytes((1<<21)+1).as_str());
/// assert_eq!("2.1 MB", format_bytes((1<<21)+500).as_str());
/// assert_eq!("1.1 GB", format_bytes((1<<30)).as_str());
/// assert_eq!("1.0 GB", format_bytes(1000*1000*1000).as_str());
/// assert_eq!("4.5 PB", format_bytes(1<<52).as_str());
/// ```
pub fn format_bytes(n: usize) -> String {
    const KB: usize = 1_000;
    const MB: usize = KB * 1000;
    const GB: usize = MB * 1000;
    const TB: usize = GB * 1000;
    const PB: usize = TB * 1000;

    match n {
        ..KB => format!("{n} B"),
        KB..MB => format!("{:.1} KB", (n as f64 / KB as f64)),
        MB..GB => format!("{:.1} MB", (n as f64 / MB as f64)),
        GB..TB => format!("{:.1} GB", (n as f64 / GB as f64)),
        TB..PB => format!("{:.1} TB", (n as f64 / TB as f64)),
        PB.. => format!("{:.1} PB", (n as f64 / PB as f64)),
    }
}

/// ```
/// # use asimov_dataset_cli::ui::format_number;
/// assert_eq!("123", format_number(123).as_str());
/// assert_eq!("1_234", format_number(1234).as_str());
/// assert_eq!("123_456", format_number(123456).as_str());
/// assert_eq!("1_234_567", format_number(1234567).as_str());
/// ```
pub fn format_number(n: usize) -> String {
    let mut out = String::new();
    let digits = n.to_string();
    let len = digits.len();

    for (i, c) in digits.chars().enumerate() {
        out.push(c);
        // Add underscore after every 3rd digit from the right, except at the end
        if (len - i - 1) % 3 == 0 && i < len - 1 {
            out.push('_');
        }
    }

    out
}
