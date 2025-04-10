// This is free and unencumbered software released into the public domain.

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use crossterm::event::{self, KeyCode, KeyModifiers};
use eyre::Result;
use futures::StreamExt;
use prepare::draw_prepare;
use publish::draw_publish;
use ratatui::{
    DefaultTerminal,
    layout::{Constraint, Layout},
};

mod format;
mod prepare;
mod publish;

pub use prepare::{PrepareProgress, PrepareState, ReaderProgress};
pub use publish::{PublishProgress, PublishState};

pub enum UIEvent {
    Input(event::KeyEvent),
    Resize,
}

pub enum Event {
    Reader(ReaderProgress),
    Prepare(PrepareProgress),
    Publish(PublishProgress),
}

pub async fn listen_input(tx: Sender<UIEvent>) -> Result<()> {
    let mut stream = crossterm::event::EventStream::new();
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                let event = match event {
                    event::Event::Key(key) => UIEvent::Input(key),
                    event::Event::Resize(_, _) => UIEvent::Resize,
                    _ => continue,
                };

                if tx.send(event).is_err() {
                    return Ok(());
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

pub fn run_prepare<T: FnOnce()>(
    terminal: &mut DefaultTerminal,
    verbose: bool,
    mut state: PrepareState,
    input_rx: Receiver<UIEvent>,
    progress_rx: Receiver<Event>,
    quit_callback: T,
) -> Result<()> {
    loop {
        terminal.draw(|frame| draw_prepare(frame, frame.area(), &state, verbose))?;

        match input_rx.try_recv() {
            Ok(event) => match event {
                UIEvent::Input(event) => {
                    if event.code == KeyCode::Char('q')
                        || (event.code == KeyCode::Char('c')
                            && event.modifiers == KeyModifiers::CONTROL)
                    {
                        quit_callback();
                        return Ok(());
                    }
                }
                UIEvent::Resize => terminal.autoresize()?,
            },
            Err(TryRecvError::Empty) => {}
            Err(err) => panic!("{err}"),
        }

        match progress_rx.recv() {
            Err(_) => return Ok(()), // no more updates, exit
            Ok(event) => match event {
                Event::Reader(progress) => state.update_reader_state(progress),
                Event::Prepare(progress) => state.update_prepare_state(progress),
                Event::Publish(_) => unreachable!(),
            },
        }
    }
}

pub fn run_publish<T: FnOnce()>(
    terminal: &mut DefaultTerminal,
    verbose: bool,
    mut state: PublishState,
    input_rx: Receiver<UIEvent>,
    progress_rx: Receiver<Event>,
    quit_callback: T,
) -> Result<()> {
    loop {
        terminal.draw(|frame| {
            if let Some(ref prepare) = state.prepare {
                let [prepare_area, publish_area] =
                    Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)])
                        .areas(frame.area());

                draw_prepare(frame, prepare_area, prepare, verbose);
                draw_publish(frame, publish_area, &state, verbose);
            } else {
                draw_publish(frame, frame.area(), &state, verbose);
            }
        })?;

        match input_rx.try_recv() {
            Ok(event) => match event {
                UIEvent::Input(event) => {
                    if event.code == KeyCode::Char('q')
                        || (event.code == KeyCode::Char('c')
                            && event.modifiers == KeyModifiers::CONTROL)
                    {
                        quit_callback();
                        return Ok(());
                    }
                }
                UIEvent::Resize => terminal.autoresize()?,
            },
            Err(TryRecvError::Empty) => {}
            Err(err) => panic!("{err}"),
        }

        match progress_rx.recv() {
            Err(_) => return Ok(()),
            Ok(event) => match event {
                Event::Reader(progress) => state
                    .prepare
                    .as_mut()
                    .unwrap()
                    .update_reader_state(progress),
                Event::Prepare(progress) => {
                    state.total_bytes += progress.bytes;
                    state
                        .queued_files
                        .push_back((progress.filename.clone(), progress.statement_count));
                    state
                        .prepare
                        .as_mut()
                        .unwrap()
                        .update_prepare_state(progress);
                }
                Event::Publish(progress) => state.update_publish_state(progress),
            },
        }
    }
}
