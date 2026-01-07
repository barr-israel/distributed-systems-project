use std::{error::Error, time::Duration};

use ratatui::{
    DefaultTerminal, Frame,
    buffer::Buffer,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Stylize},
    text::Line,
    widgets::{Block, Clear, Paragraph, Widget, Wrap},
};
use serde::Deserialize;

use crate::networking::Client;

mod networking;

fn main() {
    // dbg!(Client::new().read_document("hello"));
    ratatui::run(|terminal| CrabDocsApp::new().run(terminal)).unwrap()
}

const BLOCK_ASCII: char = char::from_u32(11036).unwrap();

#[derive(Debug, Deserialize)]
pub struct Document {
    pub name: String,
    pub revision: u64,
    pub unseen_changes: bool,
}

#[derive(Debug)]
struct CrabDocsApp {
    client: Client,
    prompt_input: Option<String>,
    popup_text: Option<&'static str>,
}

impl CrabDocsApp {
    fn new() -> Self {
        let client = crate::networking::Client::new();
        Self {
            client,
            prompt_input: None,
            popup_text: None,
        }
    }

    fn run(mut self, terminal: &mut DefaultTerminal) -> Result<(), Box<dyn Error>> {
        loop {
            terminal.draw(|frame: &mut Frame| {
                frame.render_widget(&mut self, frame.area());
                if self.prompt_input.is_some() {
                    let area = popup_area(frame.area(), 40, 11);
                    self.render_prompt(area, frame.buffer_mut());
                }
                if self.popup_text.is_some() {
                    let area = popup_area(frame.area(), 40, 10);
                    self.render_popup(area, frame.buffer_mut());
                }
            })?;
            loop {
                if event::poll(Duration::from_secs(1))?
                    && let Event::Key(KeyEvent {
                        code,
                        modifiers: _,
                        kind: KeyEventKind::Press,
                        state: _,
                    }) = event::read()?
                {
                    if self.popup_text.is_some() {
                        if code.is_esc() {
                            self.popup_text = None
                        }
                    } else if self.prompt_input.is_some() {
                        match code {
                            KeyCode::Enter => self.submit_prompt(),
                            KeyCode::Esc => self.prompt_input = None,
                            KeyCode::Backspace => {
                                let buf = self.prompt_input.as_mut().unwrap();
                                if buf.len() > BLOCK_ASCII.len_utf8() {
                                    buf.remove(buf.len() - BLOCK_ASCII.len_utf8() - 1);
                                }
                            }
                            KeyCode::Char(c) => {
                                let buf = self.prompt_input.as_mut().unwrap();
                                buf.insert(buf.len() - BLOCK_ASCII.len_utf8(), c);
                            }
                            _ => {}
                        }
                        break;
                    } else {
                        self.admin_mode_event(code);
                    }
                    self.refresh_data(false);
                    break;
                }
                if self.refresh_data(false) {
                    break;
                }
            }
        }
    }

    fn render_admin(&mut self, remainder: Rect, buf: &mut Buffer) {
        todo!()
    }

    fn admin_mode_event(&mut self, code: KeyCode) {
        todo!()
    }

    fn open_prompt(&mut self) {
        self.prompt_input = Some(BLOCK_ASCII.to_string())
    }

    fn render_prompt(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered().title("Enter document name");
        let popup_inner = block.inner(area);
        Clear.render(area, buf);
        block.render(area, buf);
        let [message, remainder] =
            Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(popup_inner);
        Paragraph::new(self.prompt_input.as_ref().unwrap().as_str())
            .block(Block::bordered())
            .render(message, buf);
        let [_, submit] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(remainder);
        Line::from("Press Enter to submit, Esc to cancel").render(submit, buf);
    }

    fn render_popup(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered().title("Message");
        let popup_inner = block.inner(area);
        Clear.render(area, buf);
        block.render(area, buf);
        let [message, dismiss] =
            Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                .areas(popup_inner);
        Line::from(self.popup_text.unwrap()).render(message, buf);
        Line::from("Press Esc to close").render(dismiss, buf);
    }

    fn refresh_data(&self, force: bool) -> bool {
        todo!()
    }

    fn submit_prompt(&mut self) {
        let input = self.prompt_input.take().unwrap();
        let input = &input[..input.len() - BLOCK_ASCII.len_utf8()];
        if input.is_empty() {
            self.popup_text = Some("Input can't be empty");
            return;
        }
        todo!()
    }
}

impl Widget for &mut CrabDocsApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = Line::from("CrabDocs".bold());
        let block = Block::bordered().title(title.centered());
        let [remainder, keybinds] =
            Layout::vertical([Constraint::Percentage(90), Constraint::Percentage(10)])
                .areas(block.inner(area));
        Paragraph::new("KEYBINDS HERE")
            .wrap(Wrap { trim: true })
            .block(Block::bordered())
            .render(keybinds, buf);
        self.render_admin(remainder, buf);
        block.render(area, buf);
    }
}

const NORMAL_ROW_BG: Color = Color::Black;
const ALT_ROW_BG: Color = Color::DarkGray;

const fn alternate_colors(i: usize) -> Color {
    if i.is_multiple_of(2) {
        NORMAL_ROW_BG
    } else {
        ALT_ROW_BG
    }
}

fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}
