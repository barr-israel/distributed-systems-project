use std::{
    error::Error,
    time::{Duration, Instant},
};

use ratatui::{
    DefaultTerminal, Frame,
    buffer::Buffer,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Stylize},
    text::{Line, Span, ToSpan},
    widgets::{Block, Clear, List, ListItem, ListState, Paragraph, StatefulWidget, Widget, Wrap},
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
enum Mode {
    Normal,
    Insert,
}
impl Mode {
    fn to_keybinds(&self) -> &str {
        match self {
            Mode::Normal => {
                "q-quit,r-reload document,Up-previous document,Down-next document,i-enter insert mode,s-save,S-force save,d-delete,D-force delete,a-add,A-force add"
            }
            Mode::Insert => "esc - exit insert mode",
        }
    }
}

impl ToSpan for Mode {
    fn to_span(&self) -> Span<'_> {
        match self {
            Mode::Normal => "NORMAL".to_span(),
            Mode::Insert => "INSERT".to_span().bg(Color::White).fg(Color::Black),
        }
    }
}

#[derive(Debug)]
struct CrabDocsApp {
    mode: Mode,
    documents: Vec<Document>,
    last_refresh: Instant,
    client: Client,
    selected_document_index: usize,
    selected_document_modified: bool,
    buffer_text: String,
    popup_text: Option<&'static str>,
    selected_document_revision: u64,
    selected_document_deleted: bool,
    prompt_input: Option<String>,
    force_add: bool,
}

impl CrabDocsApp {
    fn new() -> Self {
        let mut client = crate::networking::Client::new();
        let mut documents: Vec<Document> = client
            .read_keys()
            .into_iter()
            .map(|k| k.into_doc(false))
            .collect();
        documents.sort_unstable_by(|d1, d2| d1.name.cmp(&d2.name));
        Self {
            mode: Mode::Normal,
            client,
            last_refresh: Instant::now(),
            documents,
            selected_document_index: 0,
            selected_document_modified: false,
            buffer_text: "".to_string(),
            popup_text: None,
            selected_document_revision: 0,
            prompt_input: None,
            force_add: false,
            selected_document_deleted: false,
        }
    }

    fn get_status<'a>(&'a self) -> Span<'a> {
        if self.documents.is_empty() {
            return "NO DOCUMENTS AVAILABLE".to_span();
        }
        match (
            self.selected_document_modified,
            self.selected_document_revision
                == self.documents[self.selected_document_index].revision,
        ) {
            (true, true) => "Modified".to_span().bg(Color::Yellow).fg(Color::Black),
            (true, false) => "Modified+Out Of Date"
                .to_span()
                .bg(Color::Red)
                .fg(Color::Black),
            (false, false) => "Out Of Date".to_span().bg(Color::Red).fg(Color::Black),

            (false, true) => "Up To Date".to_span(),
        }
    }
    fn refresh_data(&mut self, force: bool) -> bool {
        let mut changes_made = false;
        let t = Instant::now();
        if force || t.duration_since(self.last_refresh) > Duration::from_millis(500) {
            let updated_keys = self.client.read_keys();
            for new_doc in updated_keys {
                // if we have an out of date document we delete its content and update the revision
                let search_result = self
                    .documents
                    .binary_search_by(|doc| doc.name.cmp(&new_doc.key));
                match search_result {
                    Ok(matching_index) => {
                        let doc = &mut self.documents[matching_index];
                        changes_made = true;
                        if doc.revision != new_doc.revision {
                            doc.revision = new_doc.revision;
                            doc.unseen_changes = true;
                        }
                    }
                    Err(index_to_insert) => {
                        changes_made = true;
                        self.documents
                            .insert(index_to_insert, new_doc.into_doc(true));
                        self.documents[index_to_insert].unseen_changes = true;
                        if index_to_insert <= self.selected_document_index {
                            if self.documents.len() > 1 {
                                self.selected_document_index += 1;
                            } else {
                                self.refresh_selected_document();
                            }
                        }
                    }
                }
            }
            self.last_refresh = Instant::now();
        }
        changes_made
    }
    fn run(mut self, terminal: &mut DefaultTerminal) -> Result<(), Box<dyn Error>> {
        self.refresh_selected_document();
        loop {
            terminal.draw(|frame: &mut Frame| {
                frame.render_widget(&mut self, frame.area());
                if self.prompt_input.is_some() {
                    self.render_prompt(frame);
                }
                if self.popup_text.is_some() {
                    self.render_popup(frame);
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
                            KeyCode::Enter => self.submit_addition(),
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
                        self.refresh_data(false);
                        break;
                    } else {
                        match self.mode {
                            Mode::Normal => {
                                if self.normal_mode_event(code) {
                                    return Ok(());
                                }
                            }
                            Mode::Insert => {
                                self.insert_mode_event(code);
                            }
                        }
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

    fn render_user(&mut self, remainder: Rect, buf: &mut Buffer) {
        let [available_documents, remainder] =
            Layout::horizontal([Constraint::Percentage(20), Constraint::Percentage(80)])
                .areas(remainder);
        let selected_document = self.documents.get(self.selected_document_index);
        let [content_status_area, content_text_area] =
            Layout::vertical([Constraint::Percentage(10), Constraint::Percentage(90)])
                .areas(remainder);
        let name = selected_document.map_or("NO DOCUMENTS", |d| d.name.as_str());
        Paragraph::new(self.buffer_text.as_str())
            .wrap(Wrap { trim: false })
            .block(
                Block::bordered()
                    .title(name)
                    .title_alignment(ratatui::layout::HorizontalAlignment::Center),
            )
            .render(content_text_area, buf);
        let mut status_text = vec![
            "Mode: ".into(),
            self.mode.to_span(),
            ", Revision: ".to_span(),
            self.selected_document_revision.to_span(),
            ", Document Status: ".to_span(),
            self.get_status(),
        ];
        if self.selected_document_deleted {
            status_text.push(", DELETED".to_span());
        }
        Paragraph::new(Line::from(status_text))
            .centered()
            .block(Block::bordered())
            .render(content_status_area, buf);
        let doc_entries = self.documents.iter().enumerate().map(|(i, doc)| {
            ListItem::from(format!(
                "{}{}",
                doc.name.as_str(),
                if doc.unseen_changes { " - !" } else { "" }
            ))
            .bg(alternate_colors(i))
        });
        let doc_list = List::new(doc_entries)
            .block(Block::bordered())
            .highlight_style(Color::Red);
        StatefulWidget::render(
            doc_list,
            available_documents,
            buf,
            &mut ListState::default().with_selected(if self.documents.is_empty() {
                None
            } else {
                Some(self.selected_document_index)
            }),
        );
    }

    fn insert_mode_event(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char(c) => self.insert_character(c),
            KeyCode::Enter => self.insert_character('\n'),
            KeyCode::Tab => self.insert_character('\t'),
            KeyCode::Backspace => self.delete_character(),
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                self.buffer_text.pop();
            }
            _ => {}
        }
    }

    fn normal_mode_event(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Char('q') => {
                return true;
            }
            KeyCode::Up => {
                self.prev_doc();
            }
            KeyCode::Down => {
                self.next_doc();
            }
            KeyCode::Char('r') => self.refresh_selected_document(),
            KeyCode::Char('i') => {
                if self.documents.is_empty() {
                    self.popup_text = Some("No Document To Edit")
                } else {
                    self.mode = Mode::Insert;
                    self.buffer_text.push(BLOCK_ASCII);
                }
            }
            KeyCode::Char('s') => self.save_selected_document(false),
            KeyCode::Char('S') => self.save_selected_document(true),
            KeyCode::Char('d') => self.delete_document(false),
            KeyCode::Char('D') => self.delete_document(true),
            KeyCode::Char('a') => self.add_document(false),
            KeyCode::Char('A') => self.add_document(true),
            _ => {}
        }
        false
    }

    fn insert_character(&mut self, c: char) {
        self.buffer_text
            .insert(self.buffer_text.len() - BLOCK_ASCII.len_utf8(), c);
        self.selected_document_modified = true;
    }

    fn delete_character(&mut self) {
        if self.buffer_text.len() > BLOCK_ASCII.len_utf8() {
            self.buffer_text
                .remove(self.buffer_text.len() - BLOCK_ASCII.len_utf8() - 1);
            self.selected_document_modified = true;
        }
    }

    fn next_doc(&mut self) {
        self.selected_document_index = if self.selected_document_index == self.documents.len() - 1 {
            0
        } else {
            self.selected_document_index + 1
        };
        self.refresh_selected_document();
    }

    fn prev_doc(&mut self) {
        self.selected_document_index = if self.selected_document_index == 0 {
            self.documents.len() - 1
        } else {
            self.selected_document_index - 1
        };
        self.refresh_selected_document();
    }

    fn refresh_selected_document(&mut self) {
        if let Some(to_update) = self.documents.get_mut(self.selected_document_index) {
            let res = self.client.read_document(&to_update.name);
            to_update.revision = res.revision;
            self.selected_document_deleted = res.value.is_none();
            self.buffer_text = res.value.unwrap_or("".to_string());
            self.selected_document_revision = self.documents[self.selected_document_index].revision;
            self.documents[self.selected_document_index].unseen_changes = false;
        } else {
            self.buffer_text = "NO DOCUMENTS AVAILABLE".to_string()
        }
        self.selected_document_modified = false;
    }

    fn render_prompt(&self, frame: &mut Frame) {
        let area = popup_area(frame.area(), 40, 6);
        let buf = frame.buffer_mut();
        let block = Block::bordered().title("Enter document name");
        let popup_inner = block.inner(area);
        Clear.render(area, buf);
        block.render(area, buf);
        let [message, remainder] =
            Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]).areas(popup_inner);
        Paragraph::new(self.prompt_input.as_ref().unwrap().as_str())
            .block(Block::bordered())
            .render(message, buf);
        let [_, submit] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(remainder);
        Line::from("Press Enter to submit, Esc to cancel").render(submit, buf);
    }

    fn render_popup(&self, frame: &mut Frame) {
        const DISMISS_TEXT: &str = "Press Esc to close message";
        let area = popup_area(
            frame.area(),
            (self.popup_text.unwrap().len().max(DISMISS_TEXT.len()) + 4) as u16,
            5,
        );
        let buf = frame.buffer_mut();
        let block = Block::bordered().title("Message");
        let popup_inner = block.inner(area);
        Clear.render(area, buf);
        block.render(area, buf);
        let [message, dismiss] =
            Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                .areas(popup_inner);
        Line::from(self.popup_text.unwrap()).render(message, buf);
        Line::from(DISMISS_TEXT).render(dismiss, buf);
    }

    fn delete_document(&mut self, force: bool) {
        let selected_document = &self.documents[self.selected_document_index];
        match self.client.delete_document(
            selected_document.name.as_str(),
            if force {
                None
            } else {
                Some(self.selected_document_revision)
            },
        ) {
            Err(_) => {
                self.popup_text = Some("Delete failed, client out of date");
            }
            Ok(rev) => {
                self.documents[self.selected_document_index].revision = rev;
                self.documents[self.selected_document_index].unseen_changes = false;
                self.selected_document_revision = rev;
                self.buffer_text = "".to_string();
                self.selected_document_modified = false;
                self.selected_document_deleted = true;
            }
        };
    }

    fn save_selected_document(&mut self, force: bool) {
        let selected_document = &self.documents[self.selected_document_index];
        match self.client.write_document(
            selected_document.name.as_str(),
            self.buffer_text.as_str(),
            if force {
                None
            } else {
                Some(self.selected_document_revision)
            },
        ) {
            Ok(rev) => {
                self.documents[self.selected_document_index].unseen_changes = false;
                self.documents[self.selected_document_index].revision = rev;
                self.selected_document_revision = rev;
                self.selected_document_modified = false;
                self.selected_document_deleted = false;
            }
            Err(_) => self.popup_text = Some("Write failed, client out of date"),
        }
    }

    fn add_document(&mut self, force: bool) {
        self.prompt_input = Some(BLOCK_ASCII.to_string());
        self.force_add = force;
    }

    fn submit_addition(&mut self) {
        let input = self.prompt_input.take().unwrap();
        let input = &input[..input.len() - BLOCK_ASCII.len_utf8()];
        if input.is_empty() {
            self.popup_text = Some("Name can't be empty");
            return;
        }
        match self
            .client
            .write_document(input, "", if self.force_add { None } else { Some(0) })
        {
            Ok(_) => {
                self.refresh_data(true);
                self.selected_document_index = self
                    .documents
                    .iter()
                    .position(|d| d.name == input)
                    .unwrap_or(0);
                self.refresh_selected_document();
            }
            Err(_) => self.popup_text = Some("Name already exists"),
        }
    }
}

impl Widget for &mut CrabDocsApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = Line::from("CrabDocs".bold());
        let block = Block::bordered().title(title.centered());
        let [remainder, keybinds] =
            Layout::vertical([Constraint::Percentage(90), Constraint::Percentage(10)])
                .areas(block.inner(area));
        Paragraph::new(self.mode.to_keybinds())
            .wrap(Wrap { trim: true })
            .block(Block::bordered())
            .render(keybinds, buf);
        self.render_user(remainder, buf);
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

fn popup_area(area: Rect, length: u16, height: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Length(height)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Length(length)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}
