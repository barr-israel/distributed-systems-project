use std::{
    error::Error,
    fmt::Display,
    time::{Duration, Instant},
};

use ratatui::{
    DefaultTerminal, Frame,
    buffer::Buffer,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Layout, Rect},
    style::{Color, Stylize},
    text::Line,
    widgets::{Block, List, ListItem, ListState, Paragraph, StatefulWidget, Tabs, Widget, Wrap},
};

use crate::{document::Document, networking::Client};

mod document;
mod networking;

fn main() {
    // dbg!(Client::new().read_document("hello"));
    ratatui::run(|terminal| CrabDocsApp::new().run(terminal)).unwrap()
}

#[derive(Debug)]
enum Mode {
    Normal,
    Insert,
    Admin,
}

#[derive(Debug)]
enum DocumentStatus {
    UpToDate,
    OutOfDate,
}

impl Display for DocumentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DocumentStatus::UpToDate => "Up To Date",
            DocumentStatus::OutOfDate => "Out Of Date",
        })
    }
}

#[derive(Debug)]
struct CrabDocsApp {
    mode: Mode,
    documents: Vec<Document>,
    last_refresh: Instant,
    client: Client,
    selected_document_index: usize,
    selected_document_status: DocumentStatus,
    // TODO: maybe CoW?
    buffer_text: String,
}

impl CrabDocsApp {
    fn new() -> Self {
        let mut client = crate::networking::Client::new();
        let mut documents = client.read_keys();
        documents.sort_unstable_by(|d1, d2| d1.name.cmp(&d2.name));
        let first_buffer = if let Some(first_doc) = documents.first_mut() {
            let res = client.read_document(&first_doc.name);
            first_doc.content = Some(res.value.clone());
            first_doc.revision = res.revision;
            res.value
        } else {
            "NO DOCUMENTS AVAILABLE".to_string()
        };
        Self {
            mode: Mode::Normal,
            client,
            last_refresh: Instant::now(),
            documents,
            selected_document_index: 0,
            selected_document_status: DocumentStatus::UpToDate,
            buffer_text: first_buffer,
        }
    }

    fn refresh_data(&mut self) -> bool {
        let mut changes_made = false;
        let t = Instant::now();
        if t.duration_since(self.last_refresh) > Duration::from_millis(500) {
            let updated_keys = self.client.read_keys();
            for new_doc in updated_keys {
                // if we have an out of date document we delete its content and update the revision
                let search_result = self
                    .documents
                    .binary_search_by(|doc| doc.name.cmp(&new_doc.name));
                match search_result {
                    Ok(matching_index) => {
                        let doc = &mut self.documents[matching_index];
                        changes_made = true;
                        if doc.revision != new_doc.revision {
                            doc.revision = new_doc.revision;
                            doc.content = None;
                            // notify if the document that changed was the selected one
                            if self.selected_document_index == matching_index {
                                self.selected_document_status = DocumentStatus::OutOfDate
                            }
                        }
                    }
                    Err(index_to_insert) => {
                        changes_made = true;
                        self.documents.insert(index_to_insert, new_doc);
                        if index_to_insert <= self.selected_document_index {
                            self.selected_document_index += 1;
                        }
                    }
                }
            }
            self.last_refresh = Instant::now();
        }
        changes_made
    }
    fn run(mut self, terminal: &mut DefaultTerminal) -> Result<(), Box<dyn Error>> {
        loop {
            terminal.draw(|frame: &mut Frame| {
                frame.render_widget(&mut self, frame.area());
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
                    match self.mode {
                        Mode::Normal => {
                            if self.normal_mode_event(code) {
                                return Ok(());
                            }
                        }
                        Mode::Insert => {
                            self.insert_mode_event(code);
                        }
                        Mode::Admin => self.admin_mode_event(code),
                    }
                    break;
                }
                if self.refresh_data() {
                    break;
                }
            }
        }
    }

    fn render_admin(&mut self, remainder: Rect, buf: &mut Buffer) {
        todo!()
    }

    fn render_user(&mut self, remainder: Rect, buf: &mut Buffer) {
        let [available_documents, content_area] =
            Layout::horizontal([Constraint::Percentage(20), Constraint::Percentage(80)])
                .areas(remainder);
        let selected_document = &self.documents[self.selected_document_index];
        let [content_status_area, content_text_area] =
            Layout::vertical([Constraint::Percentage(10), Constraint::Percentage(90)])
                .areas(content_area);
        Paragraph::new(self.buffer_text.as_str())
            .wrap(Wrap { trim: false })
            .block(Block::bordered())
            .render(content_text_area, buf);
        Paragraph::new(format!(
            "Name: {}, Revision: {}, Status: {}",
            selected_document.name, selected_document.revision, self.selected_document_status
        ))
        .centered()
        .block(Block::bordered())
        .render(content_status_area, buf);
        let doc_entries = self
            .documents
            .iter()
            .enumerate()
            .map(|(i, doc)| ListItem::from(doc.name.as_str()).bg(alternate_colors(i)));
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
            KeyCode::Esc => self.mode = Mode::Normal,
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
            KeyCode::Tab => self.mode = Mode::Admin,
            KeyCode::Char('i') => self.mode = Mode::Insert,
            _ => {}
        }
        false
    }

    fn admin_mode_event(&mut self, code: KeyCode) {
        todo!()
    }

    fn insert_character(&mut self, c: char) {
        todo!()
    }

    fn delete_character(&mut self) {
        todo!()
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

    fn fill_document(&mut self, index: usize) {
        let to_update = &mut self.documents[index];
        let res = self.client.read_document(&to_update.name);
        to_update.content = Some(res.value);
        to_update.revision = res.revision;
    }

    fn refresh_selected_document(&mut self) {
        self.fill_document(self.selected_document_index);
        self.buffer_text = self.documents[self.selected_document_index]
            .content
            .clone()
            .unwrap();
        self.selected_document_status = DocumentStatus::UpToDate
    }
}

impl Widget for &mut CrabDocsApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = Line::from("CrabDocs".bold());
        let block = Block::bordered().title(title.centered());
        let [remainder, tabs_area] =
            Layout::vertical([Constraint::Percentage(95), Constraint::Percentage(5)])
                .areas(block.inner(area));
        Tabs::new(["User", "Admin"])
            .select(if matches!(self.mode, Mode::Admin) {
                1
            } else {
                0
            })
            .block(Block::bordered())
            .render(tabs_area, buf);
        if matches!(self.mode, Mode::Admin) {
            self.render_admin(remainder, buf);
        } else {
            self.render_user(remainder, buf);
        }
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
