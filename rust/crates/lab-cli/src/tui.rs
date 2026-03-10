use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, BorderType, Borders, Cell, Gauge, Paragraph, Row, Table, TableState, Wrap},
};
use serde_json::Value;
use std::io::{self, Stdout};
use std::time::Duration;

const APP_BG: Color = Color::Rgb(14, 18, 22);
const PANEL_BG: Color = Color::Rgb(24, 29, 34);
const PANEL_ALT_BG: Color = Color::Rgb(30, 36, 42);
const BORDER: Color = Color::Rgb(63, 74, 82);
const ACCENT: Color = Color::Rgb(102, 212, 196);
const ACCENT_SOFT: Color = Color::Rgb(41, 66, 70);
const TEXT: Color = Color::Rgb(236, 232, 224);
const MUTED: Color = Color::Rgb(144, 153, 160);
const SUCCESS: Color = Color::Rgb(122, 229, 130);
const WARNING: Color = Color::Rgb(255, 194, 82);
const DANGER: Color = Color::Rgb(255, 120, 112);

pub enum Action {
    Quit,
    Back,
    Select,
    Refresh,
    ScrollUp,
    ScrollDown,
    PageUp,
    PageDown,
    Tick,
}

#[derive(Clone, Copy, Debug)]
pub struct KeyHint {
    pub key: &'static str,
    pub label: &'static str,
}

#[derive(Clone, Debug)]
pub struct RunBrowserItem {
    pub run_id: String,
    pub experiment: String,
    pub started_at: String,
    pub status: String,
    pub status_detail: String,
    pub active_trials: usize,
}

#[derive(Clone, Debug)]
pub struct ViewBrowserItem {
    pub name: String,
    pub source_view: String,
    pub purpose: String,
}

pub struct RunBrowserState<'a> {
    pub items: &'a [RunBrowserItem],
    pub refresh_secs: u64,
}

pub struct ViewBrowserState<'a> {
    pub run_id: &'a str,
    pub experiment: &'a str,
    pub started_at: &'a str,
    pub status: &'a str,
    pub items: &'a [ViewBrowserItem],
    pub refresh_secs: u64,
}

pub struct ViewState<'a> {
    pub run_id: &'a str,
    pub status: &'a str,
    pub started_at: &'a str,
    pub view_name: &'a str,
    pub interval_secs: u64,
    pub table: &'a lab_analysis::QueryTable,
    pub progress: Option<(usize, usize)>,
    pub legend: &'a [(String, String)],
    pub split_labels: Option<(&'a str, &'a str)>,
    pub hints: &'a [KeyHint],
}

pub enum Screen<'a> {
    RunBrowser(RunBrowserState<'a>),
    ViewBrowser(ViewBrowserState<'a>),
    LiveView(ViewState<'a>),
}

pub struct Term {
    terminal: ratatui::Terminal<CrosstermBackend<Stdout>>,
    table_state: TableState,
}

impl Term {
    pub fn new() -> anyhow::Result<Self> {
        enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(io::stdout());
        let terminal = ratatui::Terminal::new(backend)?;
        Ok(Self {
            terminal,
            table_state: TableState::default(),
        })
    }

    pub fn draw(&mut self, screen: &Screen) -> anyhow::Result<()> {
        let table_state = &mut self.table_state;
        self.terminal.draw(|f| render(f, screen, table_state))?;
        Ok(())
    }

    pub fn poll(&self, timeout: Duration) -> anyhow::Result<Action> {
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    return Ok(match key.code {
                        KeyCode::Char('q') => Action::Quit,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            Action::Quit
                        }
                        KeyCode::Char('r') => Action::Refresh,
                        KeyCode::Esc
                        | KeyCode::Backspace
                        | KeyCode::Left
                        | KeyCode::Char('h') => Action::Back,
                        KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => Action::Select,
                        KeyCode::Up | KeyCode::Char('k') => Action::ScrollUp,
                        KeyCode::Down | KeyCode::Char('j') => Action::ScrollDown,
                        KeyCode::PageUp => Action::PageUp,
                        KeyCode::PageDown => Action::PageDown,
                        _ => Action::Tick,
                    });
                }
            }
        }
        Ok(Action::Tick)
    }

    pub fn scroll_up(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some(i.saturating_sub(1)));
    }

    pub fn scroll_down(&mut self, max: usize) {
        if max == 0 {
            self.table_state.select(None);
            return;
        }
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state
            .select(Some((i + 1).min(max.saturating_sub(1))));
    }

    pub fn page_up(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some(i.saturating_sub(12)));
    }

    pub fn page_down(&mut self, max: usize) {
        if max == 0 {
            self.table_state.select(None);
            return;
        }
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state
            .select(Some((i + 12).min(max.saturating_sub(1))));
    }

    pub fn selected(&self) -> Option<usize> {
        self.table_state.selected()
    }

    pub fn set_selected(&mut self, idx: Option<usize>) {
        self.table_state.select(idx);
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(LeaveAlternateScreen);
    }
}

fn render(f: &mut Frame, screen: &Screen, table_state: &mut TableState) {
    match screen {
        Screen::RunBrowser(state) => render_run_browser(f, state, table_state),
        Screen::ViewBrowser(state) => render_view_browser(f, state, table_state),
        Screen::LiveView(state) => render_live_view(f, state, table_state),
    }
}

fn render_run_browser(f: &mut Frame, state: &RunBrowserState, table_state: &mut TableState) {
    paint_app_background(f);
    let shell = chrome_block("Live View Browser", "Runs");
    let inner = shell.inner(f.area());
    f.render_widget(shell, f.area());

    let sections = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(12),
        Constraint::Length(2),
    ])
    .split(inner);

    render_browser_header(
        f,
        sections[0],
        "Runs",
        "Active runs are pinned first. Pick one, then choose the exact view you want to inspect.",
        state.refresh_secs,
        state.items.len(),
    );

    if state.items.is_empty() {
        render_empty_panel(
            f,
            sections[1],
            "No active runs right now",
            "This screen keeps polling. Start a run and it will appear here.",
        );
        render_browser_footer(
            f,
            sections[2],
            "q quit",
            "Runs refresh automatically",
            None,
        );
        return;
    }

    let selected = ensure_selection(table_state, state.items.len());
    let columns = Layout::horizontal([Constraint::Percentage(63), Constraint::Percentage(37)])
        .split(sections[1]);

    let header = Row::new(["run", "experiment", "started", "state", "active"])
        .style(table_header_style())
        .height(1);

    let rows = state.items.iter().enumerate().map(|(idx, item)| {
        let bg = striped_bg(idx);
        Row::new(vec![
            Cell::from(item.run_id.as_str()).style(Style::default().fg(TEXT).bg(bg)),
            Cell::from(item.experiment.as_str()).style(Style::default().fg(TEXT).bg(bg)),
            Cell::from(item.started_at.as_str()).style(Style::default().fg(MUTED).bg(bg)),
            Cell::from(item.status.as_str()).style(status_style(item.status.as_str()).bg(bg)),
            Cell::from(item.active_trials.to_string()).style(
                Style::default()
                    .fg(if item.active_trials > 0 { ACCENT } else { MUTED })
                    .bg(bg),
            ),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(32),
            Constraint::Min(18),
            Constraint::Length(24),
            Constraint::Length(12),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .block(panel_block("Runs"))
    .row_highlight_style(selected_row_style())
    .column_spacing(1);
    f.render_stateful_widget(table, columns[0], table_state);

    let selected_item = &state.items[selected];
    let details = Text::from(vec![
        key_value_line("Run", selected_item.run_id.as_str()),
        key_value_line("Experiment", selected_item.experiment.as_str()),
        key_value_line("Started", selected_item.started_at.as_str()),
        key_value_line("Status", selected_item.status_detail.as_str()),
        key_value_line("Active trials", &selected_item.active_trials.to_string()),
        Line::default(),
        Line::from(vec![Span::styled(
            "Enter opens the view menu for this run.",
            Style::default().fg(MUTED),
        )]),
    ]);
    let detail_card = Paragraph::new(details)
        .block(panel_block("Selected Run"))
        .wrap(Wrap { trim: true })
        .style(Style::default().bg(PANEL_BG));
    f.render_widget(detail_card, columns[1]);

    render_browser_footer(
        f,
        sections[2],
        "Enter choose run",
        "Esc/q quit",
        Some("↑↓ move"),
    );
}

fn render_view_browser(f: &mut Frame, state: &ViewBrowserState, table_state: &mut TableState) {
    paint_app_background(f);
    let shell = chrome_block("Live View Browser", state.run_id);
    let inner = shell.inner(f.area());
    f.render_widget(shell, f.area());

    let sections = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(12),
        Constraint::Length(2),
    ])
    .split(inner);

    render_browser_header(
        f,
        sections[0],
        "Views",
        "Choose the lens: progress, scoreboard, task outcomes, trace, or any other standard surface.",
        state.refresh_secs,
        state.items.len(),
    );

    if state.items.is_empty() {
        render_empty_panel(
            f,
            sections[1],
            "No standard views available",
            "This run exists, but the standardized view surface could not be resolved.",
        );
        render_browser_footer(
            f,
            sections[2],
            "Esc back",
            "q quit",
            None,
        );
        return;
    }

    let selected = ensure_selection(table_state, state.items.len());
    let columns = Layout::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(sections[1]);

    let header = Row::new(["view", "source", "purpose"])
        .style(table_header_style())
        .height(1);
    let rows = state.items.iter().enumerate().map(|(idx, item)| {
        let bg = striped_bg(idx);
        Row::new(vec![
            Cell::from(item.name.as_str()).style(Style::default().fg(TEXT).bg(bg)),
            Cell::from(item.source_view.as_str()).style(Style::default().fg(MUTED).bg(bg)),
            Cell::from(item.purpose.as_str()).style(Style::default().fg(TEXT).bg(bg)),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(22),
            Constraint::Length(28),
            Constraint::Min(18),
        ],
    )
    .header(header)
    .block(panel_block("Available Views"))
    .row_highlight_style(selected_row_style())
    .column_spacing(1);
    f.render_stateful_widget(table, columns[0], table_state);

    let selected_item = &state.items[selected];
    let details = Text::from(vec![
        key_value_line("Run", state.run_id),
        key_value_line("Experiment", state.experiment),
        key_value_line("Started", state.started_at),
        key_value_line("Status", state.status),
        Line::default(),
        key_value_line("View", selected_item.name.as_str()),
        key_value_line("Source", selected_item.source_view.as_str()),
        Line::from(vec![Span::styled(
            "Purpose",
            Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![Span::styled(
            selected_item.purpose.as_str(),
            Style::default().fg(TEXT),
        )]),
    ]);
    let detail_card = Paragraph::new(details)
        .block(panel_block("Selection"))
        .wrap(Wrap { trim: true })
        .style(Style::default().bg(PANEL_BG));
    f.render_widget(detail_card, columns[1]);

    render_browser_footer(
        f,
        sections[2],
        "Enter open live view",
        "Esc back",
        Some("↑↓ move"),
    );
}

fn render_live_view(f: &mut Frame, state: &ViewState, table_state: &mut TableState) {
    paint_app_background(f);
    let shell = chrome_block("Live View Browser", state.view_name);
    let inner = shell.inner(f.area());
    f.render_widget(shell, f.area());

    let has_progress = state.progress.is_some();
    let has_legend = !state.legend.is_empty();
    let has_split = state.split_labels.is_some();

    let mut constraints = vec![Constraint::Length(4)];
    if has_progress {
        constraints.push(Constraint::Length(3));
    }
    if has_legend {
        constraints.push(Constraint::Length(3));
    }
    if has_split {
        constraints.push(Constraint::Length(2));
    }
    constraints.push(Constraint::Min(8));
    constraints.push(Constraint::Length(2));
    let sections = Layout::vertical(constraints).split(inner);

    let mut slot = 0;
    render_live_header(f, sections[slot], state);
    slot += 1;

    if has_progress {
        render_gauge(f, sections[slot], state);
        slot += 1;
    }
    if has_legend {
        render_legend(f, sections[slot], state);
        slot += 1;
    }
    if has_split {
        render_split_labels(f, sections[slot], state);
        slot += 1;
    }

    render_table(f, sections[slot], state, table_state);
    slot += 1;
    render_live_footer(f, sections[slot], state);
}

fn paint_app_background(f: &mut Frame) {
    f.render_widget(Block::default().style(Style::default().bg(APP_BG)), f.area());
}

fn chrome_block<'a>(title: &'a str, subtitle: &'a str) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(APP_BG))
        .title(Span::styled(
            format!(" {} ", title),
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ))
        .title_bottom(Span::styled(
            format!(" {} ", subtitle),
            Style::default().fg(MUTED),
        ))
}

fn panel_block<'a>(title: &'a str) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(PANEL_BG))
        .title(Span::styled(
            format!(" {} ", title),
            Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
        ))
}

fn render_browser_header(
    f: &mut Frame,
    area: Rect,
    title: &str,
    subtitle: &str,
    refresh_secs: u64,
    count: usize,
) {
    let block = panel_block(title).style(Style::default().bg(PANEL_ALT_BG));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let text = Text::from(vec![
        Line::from(vec![
            Span::styled(
                subtitle,
                Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} item{}", count, if count == 1 { "" } else { "s" }),
                Style::default().fg(ACCENT),
            ),
        ]),
        Line::from(vec![Span::styled(
            format!("refresh {}s", refresh_secs.max(1)),
            Style::default().fg(MUTED),
        )]),
    ]);
    f.render_widget(Paragraph::new(text), inner);
}

fn render_empty_panel(f: &mut Frame, area: Rect, title: &str, body: &str) {
    let block = panel_block(title);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let text = Text::from(vec![
        Line::default(),
        Line::from(vec![Span::styled(
            body,
            Style::default().fg(MUTED),
        )]),
    ]);
    f.render_widget(
        Paragraph::new(text)
            .style(Style::default().bg(PANEL_BG))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
        inner,
    );
}

fn render_browser_footer(
    f: &mut Frame,
    area: Rect,
    primary: &str,
    secondary: &str,
    tertiary: Option<&str>,
) {
    let mut spans = vec![
        Span::styled(primary, Style::default().fg(WARNING)),
        Span::raw("  "),
        Span::styled(secondary, Style::default().fg(MUTED)),
    ];
    if let Some(extra) = tertiary {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(extra, Style::default().fg(MUTED)));
    }
    f.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(APP_BG)),
        area,
    );
}

fn render_hints_footer(f: &mut Frame, area: Rect, hints: &[KeyHint]) {
    let mut spans = Vec::new();
    for (idx, hint) in hints.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(hint.key, Style::default().fg(WARNING)));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(hint.label, Style::default().fg(MUTED)));
    }
    f.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(APP_BG)),
        area,
    );
}

fn render_live_header(f: &mut Frame, area: Rect, state: &ViewState) {
    let block = panel_block("Current View").style(Style::default().bg(PANEL_ALT_BG));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let text = Text::from(vec![
        Line::from(vec![
            Span::styled(
                state.run_id,
                Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(state.status, status_style(state.status)),
            Span::raw("  "),
            Span::styled(
                state.view_name,
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                format!(
                    "{} row{}",
                    state.table.rows.len(),
                    if state.table.rows.len() == 1 { "" } else { "s" }
                ),
                Style::default().fg(MUTED),
            ),
            Span::raw("  "),
            Span::styled(
                format!("started {}", state.started_at),
                Style::default().fg(MUTED),
            ),
            Span::raw("  "),
            Span::styled(
                format!("refresh {}s", state.interval_secs),
                Style::default().fg(MUTED),
            ),
        ]),
    ]);
    f.render_widget(Paragraph::new(text), inner);
}

fn render_gauge(f: &mut Frame, area: Rect, state: &ViewState) {
    let Some((done, total)) = state.progress else {
        return;
    };
    let ratio = if total > 0 {
        done as f64 / total as f64
    } else {
        0.0
    };
    let block = panel_block("Progress");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let gauge = Gauge::default()
        .ratio(ratio.min(1.0))
        .label(format!(" {}/{} ({:.0}%) ", done, total, ratio * 100.0))
        .gauge_style(Style::default().fg(ACCENT).bg(ACCENT_SOFT));
    f.render_widget(gauge, inner);
}

fn render_legend(f: &mut Frame, area: Rect, state: &ViewState) {
    let block = panel_block("Legend");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let mut spans = Vec::new();
    for (idx, (key, value)) in state.legend.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            key.as_str(),
            Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(": ", Style::default().fg(MUTED)));
        spans.push(Span::styled(value.as_str(), Style::default().fg(TEXT)));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), inner);
}

fn render_split_labels(f: &mut Frame, area: Rect, state: &ViewState) {
    let (left, right) = match state.split_labels {
        Some(pair) => pair,
        None => return,
    };
    let block = panel_block("Panels");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let sep_idx = state
        .table
        .columns
        .iter()
        .position(|c| c == "┃")
        .unwrap_or(state.table.columns.len() / 2);
    let widths = compute_column_widths(state.table, inner.width as usize);
    let left_chars: usize = widths[..sep_idx]
        .iter()
        .map(|c| match c {
            Constraint::Length(w) => *w as usize + 1,
            _ => 1,
        })
        .sum();

    let pad = left_chars.saturating_sub(1);
    let left_padded = format!(" {:<width$}", left, width = pad);
    let line = Line::from(vec![
        Span::styled(
            left_padded,
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::styled("┃ ", Style::default().fg(MUTED)),
        Span::styled(
            right.to_string(),
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
    ]);
    f.render_widget(Paragraph::new(line), inner);
}

fn render_table(f: &mut Frame, area: Rect, state: &ViewState, table_state: &mut TableState) {
    let block = panel_block("Data");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if state.table.columns.is_empty() {
        f.render_widget(
            Paragraph::new("No rows yet")
                .style(Style::default().fg(MUTED).bg(PANEL_BG))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    }

    let header = Row::new(state.table.columns.iter().map(|column| {
        let style = if column == "┃" {
            Style::default().fg(MUTED).bg(PANEL_BG)
        } else {
            table_header_style().bg(PANEL_BG)
        };
        Cell::from(column.as_str()).style(style)
    }))
    .height(1);

    let rows: Vec<Row> = state
        .table
        .rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let bg = striped_bg(idx);
            Row::new(row.iter().enumerate().map(|(col_idx, value)| {
                let style = cell_style(&state.table.columns, col_idx, value, bg);
                Cell::from(format_cell_value(value)).style(style)
            }))
        })
        .collect();

    let table = Table::new(rows, compute_column_widths(state.table, inner.width as usize))
        .header(header)
        .row_highlight_style(selected_row_style())
        .column_spacing(1);

    ensure_selection(table_state, state.table.rows.len());
    f.render_stateful_widget(table, inner, table_state);
}

fn render_live_footer(f: &mut Frame, area: Rect, state: &ViewState) {
    if state.hints.is_empty() {
        let line = Line::from(vec![
            Span::styled("Esc", Style::default().fg(WARNING)),
            Span::raw(" "),
            Span::styled("back", Style::default().fg(MUTED)),
            Span::raw("  "),
            Span::styled("q", Style::default().fg(WARNING)),
            Span::raw(" "),
            Span::styled("quit", Style::default().fg(MUTED)),
        ]);
        f.render_widget(Paragraph::new(line), area);
    } else {
        render_hints_footer(f, area, state.hints);
    }
}

fn ensure_selection(table_state: &mut TableState, len: usize) -> usize {
    if len == 0 {
        table_state.select(None);
        return 0;
    }
    let idx = table_state.selected().unwrap_or(0).min(len.saturating_sub(1));
    table_state.select(Some(idx));
    idx
}

fn key_value_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{}: ", label),
            Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        ),
        Span::styled(value.to_string(), Style::default().fg(TEXT)),
    ])
}

fn table_header_style() -> Style {
    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
}

fn selected_row_style() -> Style {
    Style::default()
        .bg(ACCENT_SOFT)
        .fg(TEXT)
        .add_modifier(Modifier::BOLD)
}

fn striped_bg(idx: usize) -> Color {
    if idx % 2 == 0 {
        PANEL_BG
    } else {
        PANEL_ALT_BG
    }
}

fn status_style(status: &str) -> Style {
    if status.starts_with("running") {
        Style::default()
            .fg(WARNING)
            .add_modifier(Modifier::BOLD)
    } else if status.starts_with("paused") {
        Style::default()
            .fg(ACCENT)
            .add_modifier(Modifier::BOLD)
    } else if status == "completed" {
        Style::default()
            .fg(SUCCESS)
            .add_modifier(Modifier::BOLD)
    } else if status.contains("fail") || status.contains("error") || status == "killed" {
        Style::default().fg(DANGER).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(TEXT)
    }
}

fn format_cell_value(value: &Value) -> String {
    match value {
        Value::Null => "·".to_string(),
        Value::String(text) => text.clone(),
        Value::Number(number) => {
            if let Some(f) = number.as_f64() {
                if f == f.trunc() && f.abs() < 1e15 {
                    format!("{}", f as i64)
                } else {
                    format!("{:.4}", f)
                }
            } else {
                number.to_string()
            }
        }
        Value::Bool(boolean) => boolean.to_string(),
        other => other.to_string(),
    }
}

fn cell_style(columns: &[String], col_idx: usize, value: &Value, bg: Color) -> Style {
    let column = columns.get(col_idx).map(String::as_str).unwrap_or("");
    let base = Style::default().bg(bg).fg(TEXT);

    if column == "st" {
        if let Some(symbol) = value.as_str() {
            return match symbol {
                "●" => base.fg(SUCCESS),
                "✗" => base.fg(DANGER),
                _ => base.fg(MUTED),
            };
        }
    }

    if column == "┃" {
        return base.fg(MUTED);
    }

    if column.contains("rate") || column.contains("score") || column == "primary_metric_mean" {
        if let Some(number) = value.as_f64() {
            return if number >= 0.8 {
                base.fg(SUCCESS)
            } else if number >= 0.5 {
                base.fg(WARNING)
            } else {
                base.fg(DANGER)
            };
        }
    }

    if column == "outcome" || column.ends_with("_outcome") {
        if let Some(status) = value.as_str() {
            return match status {
                "success" => base.fg(SUCCESS),
                "failure" | "error" => base.fg(DANGER),
                _ => base,
            };
        }
    }

    if column == "status" || column == "lifecycle" {
        if let Some(status) = value.as_str() {
            return status_style(status).bg(bg);
        }
    }

    base
}

fn compute_column_widths(table: &lab_analysis::QueryTable, available: usize) -> Vec<Constraint> {
    if table.columns.is_empty() {
        return vec![];
    }

    let mut max_widths: Vec<usize> = table.columns.iter().map(|c| c.len()).collect();
    for row in &table.rows {
        for (idx, value) in row.iter().enumerate() {
            if idx < max_widths.len() {
                let len = format_cell_value(value).len();
                max_widths[idx] = max_widths[idx].max(len);
            }
        }
    }

    for width in &mut max_widths {
        *width = (*width).min(40);
    }

    let total: usize = max_widths.iter().sum();
    let separators = table.columns.len().saturating_sub(1);
    let usable = available.saturating_sub(separators);

    if total <= usable {
        max_widths
            .iter()
            .map(|&width| Constraint::Length(width as u16))
            .collect()
    } else {
        let min_col = 4u16;
        max_widths
            .iter()
            .map(|&width| {
                let proportional = (width as f64 / total as f64 * usable as f64) as u16;
                Constraint::Length(proportional.max(min_col))
            })
            .collect()
    }
}
