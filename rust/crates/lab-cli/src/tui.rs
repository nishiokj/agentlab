use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Cell, Gauge, Paragraph, Row, Table, TableState},
};
use serde_json::Value;
use std::io::{self, Stdout};
use std::time::Duration;

pub enum Action {
    Quit,
    ScrollUp,
    ScrollDown,
    PageUp,
    PageDown,
    Tick,
}

pub struct ViewState<'a> {
    pub run_id: &'a str,
    pub status: &'a str,
    pub view_name: &'a str,
    pub interval_secs: u64,
    pub table: &'a lab_analysis::QueryTable,
    pub progress: Option<(usize, usize)>,
    pub legend: &'a [(String, String)],
    /// When set, renders variant labels centered over each table half.
    pub split_labels: Option<(&'a str, &'a str)>,
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

    pub fn draw(&mut self, state: &ViewState) -> anyhow::Result<()> {
        let table_state = &mut self.table_state;
        self.terminal.draw(|f| render(f, state, table_state))?;
        Ok(())
    }

    pub fn poll(&self, timeout: Duration) -> anyhow::Result<Action> {
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    return Ok(match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => Action::Quit,
                        KeyCode::Char('c')
                            if key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            Action::Quit
                        }
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
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some((i + 1).min(max.saturating_sub(1))));
    }

    pub fn page_up(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some(i.saturating_sub(20)));
    }

    pub fn page_down(&mut self, max: usize) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some((i + 20).min(max.saturating_sub(1))));
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(LeaveAlternateScreen);
    }
}

fn render(f: &mut Frame, state: &ViewState, table_state: &mut TableState) {
    let has_progress = state.progress.is_some();
    let has_legend = !state.legend.is_empty();
    let has_split = state.split_labels.is_some();

    let mut constraints = vec![Constraint::Length(1)]; // header
    if has_progress {
        constraints.push(Constraint::Length(1)); // gauge
        constraints.push(Constraint::Length(1)); // spacer after gauge
    }
    if has_legend {
        constraints.push(Constraint::Length(1)); // legend
    }
    if has_split {
        constraints.push(Constraint::Length(1)); // split panel labels
    }
    constraints.push(Constraint::Length(1)); // separator
    constraints.push(Constraint::Min(3)); // table
    constraints.push(Constraint::Length(1)); // footer

    let chunks = Layout::vertical(constraints).split(f.area());

    let mut slot = 0;
    render_header(f, chunks[slot], state);
    slot += 1;

    if has_progress {
        render_gauge(f, chunks[slot], state);
        slot += 1;
        // spacer (blank row for breathing room)
        slot += 1;
    }

    if has_legend {
        render_legend(f, chunks[slot], state);
        slot += 1;
    }

    if has_split {
        render_split_labels(f, chunks[slot], state);
        slot += 1;
    }

    // separator
    let sep = Paragraph::new(
        "─".repeat(chunks[slot].width as usize),
    )
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(sep, chunks[slot]);
    slot += 1;

    render_table(f, chunks[slot], state, table_state);
    slot += 1;

    render_footer(f, chunks[slot], state);
}

fn render_header(f: &mut Frame, area: Rect, state: &ViewState) {
    let status_style = if state.status.starts_with("running") {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else if state.status == "completed" {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else if state.status.contains("fail") || state.status.contains("error") {
        Style::default()
            .fg(Color::Red)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };

    let header = Line::from(vec![
        Span::styled(" ▸ ", Style::default().fg(Color::Cyan)),
        Span::styled(
            state.run_id,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(state.status, status_style),
        Span::raw("  "),
        Span::styled(state.view_name, Style::default().fg(Color::Cyan)),
    ]);
    f.render_widget(Paragraph::new(header), area);
}

fn render_gauge(f: &mut Frame, area: Rect, state: &ViewState) {
    if let Some((done, total)) = state.progress {
        let ratio = if total > 0 {
            done as f64 / total as f64
        } else {
            0.0
        };
        let label = format!(" {}/{} ({:.0}%)", done, total, ratio * 100.0);
        // Constrain gauge to ~40% width
        let gauge_width = ((area.width as f64 * 0.4) as u16).max(20).min(area.width);
        let gauge_area = Rect {
            width: gauge_width,
            ..area
        };
        let gauge = Gauge::default()
            .ratio(ratio.min(1.0))
            .label(label)
            .gauge_style(Style::default().fg(Color::Green).bg(Color::DarkGray));
        f.render_widget(gauge, gauge_area);
    }
}

fn render_legend(f: &mut Frame, area: Rect, state: &ViewState) {
    let mut spans = vec![Span::styled(" ", Style::default())];
    for (i, (key, val)) in state.legend.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            key.as_str(),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled("=", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            val.as_str(),
            Style::default().fg(Color::White),
        ));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_split_labels(f: &mut Frame, area: Rect, state: &ViewState) {
    let (left, right) = match state.split_labels {
        Some(pair) => pair,
        None => return,
    };

    // Compute split position from actual table column widths so the label
    // aligns with the ┃ separator column in the data rows.
    let sep_idx = state
        .table
        .columns
        .iter()
        .position(|c| c == "┃")
        .unwrap_or(state.table.columns.len() / 2);
    let widths = compute_column_widths(state.table, area.width as usize);
    // Each column takes its width + 1 char gap (ratatui default column_spacing=1)
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
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("┃ ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            right.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn render_table(f: &mut Frame, area: Rect, state: &ViewState, table_state: &mut TableState) {
    if state.table.columns.is_empty() {
        f.render_widget(
            Paragraph::new(" (no data)").style(Style::default().fg(Color::DarkGray)),
            area,
        );
        return;
    }

    let header_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(Color::DarkGray);

    let header = Row::new(state.table.columns.iter().map(|c| {
        let style = if c == "┃" { sep_style } else { header_style };
        Cell::from(c.as_str()).style(style)
    }))
    .height(1);

    let rows: Vec<Row> = state
        .table
        .rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let bg = if i % 2 == 0 {
                Color::Reset
            } else {
                Color::Rgb(25, 25, 30)
            };
            Row::new(row.iter().enumerate().map(|(col_idx, v)| {
                let text = format_cell_value(v);
                let style = cell_style(&state.table.columns, col_idx, v, bg);
                Cell::from(text).style(style)
            }))
        })
        .collect();

    let widths = compute_column_widths(state.table, area.width as usize);

    let table = Table::new(rows, widths)
        .header(header)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    if table_state.selected().is_none() && !state.table.rows.is_empty() {
        table_state.select(Some(0));
    }

    f.render_stateful_widget(table, area, table_state);
}

fn render_footer(f: &mut Frame, area: Rect, state: &ViewState) {
    let footer = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("↑↓", Style::default().fg(Color::Yellow)),
        Span::styled(" scroll  ", Style::default().fg(Color::DarkGray)),
        Span::styled("PgUp/Dn", Style::default().fg(Color::Yellow)),
        Span::styled(" page  ", Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled(
            format!("refresh {}s", state.interval_secs),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw("  "),
        Span::styled(
            format!("{} rows", state.table.rows.len()),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(footer), area);
}

fn format_cell_value(v: &Value) -> String {
    match v {
        Value::Null => "·".to_string(),
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == f.trunc() && f.abs() < 1e15 {
                    format!("{}", f as i64)
                } else {
                    format!("{:.4}", f)
                }
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn cell_style(columns: &[String], col_idx: usize, v: &Value, bg: Color) -> Style {
    let col_name = columns.get(col_idx).map(String::as_str).unwrap_or("");
    let base = Style::default().bg(bg);

    // Status indicator dots (split view)
    if col_name == "st" {
        if let Some(s) = v.as_str() {
            return match s {
                "●" => base.fg(Color::Green),
                "✗" => base.fg(Color::Red),
                _ => base.fg(Color::DarkGray),
            };
        }
    }

    // Split view separator
    if col_name == "┃" {
        return base.fg(Color::DarkGray);
    }

    if col_name.contains("rate") || col_name.contains("score") || col_name == "primary_metric_mean"
    {
        if let Some(f) = v.as_f64() {
            return if f >= 0.8 {
                base.fg(Color::Green)
            } else if f >= 0.5 {
                base.fg(Color::Yellow)
            } else {
                base.fg(Color::Red)
            };
        }
    }

    if col_name == "outcome" || col_name.ends_with("_outcome") {
        if let Some(s) = v.as_str() {
            return match s {
                "success" => base.fg(Color::Green),
                "failure" | "error" => base.fg(Color::Red),
                _ => base,
            };
        }
    }

    if col_name == "status" || col_name == "lifecycle" {
        if let Some(s) = v.as_str() {
            return match s {
                "completed" | "success" => base.fg(Color::Green),
                "in_flight" | "running" => base.fg(Color::Yellow),
                "failed" | "error" => base.fg(Color::Red),
                _ => base,
            };
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
        for (i, v) in row.iter().enumerate() {
            if i < max_widths.len() {
                let len = format_cell_value(v).len();
                max_widths[i] = max_widths[i].max(len);
            }
        }
    }

    // Cap each column at 40 chars
    for w in &mut max_widths {
        *w = (*w).min(40);
    }

    let total: usize = max_widths.iter().sum();
    let separators = table.columns.len().saturating_sub(1);
    let usable = available.saturating_sub(separators);

    if total <= usable {
        max_widths
            .iter()
            .map(|&w| Constraint::Length(w as u16))
            .collect()
    } else {
        let min_col = 4u16;
        max_widths
            .iter()
            .map(|&w| {
                let proportional = (w as f64 / total as f64 * usable as f64) as u16;
                Constraint::Length(proportional.max(min_col))
            })
            .collect()
    }
}
