use std::{
    collections::{HashMap, VecDeque},
    fmt::Write,
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Color {
    #[default]
    Default,
    Index(u8),
    Rgb(u8, u8, u8),
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Style {
    pub fg: Color,
    pub bg: Color,
    pub bits: u8,
}
impl Style {
    pub fn ansi(self, out: &mut String) {
        out.push_str("\x1b[0");
        for (bit, code) in [
            (1, 1),
            (2, 2),
            (4, 3),
            (8, 4),
            (16, 5),
            (32, 7),
            (64, 8),
            (128, 9),
        ] {
            if self.bits & bit != 0 {
                let _ = write!(out, ";{code}");
            }
        }
        for (color, code) in [(self.fg, 38), (self.bg, 48)] {
            match color {
                Color::Default => {}
                Color::Index(n) => {
                    let _ = write!(out, ";{code};5;{n}");
                }
                Color::Rgb(r, g, b) => {
                    let _ = write!(out, ";{code};2;{r};{g};{b}");
                }
            }
        }
        out.push('m');
    }
}

/// The longest grapheme cluster a cell keeps, in UTF-8 bytes.
const CLUSTER_BYTES: usize = 16;
const CLUSTERS: usize = 4096;
/// Grapheme clusters of more than one code point. A cell refers to its
/// cluster by index, so every cell has the same small size. Entries are never
/// removed; the table is bounded and starts empty after a terminal reset.
#[derive(Default)]
pub struct Clusters {
    list: Vec<Box<str>>,
    index: HashMap<Box<str>, u32>,
}
impl Clusters {
    fn intern(&mut self, text: &str) -> Option<u32> {
        if let Some(id) = self.index.get(text) {
            return Some(*id);
        }
        if self.list.len() == CLUSTERS {
            return None;
        }
        let id = self.list.len() as u32;
        self.list.push(text.into());
        self.index.insert(text.into(), id);
        Some(id)
    }
}

const CHAR: u32 = (1 << 21) - 1;
const CLUSTER: u32 = 1 << 23;
/// One grid position: a code point or cluster index, a display width of 0
/// (the second half of a wide character), 1 or 2, and the style.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    head: u32,
    fg: Color,
    bg: Color,
}
pub const CELL_BYTES: usize = std::mem::size_of::<Cell>();
impl Cell {
    fn new(text: u32, width: usize, style: Style) -> Self {
        Self {
            head: text | (width as u32) << 21 | u32::from(style.bits) << 24,
            fg: style.fg,
            bg: style.bg,
        }
    }
    pub fn blank(style: Style) -> Self {
        Self::new(0, 1, style)
    }
    fn continuation(style: Style) -> Self {
        Self::new(0, 0, style)
    }
    pub fn width(self) -> usize {
        (self.head >> 21 & 3) as usize
    }
    pub fn style(self) -> Style {
        Style {
            fg: self.fg,
            bg: self.bg,
            bits: (self.head >> 24) as u8,
        }
    }
    fn is_empty(self) -> bool {
        self.head & (CHAR | CLUSTER) == 0
    }
    /// Appends the cell's text; an empty cell appends nothing.
    pub fn text(self, clusters: &Clusters, out: &mut String) {
        let value = self.head & CHAR;
        if self.head & CLUSTER != 0 {
            out.push_str(&clusters.list[value as usize])
        } else if let Some(ch) = char::from_u32(value).filter(|_| value != 0) {
            out.push(ch)
        }
    }
}
pub type Line = Vec<Cell>;

/// Code points that neither extend a preceding cluster nor start one that
/// later code points join, unless the preceding cell is not plain: Latin
/// letters, box drawing, block elements, Braille and CJK ideographs. They are
/// the bulk of non-ASCII terminal output and skip grapheme segmentation.
fn plain(ch: char) -> bool {
    matches!(ch, '\u{c0}'..='\u{24f}' | '\u{2500}'..='\u{259f}' | '\u{2800}'..='\u{28ff}' | '\u{4e00}'..='\u{9fff}')
}

pub struct Screen {
    pub rows: usize,
    pub cols: usize,
    /// Scrollback first, then the `rows` visible lines.
    lines: VecDeque<Line>,
    /// Cell storage held by `lines`.
    bytes: usize,
    scrollback: usize,
    budget: usize,
    pub row: usize,
    pub col: usize,
    pub saved: (usize, usize, Style),
    pub top: usize,
    pub bottom: usize,
    /// The cursor's cell is the last column and was just written. The next
    /// character wraps first with autowrap and overwrites it without, and a
    /// combining character joins it either way.
    pub wrap: bool,
    pub limited: bool,
    /// Scrollback was dropped to stay within the limits.
    pub truncated: bool,
}
impl Screen {
    /// `scrollback` lines within `budget` bytes of cell storage are kept
    /// when they leave the top of the screen; zero keeps none.
    pub fn new(rows: usize, cols: usize, scrollback: usize, budget: usize) -> Self {
        let mut screen = Self {
            rows,
            cols,
            lines: VecDeque::new(),
            bytes: 0,
            scrollback,
            budget,
            row: 0,
            col: 0,
            saved: (0, 0, Style::default()),
            top: 0,
            bottom: rows - 1,
            wrap: false,
            limited: false,
            truncated: false,
        };
        for _ in 0..rows {
            screen.push(Vec::new(), Style::default());
        }
        screen
    }
    #[cfg(test)]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    /// Every wide character has its second half and no half stands alone.
    #[cfg(test)]
    pub fn whole(&self) -> bool {
        self.visible().all(|line| {
            line.len() == self.cols
                && (0..line.len()).all(|col| match line[col].width() {
                    0 => col > 0 && line[col - 1].width() == 2,
                    2 => col + 1 < line.len() && line[col + 1].width() == 0,
                    _ => true,
                })
        })
    }
    pub fn history(&self) -> usize {
        self.lines.len() - self.rows
    }
    pub fn scrollback(&self) -> impl Iterator<Item = &Line> {
        self.lines.iter().take(self.history())
    }
    pub fn visible(&self) -> impl Iterator<Item = &Line> {
        self.lines.iter().skip(self.history())
    }
    pub fn line(&self, row: usize) -> &Line {
        &self.lines[self.history() + row]
    }
    fn line_mut(&mut self, row: usize) -> &mut Line {
        let at = self.history() + row;
        &mut self.lines[at]
    }
    fn push(&mut self, mut line: Line, style: Style) {
        line.clear();
        if line.capacity() > self.cols * 2 {
            line.shrink_to(self.cols);
        }
        line.resize(self.cols, Cell::blank(style));
        self.bytes += line.capacity() * CELL_BYTES;
        self.lines.push_back(line);
    }
    fn evict(&mut self) -> Line {
        let line = self.lines.pop_front().unwrap_or_default();
        self.bytes -= line.capacity() * CELL_BYTES;
        self.truncated = true;
        line
    }
    fn bound(&mut self) {
        while self.history() > 0 && (self.history() > self.scrollback || self.bytes > self.budget) {
            self.evict();
        }
    }
    pub fn erase_history(&mut self) {
        for _ in 0..self.history() {
            if let Some(line) = self.lines.pop_front() {
                self.bytes -= line.capacity() * CELL_BYTES;
            }
        }
    }
    pub fn resize(&mut self, rows: usize, cols: usize) {
        let history = self.history();
        self.cols = cols;
        while self.lines.len() > history + rows {
            if let Some(line) = self.lines.pop_back() {
                self.bytes -= line.capacity() * CELL_BYTES;
            }
        }
        for line in self.lines.iter_mut().skip(history) {
            self.bytes -= line.capacity() * CELL_BYTES;
            line.resize(cols, Cell::blank(Style::default()));
            line.shrink_to(cols);
            self.bytes += line.capacity() * CELL_BYTES;
            repair(line);
        }
        while self.lines.len() < history + rows {
            self.push(Vec::new(), Style::default());
        }
        self.rows = rows;
        self.row = self.row.min(rows - 1);
        self.col = self.col.min(cols - 1);
        self.top = 0;
        self.bottom = rows - 1;
        self.wrap = false;
        self.bound();
    }
    /// `keep` moves lines that leave a full-height region into scrollback.
    pub fn scroll_up(&mut self, n: usize, style: Style, keep: bool) {
        let n = n.min(self.bottom - self.top + 1);
        if keep && self.scrollback > 0 && self.top == 0 && self.bottom + 1 == self.rows {
            // The top line becomes scrollback by staying where it is. Once
            // the scrollback is full its oldest line is reused as the new
            // bottom line, so steady scrolling allocates nothing.
            for _ in 0..n {
                let line = if self.history() >= self.scrollback {
                    self.evict()
                } else {
                    Vec::new()
                };
                self.push(line, style);
            }
            self.bound();
            return;
        }
        let base = self.history();
        for _ in 0..n {
            for row in base + self.top..base + self.bottom {
                self.lines.swap(row, row + 1);
            }
            self.lines[base + self.bottom].fill(Cell::blank(style));
        }
    }
    pub fn scroll_down(&mut self, n: usize, style: Style) {
        let base = self.history();
        for _ in 0..n.min(self.bottom - self.top + 1) {
            for row in (base + self.top..base + self.bottom).rev() {
                self.lines.swap(row, row + 1);
            }
            self.lines[base + self.top].fill(Cell::blank(style));
        }
    }
    pub fn newline(&mut self, style: Style) {
        self.wrap = false;
        if self.row == self.bottom {
            self.scroll_up(1, style, true)
        } else {
            self.row = (self.row + 1).min(self.rows - 1);
        }
    }
    /// Whether the cell before the cursor holds a cluster, the only kind of
    /// cell a following ASCII character can join.
    pub fn after_cluster(&self) -> bool {
        self.col > 0 && self.line(self.row)[self.col - 1].head & CLUSTER != 0
    }
    /// Writes printable ASCII that does not follow a cluster: each byte takes
    /// one cell and joins nothing.
    pub fn write_ascii(&mut self, mut run: &[u8], style: Style, autowrap: bool) {
        while !run.is_empty() {
            if self.wrap {
                if autowrap {
                    self.col = 0;
                    self.newline(style);
                }
                self.wrap = false;
            }
            let (col, cols) = (self.col, self.cols);
            let n = run.len().min(cols - col);
            let line = self.line_mut(self.row);
            if line[col].width() == 0 && col > 0 {
                line[col - 1] = Cell::blank(style);
            }
            if line[col + n - 1].width() == 2 && col + n < cols {
                line[col + n] = Cell::blank(style);
            }
            for (cell, byte) in line[col..col + n].iter_mut().zip(run) {
                *cell = Cell::new(u32::from(*byte), 1, style);
            }
            run = &run[n..];
            self.col = (col + n).min(cols - 1);
            self.wrap = col + n >= cols;
            if !autowrap && let Some(last) = run.last() {
                // Without autowrap the rest overwrites the last column.
                self.line_mut(self.row)[cols - 1] = Cell::new(u32::from(*last), 1, style);
                return;
            }
        }
    }
    pub fn put(
        &mut self,
        ch: char,
        style: Style,
        autowrap: bool,
        insert: bool,
        clusters: &mut Clusters,
    ) {
        if ch.is_control() {
            return;
        }
        if self.col > 0 || self.wrap {
            let mut prev = if self.wrap { self.col } else { self.col - 1 };
            if self.line(self.row)[prev].width() == 0 && prev > 0 {
                prev -= 1;
            }
            let cell = self.line(self.row)[prev];
            let ascii = cell.head & (CHAR | CLUSTER) < 128;
            // ASCII joins only a cluster that ends in a zero-width joiner.
            let candidate = if ch.is_ascii() {
                self.col > 0 && {
                    let mut text = String::new();
                    self.line(self.row)[self.col - 1].text(clusters, &mut text);
                    text.contains('\u{200d}')
                }
            } else {
                !(plain(ch) && ascii)
            };
            if candidate {
                let mut joined = String::with_capacity(CLUSTER_BYTES);
                cell.text(clusters, &mut joined);
                if joined.len() + ch.len_utf8() > CLUSTER_BYTES {
                    self.limited = true;
                } else if !joined.is_empty() {
                    joined.push(ch);
                    if joined.graphemes(true).count() == 1 {
                        if let Some(id) = clusters.intern(&joined) {
                            let wide = UnicodeWidthStr::width(joined.as_str()).clamp(1, 2);
                            let width = wide.min(self.cols - prev);
                            self.limited |= width < wide;
                            let line = self.line_mut(self.row);
                            line[prev] = Cell::new(id | CLUSTER, width, cell.style());
                            // Joining can change the width, for example with
                            // a variation selector.
                            if width == 2 {
                                if line[prev + 1].width() == 2 && prev + 2 < line.len() {
                                    line[prev + 2] = Cell::blank(style);
                                }
                                line[prev + 1] = Cell::continuation(style);
                            } else if cell.width() == 2 {
                                line[prev + 1] = Cell::blank(style);
                            }
                            self.col = (prev + width).min(self.cols - 1);
                            self.wrap = prev + width >= self.cols;
                            return;
                        }
                        self.limited = true;
                    }
                }
            }
        }
        let mut encoded = [0; 4];
        let mut width = UnicodeWidthStr::width(&*ch.encode_utf8(&mut encoded)).min(2);
        if width == 0 {
            return;
        }
        if self.wrap || width == 2 && self.col + 1 == self.cols && autowrap {
            if autowrap {
                self.col = 0;
                self.newline(style);
            }
            self.wrap = false;
        }
        if width > self.cols - self.col {
            self.limited = true;
        }
        width = width.min(self.cols - self.col);
        let (col, cols) = (self.col, self.cols);
        let line = self.line_mut(self.row);
        if insert {
            line.copy_within(col..cols - width, col + width);
            line[col..col + width].fill(Cell::blank(style));
            repair(line);
        }
        if line[col].width() == 0 && col > 0 {
            line[col - 1] = Cell::blank(style);
        }
        if line[col].width() == 2 && col + 1 < cols {
            line[col + 1] = Cell::blank(style);
        }
        if width == 2 && col + 2 < cols && line[col + 1].width() == 2 {
            line[col + 2] = Cell::blank(style);
        }
        line[col] = Cell::new(ch as u32, width, style);
        if width == 2 {
            line[col + 1] = Cell::continuation(style);
        }
        self.col = (col + width).min(cols - 1);
        self.wrap = col + width >= cols;
    }
    pub fn erase_line(&mut self, mode: u16, style: Style) {
        let (from, to) = match mode {
            1 => (0, self.col + 1),
            2 => (0, self.cols),
            _ => (self.col, self.cols),
        };
        let line = self.line_mut(self.row);
        line[from..to].fill(Cell::blank(style));
        repair(line);
    }
    pub fn erase_display(&mut self, mode: u16, style: Style) {
        let rows = match mode {
            0 => {
                self.erase_line(0, style);
                self.row + 1..self.rows
            }
            1 => {
                self.erase_line(1, style);
                0..self.row
            }
            2 => 0..self.rows,
            _ => return,
        };
        for row in rows {
            self.line_mut(row).fill(Cell::blank(style));
        }
    }
    pub fn edit_chars(&mut self, kind: u8, n: usize, style: Style) {
        let (col, cols) = (self.col, self.cols);
        let n = n.min(cols - col);
        let line = self.line_mut(self.row);
        match kind {
            b'@' => {
                line.copy_within(col..cols - n, col + n);
                line[col..col + n].fill(Cell::blank(style));
            }
            b'P' => {
                line.copy_within(col + n.., col);
                line[cols - n..].fill(Cell::blank(style));
            }
            b'X' => line[col..col + n].fill(Cell::blank(style)),
            _ => {}
        }
        repair(line);
    }
    pub fn paint(&self, clusters: &Clusters, out: &mut String) {
        out.push_str("\x1b[H\x1b[2J");
        for (row, line) in self.visible().enumerate() {
            let _ = write!(out, "\x1b[{};1H", row + 1);
            paint_line(line, clusters, out);
        }
    }
    pub fn cursor(&self, out: &mut String) {
        let _ = write!(out, "\x1b[{};{}H", self.row + 1, self.col + 1);
    }
}
/// Blanks the halves of wide characters that an edit separated.
fn repair(line: &mut [Cell]) {
    for col in 0..line.len() {
        if line[col].width() == 0 && (col == 0 || line[col - 1].width() != 2)
            || line[col].width() == 2 && (col + 1 == line.len() || line[col + 1].width() != 0)
        {
            line[col] = Cell::blank(line[col].style());
        }
    }
}
pub fn paint_cell(cell: Cell, clusters: &Clusters, out: &mut String) {
    if cell.is_empty() {
        out.push(' ')
    } else {
        cell.text(clusters, out)
    }
}
pub fn paint_line(line: &Line, clusters: &Clusters, out: &mut String) {
    let mut style = None;
    for cell in line {
        if cell.width() == 0 {
            continue;
        }
        if style != Some(cell.style()) {
            cell.style().ansi(out);
            style = Some(cell.style());
        }
        paint_cell(*cell, clusters, out);
    }
}
