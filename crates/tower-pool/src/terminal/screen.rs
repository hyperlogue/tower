use std::fmt::Write;
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
    pub bits: u16,
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
#[derive(Clone, Copy, Debug, Default)]
pub struct CellText {
    bytes: [u8; 16],
    len: u8,
}
impl CellText {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap()
    }
    fn push(&mut self, ch: char) {
        let at = self.len as usize;
        let len = ch.encode_utf8(&mut self.bytes[at..]).len();
        self.len += len as u8;
    }
}
impl std::ops::Deref for CellText {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl From<&str> for CellText {
    fn from(text: &str) -> Self {
        let mut value = Self::default();
        value.bytes[..text.len()].copy_from_slice(text.as_bytes());
        value.len = text.len() as u8;
        value
    }
}
#[derive(Clone, Debug)]
pub struct Cell {
    pub text: CellText,
    pub style: Style,
    pub width: u8,
}
impl Cell {
    fn blank(style: Style) -> Self {
        Self {
            text: CellText::default(),
            style,
            width: 1,
        }
    }
}
pub type Line = Vec<Cell>;
#[derive(Clone)]
pub struct Screen {
    pub rows: usize,
    pub cols: usize,
    pub lines: Vec<Line>,
    pub row: usize,
    pub col: usize,
    pub saved: (usize, usize, Style),
    pub top: usize,
    pub bottom: usize,
    pub wrap: bool,
    pub limited: bool,
}
impl Screen {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            lines: vec![vec![Cell::blank(Style::default()); cols]; rows],
            row: 0,
            col: 0,
            saved: (0, 0, Style::default()),
            top: 0,
            bottom: rows - 1,
            wrap: false,
            limited: false,
        }
    }
    pub fn resize(&mut self, rows: usize, cols: usize) {
        for line in &mut self.lines {
            line.resize(cols, Cell::blank(Style::default()));
            repair(line);
        }
        self.lines
            .resize(rows, vec![Cell::blank(Style::default()); cols]);
        self.rows = rows;
        self.cols = cols;
        self.row = self.row.min(rows - 1);
        self.col = self.col.min(cols - 1);
        self.top = 0;
        self.bottom = rows - 1;
        self.wrap = false;
    }
    pub fn scroll_up(&mut self, n: usize, style: Style) -> Vec<Line> {
        let mut removed = Vec::new();
        for _ in 0..n.min(self.bottom - self.top + 1) {
            let line = self.lines.remove(self.top);
            self.lines
                .insert(self.bottom, vec![Cell::blank(style); self.cols]);
            removed.push(line);
        }
        removed
    }
    pub fn scroll_down(&mut self, n: usize, style: Style) {
        for _ in 0..n.min(self.bottom - self.top + 1) {
            self.lines.remove(self.bottom);
            self.lines
                .insert(self.top, vec![Cell::blank(style); self.cols]);
        }
    }
    pub fn newline(&mut self, style: Style) -> Vec<Line> {
        self.wrap = false;
        if self.row == self.bottom {
            self.scroll_up(1, style)
        } else {
            self.row = (self.row + 1).min(self.rows - 1);
            Vec::new()
        }
    }
    pub fn put(&mut self, ch: char, style: Style, autowrap: bool, insert: bool) -> Vec<Line> {
        if ch.is_control() {
            return Vec::new();
        }
        if (!ch.is_ascii()
            || self.col > 0 && self.lines[self.row][self.col - 1].text.contains('\u{200d}'))
            && (self.col > 0 || self.wrap)
        {
            let mut prev = if self.wrap { self.col } else { self.col - 1 };
            if self.lines[self.row][prev].width == 0 && prev > 0 {
                prev -= 1;
            }
            let cell = &self.lines[self.row][prev];
            if cell.text.len() + ch.len_utf8() > 16 {
                self.limited = true;
            }
            if !cell.text.is_empty() && cell.text.len() + ch.len_utf8() <= 16 {
                let mut joined = cell.text;
                joined.push(ch);
                if joined.graphemes(true).count() == 1 {
                    let width = UnicodeWidthStr::width(joined.as_str())
                        .clamp(1, 2)
                        .min(self.cols - prev);
                    self.lines[self.row][prev].text = joined;
                    self.lines[self.row][prev].width = width as u8;
                    if width == 2 {
                        self.lines[self.row][prev + 1] = Cell {
                            text: CellText::default(),
                            style,
                            width: 0,
                        };
                    }
                    self.col = (prev + width).min(self.cols - 1);
                    self.wrap = autowrap && prev + width >= self.cols;
                    return Vec::new();
                }
            }
        }
        let mut encoded = [0; 4];
        let text = ch.encode_utf8(&mut encoded);
        let mut width = UnicodeWidthStr::width(text).min(2);
        if width == 0 {
            return Vec::new();
        }
        let mut scrolled = Vec::new();
        if self.wrap || width == 2 && self.col + 1 == self.cols && autowrap {
            if autowrap {
                self.col = 0;
                scrolled = self.newline(style);
            }
            self.wrap = false;
        }
        if width > self.cols - self.col {
            self.limited = true;
        }
        width = width.min(self.cols - self.col);
        let line = &mut self.lines[self.row];
        if insert {
            for _ in 0..width {
                line.insert(self.col, Cell::blank(style));
                line.pop();
            }
        }
        if line[self.col].width == 0 && self.col > 0 {
            line[self.col - 1] = Cell::blank(style);
        }
        if line[self.col].width == 2 && self.col + 1 < self.cols {
            line[self.col + 1] = Cell::blank(style);
        }
        if width == 2 && self.col + 2 < self.cols && line[self.col + 1].width == 2 {
            line[self.col + 2] = Cell::blank(style);
        }
        line[self.col] = Cell {
            text: (&*text).into(),
            style,
            width: width as u8,
        };
        if width == 2 {
            line[self.col + 1] = Cell {
                text: CellText::default(),
                style,
                width: 0,
            };
        }
        let next = self.col + width;
        self.col = next.min(self.cols - 1);
        self.wrap = autowrap && next >= self.cols;
        scrolled
    }
    pub fn erase_line(&mut self, mode: u16, style: Style) {
        let (from, to) = match mode {
            1 => (0, self.col + 1),
            2 => (0, self.cols),
            _ => (self.col, self.cols),
        };
        self.lines[self.row][from..to].fill(Cell::blank(style));
        repair(&mut self.lines[self.row]);
    }
    pub fn erase_display(&mut self, mode: u16, style: Style) {
        match mode {
            0 => {
                self.erase_line(0, style);
                for row in self.row + 1..self.rows {
                    self.lines[row].fill(Cell::blank(style));
                }
            }
            1 => {
                self.erase_line(1, style);
                for row in 0..self.row {
                    self.lines[row].fill(Cell::blank(style));
                }
            }
            2 => {
                for line in &mut self.lines {
                    line.fill(Cell::blank(style));
                }
            }
            _ => {}
        }
    }
    pub fn edit_chars(&mut self, kind: u8, n: usize, style: Style) {
        let n = n.min(self.cols - self.col);
        let line = &mut self.lines[self.row];
        match kind {
            b'@' => {
                for _ in 0..n {
                    line.insert(self.col, Cell::blank(style));
                    line.pop();
                }
            }
            b'P' => {
                line.drain(self.col..self.col + n);
                line.resize(self.cols, Cell::blank(style));
            }
            b'X' => line[self.col..self.col + n].fill(Cell::blank(style)),
            _ => {}
        }
        repair(line);
    }
    pub fn paint(&self, out: &mut String) {
        out.push_str("\x1b[H\x1b[2J");
        for (row, line) in self.lines.iter().enumerate() {
            let _ = write!(out, "\x1b[{};1H", row + 1);
            paint_line(line, out);
        }
    }
    pub fn cursor(&self, out: &mut String) {
        let _ = write!(out, "\x1b[{};{}H", self.row + 1, self.col + 1);
    }
}
fn repair(line: &mut [Cell]) {
    for col in 0..line.len() {
        if line[col].width == 0 && (col == 0 || line[col - 1].width != 2)
            || line[col].width == 2 && (col + 1 == line.len() || line[col + 1].width != 0)
        {
            line[col] = Cell::blank(line[col].style);
        }
    }
}
pub fn paint_line(line: &Line, out: &mut String) {
    let mut style = None;
    for cell in line {
        if cell.width == 0 {
            continue;
        }
        if style != Some(cell.style) {
            cell.style.ansi(out);
            style = Some(cell.style);
        }
        if cell.text.is_empty() {
            out.push(' ')
        } else {
            out.push_str(&cell.text)
        }
    }
}
pub fn line_bytes(line: &Line) -> usize {
    line.capacity() * std::mem::size_of::<Cell>()
}
