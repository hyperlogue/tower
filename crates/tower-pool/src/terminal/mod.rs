mod lexer;
mod program;
mod screen;
use lexer::{Lexer, Token};
use screen::{Clusters, Color, Screen, Style};
use std::{collections::VecDeque, fmt::Write};
use tower_pool_proto::{Program, Scrollback, Size};

/// The private modes the terminal tracks, one bit each in this order.
const MODES: [u16; 15] = [
    1, 6, 7, 25, 1000, 1002, 1003, 1004, 1005, 1006, 1007, 1015, 1016, 2004, 2026,
];
const ORIGIN: u16 = 1 << 1;
const AUTOWRAP: u16 = 1 << 2;
const CURSOR: u16 = 1 << 3;
fn mode_bit(n: u16) -> Option<u16> {
    MODES.iter().position(|m| *m == n).map(|i| 1 << i)
}
/// Semicolon-separated decimal parameters; an empty one is zero.
fn params(raw: &[u8]) -> Option<([u16; 32], usize)> {
    let mut values = [0u16; 32];
    let mut count = 0;
    for &b in raw {
        match b {
            b'0'..=b'9' => {
                values[count] = values[count]
                    .checked_mul(10)?
                    .checked_add(u16::from(b - b'0'))?
            }
            b';' if count + 1 < values.len() => count += 1,
            _ => return None,
        }
    }
    Some((values, count + 1))
}
#[derive(Default)]
struct Keyboard {
    flags: u16,
    saved: VecDeque<u16>,
}
impl Keyboard {
    fn apply(&mut self, kind: u8, p: &[u16]) {
        let n = p.first().copied().unwrap_or(0);
        let flags = n & 31;
        match kind {
            b'>' => {
                if self.saved.len() == 32 {
                    self.saved.pop_front();
                }
                self.saved.push_back(self.flags);
                self.flags = flags;
            }
            b'<' => {
                for _ in 0..n.clamp(1, 33) {
                    self.flags = self.saved.pop_back().unwrap_or(0)
                }
            }
            b'=' => match p.get(1).copied().unwrap_or(1).max(1) {
                1 => self.flags = flags,
                2 => self.flags |= flags,
                3 => self.flags &= !flags,
                _ => {}
            },
            _ => {}
        }
    }
    fn restore(&self, out: &mut String) {
        out.push_str("\x1b[<65535u");
        if let Some(first) = self.saved.front() {
            let _ = write!(out, "\x1b[={first}u");
            for flags in self
                .saved
                .iter()
                .skip(1)
                .chain(std::iter::once(&self.flags))
            {
                let _ = write!(out, "\x1b[>{flags}u");
            }
        } else {
            let _ = write!(out, "\x1b[={}u", self.flags);
        }
    }
}
pub struct Output {
    pub bytes: Vec<u8>,
    pub replies: Vec<u8>,
}
pub struct Terminal {
    lexer: Lexer,
    screens: [Screen; 2],
    active: usize,
    keys: [Keyboard; 2],
    modes: u16,
    style: Style,
    keypad: bool,
    insert: bool,
    modify: u16,
    cursor_style: u16,
    clusters: Clusters,
    scrollback: Scrollback,
    programs: program::Programs,
    pub limited: bool,
}
impl Terminal {
    pub fn new(size: Size, scrollback: Scrollback) -> Self {
        let (rows, cols) = (size.rows as usize, size.cols as usize);
        Self {
            lexer: Lexer::default(),
            screens: [
                Screen::new(
                    rows,
                    cols,
                    scrollback.lines as usize,
                    usize::try_from(scrollback.bytes).unwrap_or(usize::MAX),
                ),
                Screen::new(rows, cols, 0, 0),
            ],
            active: 0,
            keys: Default::default(),
            modes: AUTOWRAP | CURSOR,
            style: Style::default(),
            keypad: false,
            insert: false,
            modify: 0,
            cursor_style: 0,
            clusters: Clusters::default(),
            scrollback,
            programs: program::Programs::default(),
            limited: false,
        }
    }
    pub fn size(&self) -> Size {
        Size {
            rows: self.screens[0].rows as u16,
            cols: self.screens[0].cols as u16,
        }
    }
    pub fn resize(&mut self, size: Size) {
        for screen in &mut self.screens {
            screen.resize(size.rows as usize, size.cols as usize)
        }
    }
    /// What the program last reported about itself (OSC 7501).
    pub fn program(&self) -> Option<&Program> {
        self.programs.summary()
    }
    /// The program exited: its unfinished work is over.
    pub fn finish(&mut self) {
        self.programs.finish();
    }
    /// Scrollback was dropped to stay within the limits.
    pub fn truncated(&self) -> bool {
        self.screens[0].truncated
    }
    pub(crate) fn mode(&self, n: u16) -> bool {
        mode_bit(n).is_some_and(|bit| self.modes & bit != 0)
    }
    fn set_mode(&mut self, n: u16, on: bool) {
        if let Some(bit) = mode_bit(n) {
            self.modes = if on {
                self.modes | bit
            } else {
                self.modes & !bit
            }
        }
    }
    /// Updates the screen from program output. `bytes` is what attached
    /// clients receive: the same output without the queries answered here in
    /// `replies`, clipboard reads, and bytes no terminal would print.
    pub fn process(&mut self, bytes: &[u8]) -> Output {
        let mut output = Output {
            bytes: Vec::with_capacity(bytes.len()),
            replies: Vec::new(),
        };
        // A full reset replaces the terminal but not the position in its input.
        let mut lexer = std::mem::take(&mut self.lexer);
        lexer.push(bytes, &mut |token| match token {
            Token::Text(text) => {
                output.bytes.extend_from_slice(text);
                self.print(text);
            }
            Token::Control(seq) => match self.control(seq) {
                Some(reply) => output.replies.extend(reply),
                None => output.bytes.extend_from_slice(seq),
            },
            Token::String(seq) => match self.string(seq) {
                Some(reply) => output.replies.extend(reply),
                None => output.bytes.extend_from_slice(seq),
            },
            Token::Opaque(seq) => {
                self.limited = true;
                output.bytes.extend_from_slice(seq);
            }
        });
        self.lexer = lexer;
        output
    }
    fn print(&mut self, text: &[u8]) {
        let wrap = self.modes & AUTOWRAP != 0;
        let screen = &mut self.screens[self.active];
        // Text is whole characters, except that an ESC may have consumed
        // the first byte of one; what is left of it is not printable.
        for chunk in text.utf8_chunks() {
            let mut text = chunk.valid();
            while let Some(ch) = text.chars().next() {
                let ascii = text
                    .bytes()
                    .position(|b| !b.is_ascii())
                    .unwrap_or(text.len());
                if ascii > 0 && !self.insert && !screen.after_cluster() {
                    screen.write_ascii(&text.as_bytes()[..ascii], self.style, wrap);
                    text = &text[ascii..];
                } else {
                    screen.put(ch, self.style, wrap, self.insert, &mut self.clusters);
                    text = &text[ch.len_utf8()..];
                }
            }
            if !chunk.invalid().is_empty() {
                screen.put(
                    '\u{fffd}',
                    self.style,
                    wrap,
                    self.insert,
                    &mut self.clusters,
                );
            }
        }
        self.limited |= screen.limited;
    }
    fn control(&mut self, b: &[u8]) -> Option<Vec<u8>> {
        if b.len() == 1 {
            let s = &mut self.screens[self.active];
            match b[0] {
                b'\r' => {
                    s.col = 0;
                    s.wrap = false;
                }
                b'\n' | 11 | 12 => s.newline(self.style),
                8 => {
                    s.col = s.col.saturating_sub(1);
                    s.wrap = false;
                }
                9 => {
                    s.col = ((s.col / 8 + 1) * 8).min(s.cols - 1);
                    s.wrap = false;
                }
                _ => {}
            }
            return None;
        }
        if b.starts_with(b"\x1b[") {
            return self.csi(b);
        }
        if b.len() == 2 {
            let s = &mut self.screens[self.active];
            match b[1] {
                b'7' => s.saved = (s.row, s.col, self.style),
                b'8' => {
                    s.row = s.saved.0.min(s.rows - 1);
                    s.col = s.saved.1.min(s.cols - 1);
                    self.style = s.saved.2;
                    s.wrap = false;
                }
                b'D' => s.newline(self.style),
                b'E' => {
                    s.col = 0;
                    s.newline(self.style);
                }
                b'M' => {
                    if s.row == s.top {
                        s.scroll_down(1, self.style)
                    } else {
                        s.row = s.row.saturating_sub(1)
                    }
                    s.wrap = false;
                }
                b'=' => self.keypad = true,
                b'>' => self.keypad = false,
                b'c' => *self = Self::new(self.size(), self.scrollback),
                _ => {}
            }
        } else {
            self.limited = true;
        }
        None
    }
    fn csi(&mut self, b: &[u8]) -> Option<Vec<u8>> {
        let action = *b.last()?;
        let body = &b[2..b.len() - 1];
        if action == b'q'
            && let Some(value) = body.strip_suffix(b" ")
        {
            if let Ok(text) = std::str::from_utf8(value)
                && let Ok(style) = text.parse::<u16>()
                && style <= 6
            {
                self.cursor_style = style;
            } else {
                self.limited = true;
            }
            return None;
        }
        let prefix = body.first().copied().filter(|b| b"?<=>".contains(b));
        let raw = if prefix.is_some() { &body[1..] } else { body };
        if raw.ends_with(b"$") && action == b'p' {
            let n = std::str::from_utf8(&raw[..raw.len() - 1])
                .ok()?
                .parse::<u16>()
                .ok()?;
            let state = if self.mode(n) { 1 } else { 2 };
            return Some(format!("\x1b[?{n};{state}$y").into_bytes());
        }
        let Some((params, count)) = params(raw) else {
            self.limited = true;
            return None;
        };
        let p = &params[..count];
        let n = p[0].max(1) as usize;
        if action == b'u' && prefix == Some(b'?') {
            return Some(format!("\x1b[?{}u", self.keys[self.active].flags).into_bytes());
        }
        if action == b'c' {
            return Some(if prefix == Some(b'>') {
                b"\x1b[>0;1;0c".to_vec()
            } else {
                b"\x1b[?62;22c".to_vec()
            });
        }
        if action == b'q' && prefix == Some(b'>') {
            return Some(b"\x1bP>|tower\x1b\\".to_vec());
        }
        if action == b'n' {
            let s = &self.screens[self.active];
            return Some(match p[0] {
                5 => b"\x1b[0n".to_vec(),
                6 => format!(
                    "\x1b[{}{};{}R",
                    if prefix == Some(b'?') { "?" } else { "" },
                    s.row + 1,
                    s.col + 1
                )
                .into_bytes(),
                _ => Vec::new(),
            });
        }
        if action == b't' {
            let s = &self.screens[self.active];
            return Some(match p[0] {
                18 => format!("\x1b[8;{};{}t", s.rows, s.cols).into_bytes(),
                14 => b"\x1b[4;0;0t".to_vec(),
                _ => Vec::new(),
            });
        }
        if action == b'u' && prefix.is_some_and(|v| b"<=>".contains(&v)) {
            self.keys[self.active].apply(prefix.unwrap(), p);
            return None;
        }
        if action == b'm' {
            if prefix == Some(b'>') && p[0] == 4 {
                self.modify = p.get(1).copied().unwrap_or(0).min(2)
            } else if prefix.is_none() {
                self.sgr(p)
            }
            return None;
        }
        if matches!(action, b'h' | b'l') {
            for &mode in p {
                let set = action == b'h';
                if prefix == Some(b'?') {
                    match mode {
                        47 | 1047 | 1049 => {
                            if set && self.active == 0 {
                                if mode == 1049 {
                                    let s = &mut self.screens[0];
                                    s.saved = (s.row, s.col, self.style);
                                }
                                self.active = 1;
                                if mode != 47 {
                                    let size = self.size();
                                    self.screens[1] =
                                        Screen::new(size.rows as usize, size.cols as usize, 0, 0);
                                }
                            } else if !set && self.active == 1 {
                                self.active = 0;
                                if mode == 1049 {
                                    let s = &mut self.screens[0];
                                    s.row = s.saved.0.min(s.rows - 1);
                                    s.col = s.saved.1.min(s.cols - 1);
                                    self.style = s.saved.2;
                                }
                            }
                        }
                        1 | 6 | 7 | 25 | 1000 | 1002 | 1003 | 1004 | 1005 | 1006 | 1007 | 1015
                        | 1016 | 2004 | 2026 => {
                            if set && matches!(mode, 1000 | 1002 | 1003) {
                                for m in [1000, 1002, 1003] {
                                    self.set_mode(m, false);
                                }
                            }
                            self.set_mode(mode, set);
                            if mode == 6 {
                                let s = &mut self.screens[self.active];
                                s.row = if set { s.top } else { 0 };
                                s.col = 0;
                                s.wrap = false;
                            }
                        }
                        _ => self.limited = true,
                    }
                } else if mode == 4 {
                    self.insert = set
                } else {
                    self.limited = true;
                }
            }
            return None;
        }
        if prefix.is_some() {
            self.limited = true;
            return None;
        }
        let origin = self.modes & ORIGIN != 0;
        let s = &mut self.screens[self.active];
        match action {
            b'A' => s.row = s.row.saturating_sub(n).max(if origin { s.top } else { 0 }),
            b'B' | b'e' => s.row = (s.row + n).min(if origin { s.bottom } else { s.rows - 1 }),
            b'C' | b'a' => s.col = (s.col + n).min(s.cols - 1),
            b'D' => s.col = s.col.saturating_sub(n),
            b'E' => {
                s.row = (s.row + n).min(s.rows - 1);
                s.col = 0;
            }
            b'F' => {
                s.row = s.row.saturating_sub(n);
                s.col = 0;
            }
            b'G' | b'`' => s.col = (n - 1).min(s.cols - 1),
            b'd' => {
                s.row = (n - 1 + if origin { s.top } else { 0 }).min(if origin {
                    s.bottom
                } else {
                    s.rows - 1
                })
            }
            b'H' | b'f' => {
                s.row = (n - 1 + if origin { s.top } else { 0 }).min(if origin {
                    s.bottom
                } else {
                    s.rows - 1
                });
                s.col = (p.get(1).copied().unwrap_or(1).max(1) as usize - 1).min(s.cols - 1);
            }
            b'J' => {
                s.erase_display(p[0], self.style);
                if p[0] == 3 {
                    self.screens[0].erase_history();
                }
            }
            b'K' => s.erase_line(p[0], self.style),
            b'@' | b'P' | b'X' => s.edit_chars(action, n, self.style),
            b'S' => s.scroll_up(n, self.style, true),
            b'T' => s.scroll_down(n, self.style),
            b'L' | b'M' => {
                if s.row >= s.top && s.row <= s.bottom {
                    let top = s.top;
                    s.top = s.row;
                    if action == b'L' {
                        s.scroll_down(n, self.style)
                    } else {
                        s.scroll_up(n, self.style, false)
                    }
                    s.top = top;
                }
            }
            b'r' => {
                let top = n - 1;
                let bottom = p
                    .get(1)
                    .copied()
                    .filter(|v| *v > 0)
                    .unwrap_or(s.rows as u16) as usize
                    - 1;
                if top < bottom && bottom < s.rows {
                    s.top = top;
                    s.bottom = bottom;
                    s.row = if origin { top } else { 0 };
                    s.col = 0;
                }
            }
            b's' => s.saved = (s.row, s.col, self.style),
            b'u' => {
                s.row = s.saved.0.min(s.rows - 1);
                s.col = s.saved.1.min(s.cols - 1);
                self.style = s.saved.2;
            }
            b'q' => {}
            _ => self.limited = true,
        }
        self.screens[self.active].wrap = false;
        None
    }
    fn sgr(&mut self, p: &[u16]) {
        let mut i = 0;
        while i < p.len() {
            let n = p[i];
            match n {
                0 => self.style = Style::default(),
                1 => self.style.bits |= 1,
                2 => self.style.bits |= 2,
                3 => self.style.bits |= 4,
                4 => self.style.bits |= 8,
                5 => self.style.bits |= 16,
                7 => self.style.bits |= 32,
                8 => self.style.bits |= 64,
                9 => self.style.bits |= 128,
                22 => self.style.bits &= !3,
                23 => self.style.bits &= !4,
                24 => self.style.bits &= !8,
                25 => self.style.bits &= !16,
                27 => self.style.bits &= !32,
                28 => self.style.bits &= !64,
                29 => self.style.bits &= !128,
                30..=37 => self.style.fg = Color::Index((n - 30) as u8),
                40..=47 => self.style.bg = Color::Index((n - 40) as u8),
                90..=97 => self.style.fg = Color::Index((n - 90 + 8) as u8),
                100..=107 => self.style.bg = Color::Index((n - 100 + 8) as u8),
                39 => self.style.fg = Color::Default,
                49 => self.style.bg = Color::Default,
                38 | 48 => {
                    let color =
                        if p.get(i + 1) == Some(&5) && p.get(i + 2).is_some_and(|v| *v < 256) {
                            i += 2;
                            Some(Color::Index(p[i] as u8))
                        } else if p.get(i + 1) == Some(&2)
                            && p.get(i + 4).is_some()
                            && p[i + 2..i + 5].iter().all(|v| *v < 256)
                        {
                            i += 4;
                            Some(Color::Rgb(p[i - 2] as u8, p[i - 1] as u8, p[i] as u8))
                        } else {
                            None
                        };
                    if let Some(color) = color {
                        if n == 38 {
                            self.style.fg = color
                        } else {
                            self.style.bg = color
                        }
                    } else {
                        self.limited = true;
                    }
                }
                _ => self.limited = true,
            }
            i += 1;
        }
    }
    fn string(&mut self, b: &[u8]) -> Option<Vec<u8>> {
        if b.starts_with(b"\x1b]") {
            let end = if b.ends_with(b"\x1b\\") {
                b.len() - 2
            } else {
                b.len() - 1
            };
            let body = &b[2..end];
            for (query, answer) in [
                (
                    b"10;?".as_slice(),
                    b"\x1b]10;rgb:dddd/dddd/dddd\x1b\\".as_slice(),
                ),
                (b"11;?", b"\x1b]11;rgb:1111/1111/1111\x1b\\"),
            ] {
                if body == query {
                    return Some(answer.to_vec());
                }
            }
            if let Some(report) = body.strip_prefix(b"7501;") {
                // Reports still reach attached terminals, which may show
                // them; only the feature query is answered here.
                if let program::Report::Query = self.programs.report(report) {
                    return Some(b"\x1b]7501;?\x1b\\".to_vec());
                }
            } else if body.starts_with(b"133;A") {
                // A new shell prompt: the previous command is over.
                self.programs.finish();
            }
        } else {
            self.limited = true;
        }
        None
    }
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = String::new();
        for line in self.screens[0].scrollback() {
            screen::paint_line(line, &self.clusters, &mut out);
            out.push_str("\x1b[0m\r\n");
        }
        let rows = self.screens[self.active].rows;
        for (n, line) in self.screens[self.active].visible().enumerate() {
            screen::paint_line(line, &self.clusters, &mut out);
            if n + 1 < rows {
                out.push_str("\x1b[0m\r\n");
            }
        }
        out.push_str("\x1b[0m");
        out.into_bytes()
    }
    pub fn restore(&self) -> Vec<u8> {
        let mut out = String::from(
            "\x18\x1b[?2026l\x1b[?1049h\x1b[<65535u\x1b[=0u\x1b[?1049l\x1b[<65535u\x1b[=0u\x1b[?6l\x1b[?7l\x1b[4l\x1b[0m\x1b[r\x1b[H\x1b[2J\x1b[3J",
        );
        // History is regenerated from cells, never from retained terminal commands.
        for line in self.screens[0].scrollback() {
            screen::paint_line(line, &self.clusters, &mut out);
            out.push_str("\x1b[0m\r\n");
        }
        for (row, line) in self.screens[0].visible().enumerate() {
            screen::paint_line(line, &self.clusters, &mut out);
            if row + 1 < self.screens[0].rows {
                out.push_str("\x1b[0m\r\n");
            }
        }
        self.restore_cursor(0, &mut out);
        self.keys[0].restore(&mut out);
        out.push_str("\x1b[?47h");
        self.screens[1].paint(&self.clusters, &mut out);
        self.restore_cursor(1, &mut out);
        self.keys[1].restore(&mut out);
        if self.active == 0 {
            out.push_str("\x1b[?47l");
        }
        let s = &self.screens[self.active];
        let _ = write!(out, "\x1b[{};{}r", s.top + 1, s.bottom + 1);
        for mode in [
            1, 6, 7, 25, 1000, 1002, 1003, 1004, 1005, 1006, 1007, 1015, 1016, 2004,
        ] {
            let _ = write!(
                out,
                "\x1b[?{mode}{}",
                if self.mode(mode) { 'h' } else { 'l' }
            );
        }
        let _ = write!(
            out,
            "\x1b[>4;{}m\x1b{}\x1b[4{}",
            self.modify,
            if self.keypad { '=' } else { '>' },
            if self.insert { 'h' } else { 'l' }
        );
        self.style.ansi(&mut out);
        let _ = write!(out, "\x1b[{} q", self.cursor_style);
        let row = if self.modes & ORIGIN != 0 {
            s.row.saturating_sub(s.top)
        } else {
            s.row
        };
        let _ = write!(out, "\x1b[{};{}H", row + 1, s.col + 1);
        if s.wrap {
            let mut col = s.col;
            if s.line(s.row)[col].width() == 0 && col > 0 {
                col -= 1;
            }
            let _ = write!(out, "\x1b[{};{}H", row + 1, col + 1);
            let cell = s.line(s.row)[col];
            cell.style().ansi(&mut out);
            screen::paint_cell(cell, &self.clusters, &mut out);
            self.style.ansi(&mut out);
        }
        out.push_str(std::str::from_utf8(self.lexer.continuation()).unwrap());
        out.into_bytes()
    }
    fn restore_cursor(&self, index: usize, out: &mut String) {
        let s = &self.screens[index];
        let _ = write!(
            out,
            "\x1b[{};{}r\x1b[{};{}H",
            s.top + 1,
            s.bottom + 1,
            s.saved.0 + 1,
            s.saved.1 + 1
        );
        s.saved.2.ansi(out);
        out.push_str("\x1b7");
        s.cursor(out);
    }
}

#[cfg(test)]
mod tests;
