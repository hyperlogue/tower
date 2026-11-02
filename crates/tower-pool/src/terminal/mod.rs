mod lexer;
mod screen;
use lexer::{Lexer, Token};
use screen::{Color, Line, Screen, Style};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write,
};
use tower_pool_proto::Size;
const MAX_HISTORY: usize = 10_000;
const MAX_BYTES: usize = 16 * 1024 * 1024;
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
    utf8: Vec<u8>,
    screens: [Screen; 2],
    active: usize,
    keys: [Keyboard; 2],
    modes: BTreeMap<u16, bool>,
    style: Style,
    keypad: bool,
    insert: bool,
    modify: u16,
    cursor_style: u16,
    history: VecDeque<Line>,
    history_bytes: usize,
    pub truncated: bool,
    pub limited: bool,
}
impl Terminal {
    pub fn new(size: Size) -> Self {
        Self {
            lexer: Lexer::default(),
            utf8: Vec::new(),
            screens: [
                Screen::new(size.rows as usize, size.cols as usize),
                Screen::new(size.rows as usize, size.cols as usize),
            ],
            active: 0,
            keys: Default::default(),
            modes: [(7, true), (25, true)].into(),
            style: Style::default(),
            keypad: false,
            insert: false,
            modify: 0,
            cursor_style: 0,
            history: VecDeque::new(),
            history_bytes: 0,
            truncated: false,
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
        self.bound_history();
    }
    pub(crate) fn mode(&self, n: u16) -> bool {
        *self.modes.get(&n).unwrap_or(&false)
    }
    fn remember(&mut self, lines: Vec<Line>) {
        if self.active != 0
            || self.screens[0].top != 0
            || self.screens[0].bottom + 1 != self.screens[0].rows
        {
            return;
        }
        for line in lines {
            self.history_bytes += screen::line_bytes(&line);
            self.history.push_back(line);
        }
        self.bound_history();
    }
    fn bound_history(&mut self) {
        let screen_bytes: usize = self
            .screens
            .iter()
            .flat_map(|screen| &screen.lines)
            .map(screen::line_bytes)
            .sum();
        while self.history.len() > MAX_HISTORY || self.history_bytes + screen_bytes > MAX_BYTES {
            let Some(line) = self.history.pop_front() else {
                break;
            };
            self.history_bytes -= screen::line_bytes(&line);
            self.truncated = true;
        }
    }
    pub fn process(&mut self, bytes: &[u8]) -> Output {
        let mut output = Output {
            bytes: Vec::new(),
            replies: Vec::new(),
        };
        for token in self.lexer.push(bytes) {
            match token {
                Token::Text(bytes) => {
                    self.utf8.extend(bytes);
                    let mut used = 0;
                    while used < self.utf8.len() {
                        match std::str::from_utf8(&self.utf8[used..]) {
                            Ok(s) => {
                                let text = s.to_owned();
                                used = self.utf8.len();
                                for ch in text.chars() {
                                    self.character(ch)
                                }
                            }
                            Err(e) => {
                                let valid = e.valid_up_to();
                                if valid > 0 {
                                    let text =
                                        String::from_utf8_lossy(&self.utf8[used..used + valid])
                                            .into_owned();
                                    for ch in text.chars() {
                                        self.character(ch)
                                    }
                                    used += valid;
                                }
                                if let Some(n) = e.error_len() {
                                    self.character('\u{fffd}');
                                    used += n;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                    output.bytes.extend(self.utf8.drain(..used));
                }
                Token::Control(bytes) => {
                    self.flush_utf8(&mut output.bytes);
                    let reply = self.control(&bytes);
                    if let Some(reply) = reply {
                        output.replies.extend(reply)
                    } else {
                        output.bytes.extend(bytes)
                    }
                }
                Token::String(bytes) => {
                    self.flush_utf8(&mut output.bytes);
                    if let Some(reply) = self.string(&bytes) {
                        output.replies.extend(reply)
                    } else {
                        output.bytes.extend(bytes)
                    }
                }
                Token::Opaque(bytes) => {
                    self.flush_utf8(&mut output.bytes);
                    self.limited = true;
                    output.bytes.extend(bytes);
                }
            }
        }
        output
    }
    fn flush_utf8(&mut self, out: &mut Vec<u8>) {
        if !self.utf8.is_empty() {
            self.character('\u{fffd}');
            out.append(&mut self.utf8);
        }
    }
    fn character(&mut self, ch: char) {
        let wrap = self.mode(7);
        let lines = self.screens[self.active].put(ch, self.style, wrap, self.insert);
        self.limited |= self.screens[self.active].limited;
        self.remember(lines);
    }
    fn control(&mut self, b: &[u8]) -> Option<Vec<u8>> {
        if b.len() == 1 {
            let s = &mut self.screens[self.active];
            match b[0] {
                b'\r' => {
                    s.col = 0;
                    s.wrap = false;
                }
                b'\n' | 11 | 12 => {
                    let lines = s.newline(self.style);
                    self.remember(lines);
                }
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
                b'D' => {
                    let lines = s.newline(self.style);
                    self.remember(lines);
                }
                b'E' => {
                    s.col = 0;
                    let lines = s.newline(self.style);
                    self.remember(lines);
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
                b'c' => {
                    let size = self.size();
                    *self = Self::new(size);
                }
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
        let Ok(text) = std::str::from_utf8(raw) else {
            self.limited = true;
            return None;
        };
        let Some(p) = text
            .split(';')
            .map(|v| {
                if v.is_empty() {
                    Some(0)
                } else {
                    v.parse::<u16>().ok()
                }
            })
            .collect::<Option<Vec<_>>>()
        else {
            self.limited = true;
            return None;
        };
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
            self.keys[self.active].apply(prefix.unwrap(), &p);
            return None;
        }
        if action == b'm' {
            if prefix == Some(b'>') && p[0] == 4 {
                self.modify = p.get(1).copied().unwrap_or(0).min(2)
            } else if prefix.is_none() {
                self.sgr(&p)
            }
            return None;
        }
        if matches!(action, b'h' | b'l') {
            for &mode in &p {
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
                                        Screen::new(size.rows as usize, size.cols as usize);
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
                                    self.modes.insert(m, false);
                                }
                            }
                            self.modes.insert(mode, set);
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
        let origin = self.mode(6);
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
                    self.history.clear();
                    self.history_bytes = 0;
                }
            }
            b'K' => s.erase_line(p[0], self.style),
            b'@' | b'P' | b'X' => s.edit_chars(action, n, self.style),
            b'S' => {
                let lines = s.scroll_up(n, self.style);
                self.remember(lines);
            }
            b'T' => s.scroll_down(n, self.style),
            b'L' | b'M' => {
                if s.row >= s.top && s.row <= s.bottom {
                    let top = s.top;
                    s.top = s.row;
                    if action == b'L' {
                        s.scroll_down(n, self.style)
                    } else {
                        s.scroll_up(n, self.style);
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
        } else {
            self.limited = true;
        }
        None
    }
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = String::new();
        for line in &self.history {
            screen::paint_line(line, &mut out);
            out.push_str("\x1b[0m\r\n");
        }
        for (n, line) in self.screens[self.active].lines.iter().enumerate() {
            screen::paint_line(line, &mut out);
            if n + 1 < self.screens[self.active].rows {
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
        for line in &self.history {
            screen::paint_line(line, &mut out);
            out.push_str("\x1b[0m\r\n");
        }
        for (row, line) in self.screens[0].lines.iter().enumerate() {
            screen::paint_line(line, &mut out);
            if row + 1 < self.screens[0].rows {
                out.push_str("\x1b[0m\r\n");
            }
        }
        self.restore_cursor(0, &mut out);
        self.keys[0].restore(&mut out);
        out.push_str("\x1b[?47h");
        self.screens[1].paint(&mut out);
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
        let row = if self.mode(6) {
            s.row.saturating_sub(s.top)
        } else {
            s.row
        };
        let _ = write!(out, "\x1b[{};{}H", row + 1, s.col + 1);
        if s.wrap && self.mode(7) {
            let mut col = s.col;
            if s.lines[s.row][col].width == 0 && col > 0 {
                col -= 1;
            }
            let _ = write!(out, "\x1b[{};{}H", row + 1, col + 1);
            let cell = &s.lines[s.row][col];
            cell.style.ansi(&mut out);
            if cell.text.is_empty() {
                out.push(' ')
            } else {
                out.push_str(&cell.text)
            }
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
