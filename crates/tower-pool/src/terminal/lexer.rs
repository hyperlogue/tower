#[derive(Default)]
pub struct Lexer {
    state: State,
    pending: Vec<u8>,
    /// A UTF-8 lead byte that may begin an encoded C1 control.
    lead: bool,
}
#[derive(Default)]
enum State {
    #[default]
    Text,
    Escape,
    Csi,
    String {
        osc: bool,
        escape: bool,
    },
    /// Forwarded in pieces. A trailing ESC is held back until the next byte
    /// shows whether it terminates the string or starts a new sequence.
    LongString {
        osc: bool,
        escape: bool,
    },
    LongCsi,
    DiscardString {
        escape: bool,
    },
}
pub enum Token {
    Text(Vec<u8>),
    Control(Vec<u8>),
    String(Vec<u8>),
    Opaque(Vec<u8>),
}
/// Clipboard access needs an authorized route to one client. Never broadcast
/// OSC 52 or kitty OSC 5522 payloads or clipboard-read queries as raw output.
/// Terminals parse the OSC number numerically, so leading zeros are equivalent.
fn blocked_osc(pending: &[u8]) -> bool {
    let number = &pending[2..];
    let Some((&last, digits)) = number.split_last() else {
        return false;
    };
    if !digits.iter().all(u8::is_ascii_digit) {
        return false;
    }
    if last.is_ascii_digit() {
        // No real OSC number is this long; it cannot be classified safely.
        return number.len() > 16;
    }
    last == b';'
        && matches!(
            digits
                .iter()
                .fold(0u64, |n, d| n * 10 + u64::from(d - b'0')),
            52 | 5522
        )
}
impl Lexer {
    /// Terminals treat UTF-8-encoded C1 controls (U+0080..U+009F) like their
    /// ESC equivalents, for example U+009D as OSC. They are never printable, so
    /// remove them before lexing; every client then parses what tower parses.
    fn strip_c1(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len() + 1);
        for &b in bytes {
            if std::mem::take(&mut self.lead) {
                if (0x80..=0x9f).contains(&b) {
                    continue;
                }
                out.push(0xc2);
            }
            if b == 0xc2 {
                self.lead = true;
            } else {
                out.push(b);
            }
        }
        out
    }
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Token> {
        let bytes = self.strip_c1(bytes);
        let mut out = Vec::new();
        let mut text = Vec::new();
        for &b in &bytes {
            // An ESC inside a string or control sequence ends it and starts a
            // new sequence, as in the standard terminal parser.
            match &mut self.state {
                State::DiscardString { escape } => {
                    if *escape && b != b'\\' {
                        self.state = State::Escape;
                        self.pending = vec![27];
                    } else {
                        let done = (*escape && b == b'\\') || matches!(b, 7 | 24 | 26);
                        *escape = b == 27;
                        if done {
                            self.state = State::Text;
                        }
                        continue;
                    }
                }
                State::Text => {
                    if b == 27 {
                        if !text.is_empty() {
                            out.push(Token::Text(std::mem::take(&mut text)))
                        }
                        self.pending.push(b);
                        self.state = State::Escape;
                    } else if b < 32 || b == 127 {
                        if !text.is_empty() {
                            out.push(Token::Text(std::mem::take(&mut text)))
                        }
                        out.push(Token::Control(vec![b]));
                    } else {
                        text.push(b)
                    }
                    continue;
                }
                State::LongCsi => {
                    if b == 27 {
                        if !text.is_empty() {
                            out.push(Token::Opaque(std::mem::take(&mut text)));
                        }
                        self.state = State::Escape;
                        self.pending = vec![27];
                        continue;
                    }
                    text.push(b);
                    if (0x40..=0x7e).contains(&b) || matches!(b, 24 | 26) {
                        out.push(Token::Opaque(std::mem::take(&mut text)));
                        self.state = State::Text;
                    }
                    continue;
                }
                State::LongString { osc, escape } => {
                    if *escape {
                        *escape = false;
                        if b == b'\\' {
                            text.extend([27, b'\\']);
                            out.push(Token::Opaque(std::mem::take(&mut text)));
                            self.state = State::Text;
                            continue;
                        }
                        if !text.is_empty() {
                            out.push(Token::Opaque(std::mem::take(&mut text)));
                        }
                        self.state = State::Escape;
                        self.pending = vec![27];
                    } else if b == 27 {
                        *escape = true;
                        continue;
                    } else {
                        text.push(b);
                        if (*osc && b == 7) || matches!(b, 24 | 26) {
                            out.push(Token::Opaque(std::mem::take(&mut text)));
                            self.state = State::Text;
                        }
                        continue;
                    }
                }
                State::String { escape: true, .. } if b != b'\\' => {
                    // Drop the aborted string; it was never forwarded.
                    self.state = State::Escape;
                    self.pending = vec![27];
                }
                _ => {}
            }
            if !text.is_empty() {
                out.push(Token::Opaque(std::mem::take(&mut text)))
            }
            if matches!(self.state, State::Escape) && b == 27 {
                // ESC restarts an escape sequence; the abandoned prefix has no effect.
                self.pending = vec![27];
                continue;
            }
            self.pending.push(b);
            if matches!(b, 24 | 26) {
                out.push(Token::Opaque(std::mem::take(&mut self.pending)));
                self.state = State::Text;
                continue;
            }
            match &mut self.state {
                State::Escape => match b {
                    b'[' if self.pending.len() == 2 => self.state = State::Csi,
                    b']' | b'P' | b'X' | b'^' | b'_' if self.pending.len() == 2 => {
                        self.state = State::String {
                            osc: b == b']',
                            escape: false,
                        }
                    }
                    0x20..=0x2f => {}
                    _ => {
                        out.push(Token::Control(std::mem::take(&mut self.pending)));
                        self.state = State::Text;
                    }
                },
                State::Csi => {
                    if (0x40..=0x7e).contains(&b) {
                        out.push(Token::Control(std::mem::take(&mut self.pending)));
                        self.state = State::Text;
                    } else if b == 27 {
                        self.pending.pop();
                        out.push(Token::Opaque(std::mem::take(&mut self.pending)));
                        self.pending.push(27);
                        self.state = State::Escape;
                    }
                }
                State::String { osc, escape } => {
                    let done = (*escape && b == b'\\') || (*osc && b == 7);
                    *escape = b == 27;
                    if done {
                        out.push(Token::String(std::mem::take(&mut self.pending)));
                        self.state = State::Text;
                    } else if *osc && blocked_osc(&self.pending) {
                        self.pending.clear();
                        self.state = State::DiscardString { escape: false };
                        continue;
                    }
                }
                _ => {}
            }
            if self.pending.len() > 512 {
                let held = match self.state {
                    State::String { escape, .. } => escape,
                    _ => false,
                };
                if held {
                    self.pending.pop();
                }
                out.push(Token::Opaque(std::mem::take(&mut self.pending)));
                self.state = match self.state {
                    State::String { osc, escape } => State::LongString { osc, escape },
                    _ => State::LongCsi,
                };
            }
        }
        if !text.is_empty() {
            out.push(if matches!(self.state, State::Text) {
                Token::Text(text)
            } else {
                Token::Opaque(text)
            });
        }
        out
    }
    pub fn continuation(&self) -> &'static [u8] {
        match self.state {
            State::LongString { osc: true, .. } => b"\x1b]99999;",
            State::LongString { osc: false, .. } => b"\x1bP99999z",
            State::LongCsi => b"\x1b[??",
            _ => b"",
        }
    }
}
