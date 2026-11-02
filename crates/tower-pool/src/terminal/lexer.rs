#[derive(Default)]
pub struct Lexer {
    state: State,
    pending: Vec<u8>,
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
impl Lexer {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Token> {
        let mut out = Vec::new();
        let mut text = Vec::new();
        for &b in bytes {
            if let State::DiscardString { escape } = &mut self.state {
                let done = (*escape && b == b'\\') || matches!(b, 7 | 24 | 26);
                *escape = b == 27;
                if done {
                    self.state = State::Text;
                }
                continue;
            }
            if matches!(self.state, State::Text) {
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
            if matches!(self.state, State::LongCsi | State::LongString { .. }) {
                let done = match &mut self.state {
                    State::LongCsi => (0x40..=0x7e).contains(&b) || matches!(b, 24 | 26),
                    State::LongString { osc, escape } => {
                        let done =
                            (*escape && b == b'\\') || (*osc && b == 7) || matches!(b, 24 | 26);
                        *escape = b == 27;
                        done
                    }
                    _ => false,
                };
                text.push(b);
                if done {
                    out.push(Token::Opaque(std::mem::take(&mut text)));
                    self.state = State::Text;
                }
                continue;
            }
            if !text.is_empty() {
                out.push(Token::Opaque(std::mem::take(&mut text)))
            }
            self.pending.push(b);
            // Clipboard access needs an authorized route to one client. Never
            // broadcast OSC 52 payloads or clipboard-read queries as raw output.
            if self.pending == b"\x1b]52;" {
                self.pending.clear();
                self.state = State::DiscardString { escape: false };
                continue;
            }
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
                    }
                }
                _ => {}
            }
            if self.pending.len() > 512 {
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
            State::LongString {
                osc: true,
                escape: true,
            } => b"\x1b]99999;\x1b",
            State::LongString {
                osc: false,
                escape: true,
            } => b"\x1bP99999z\x1b",
            State::LongString { osc: true, .. } => b"\x1b]99999;",
            State::LongString { osc: false, .. } => b"\x1bP99999z",
            State::LongCsi => b"\x1b[??",
            _ => b"",
        }
    }
}
