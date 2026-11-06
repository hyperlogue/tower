#[derive(Default)]
pub struct Lexer {
    state: State,
    pending: Vec<u8>,
    /// An incomplete UTF-8 sequence at the end of the previous chunk.
    carry: Vec<u8>,
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
        allowed: bool,
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
/// A clipboard read returns the human's local clipboard to the session as
/// input, so it never reaches attached clients. Everything else, including
/// clipboard writes, is forwarded as when running the program directly.
/// Returns `None` while the prefix cannot decide yet.
fn allowed_string(pending: &[u8], complete: bool) -> Option<bool> {
    if pending[1] != b']' {
        return Some(true);
    }
    let body = &pending[2..];
    let digits = body.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits > 16 {
        // No real OSC number is this long; it cannot be classified safely.
        return Some(false);
    }
    let (number, rest) = body.split_at(digits);
    let args = match rest.split_first() {
        Some((b';', args)) => args,
        None if !complete => return None,
        _ => return Some(true),
    };
    // Terminals parse the number numerically, so leading zeros are equivalent.
    let number = number
        .iter()
        .fold(0u64, |n, d| n * 10 + u64::from(d - b'0'));
    if !matches!(number, 52 | 5522) {
        return Some(true);
    }
    let end = args.iter().position(|&b| b == b';');
    if end.is_none() && !complete {
        return None;
    }
    if number == 52 {
        // Selection, then base64 data or `?` to read.
        return match end.map(|i| args.get(i + 1)) {
            Some(Some(&b)) => Some(b != b'?'),
            _ if complete => Some(true),
            _ => None,
        };
    }
    // Kitty: colon-separated metadata, then the payload.
    Some(
        args[..end.unwrap_or(args.len())]
            .split(|&b| b == b':')
            .any(|kv| matches!(kv, b"type=write" | b"type=wdata" | b"type=walias")),
    )
}
impl Lexer {
    /// Forward only valid UTF-8 without C1 controls (U+0080..U+009F).
    /// Terminals treat C1 controls, and raw 0x80..0x9F bytes in 8-bit mode,
    /// like their ESC equivalents, for example U+009D as OSC. They are never
    /// printable, so every client then parses what tower parses. Invalid
    /// bytes become U+FFFD.
    fn sanitize(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut input = std::mem::take(&mut self.carry);
        input.extend_from_slice(bytes);
        let mut out = Vec::with_capacity(input.len());
        let mut rest = &input[..];
        loop {
            let (valid, error) = match std::str::from_utf8(rest) {
                Ok(_) => (rest, None),
                Err(e) => (&rest[..e.valid_up_to()], Some(e)),
            };
            let mut i = 0;
            while i < valid.len() {
                // In valid UTF-8, C1 controls are exactly C2 80..C2 9F.
                if valid[i] == 0xc2 && valid[i + 1] <= 0x9f {
                    i += 2;
                } else {
                    out.push(valid[i]);
                    i += 1;
                }
            }
            let Some(error) = error else { break };
            let Some(len) = error.error_len() else {
                self.carry = rest[valid.len()..].to_vec();
                break;
            };
            out.extend_from_slice("\u{fffd}".as_bytes());
            rest = &rest[valid.len() + len..];
        }
        out
    }
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Token> {
        let bytes = self.sanitize(bytes);
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
                if matches!(self.state, State::String { allowed: false, .. }) {
                    self.pending = vec![b];
                }
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
                            allowed: false,
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
                State::String {
                    osc,
                    escape,
                    allowed,
                } => {
                    let done = (*escape && b == b'\\') || (*osc && b == 7);
                    *escape = b == 27;
                    if done {
                        let pending = std::mem::take(&mut self.pending);
                        let body = pending.len() - if b == 7 { 1 } else { 2 };
                        if *allowed || allowed_string(&pending[..body], true) == Some(true) {
                            out.push(Token::String(pending));
                        }
                        self.state = State::Text;
                    } else if !*allowed && b != 27 {
                        match allowed_string(&self.pending, false) {
                            Some(true) => *allowed = true,
                            Some(false) => {
                                self.pending.clear();
                                self.state = State::DiscardString { escape: false };
                                continue;
                            }
                            None => {}
                        }
                    }
                }
                _ => {}
            }
            if self.pending.len() > 512 {
                if let State::String {
                    allowed: false,
                    escape,
                    ..
                } = self.state
                {
                    // Too long to classify safely.
                    self.pending.clear();
                    self.state = State::DiscardString { escape };
                    continue;
                }
                let held = match self.state {
                    State::String { escape, .. } => escape,
                    _ => false,
                };
                if held {
                    self.pending.pop();
                }
                out.push(Token::Opaque(std::mem::take(&mut self.pending)));
                self.state = match self.state {
                    State::String { osc, escape, .. } => State::LongString { osc, escape },
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
