/// The longest escape sequence held back whole. Longer ones are forwarded
/// in pieces without being interpreted.
const SEQUENCE: usize = 512;
/// The longest string sequence held back whole once it is known not to be a
/// clipboard read: a program status report may take 4096 bytes.
const STRING: usize = 4096;
#[derive(Default)]
pub struct Lexer {
    state: State,
    pending: Vec<u8>,
    /// An incomplete UTF-8 sequence at the end of the previous chunk.
    carry: Vec<u8>,
    scratch: Vec<u8>,
}
#[derive(Clone, Copy, Default)]
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
/// Tokens borrow the input or the lexer's buffer for the duration of the
/// callback. Text has no control characters and is whole UTF-8 characters,
/// except after an ESC that consumed the first byte of one.
pub enum Token<'a> {
    Text(&'a [u8]),
    Control(&'a [u8]),
    String(&'a [u8]),
    Opaque(&'a [u8]),
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
enum Kind {
    Control,
    String,
    Opaque,
}
/// The first C1 control (U+0080..U+009F) in valid UTF-8: exactly C2 80..C2 9F.
fn c1(valid: &[u8]) -> Option<usize> {
    let mut from = 0;
    while let Some(i) = valid[from..].iter().position(|&b| b == 0xc2) {
        if valid[from + i + 1] <= 0x9f {
            return Some(from + i);
        }
        from += i + 2;
    }
    None
}
fn run(bytes: &[u8], stop: impl Fn(u8) -> bool) -> usize {
    bytes.iter().position(|&b| stop(b)).unwrap_or(bytes.len())
}
impl Lexer {
    /// Forward only valid UTF-8 without C1 controls (U+0080..U+009F).
    /// Terminals treat C1 controls, and raw 0x80..0x9F bytes in 8-bit mode,
    /// like their ESC equivalents, for example U+009D as OSC. They are never
    /// printable, so every client then parses what tower parses. Invalid
    /// bytes become U+FFFD.
    fn sanitize(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        let mut input = std::mem::take(&mut self.carry);
        input.extend_from_slice(bytes);
        let mut rest = &input[..];
        loop {
            let (mut valid, error) = match std::str::from_utf8(rest) {
                Ok(_) => (rest, None),
                Err(e) => (&rest[..e.valid_up_to()], Some(e)),
            };
            let consumed = valid.len();
            while let Some(at) = c1(valid) {
                out.extend_from_slice(&valid[..at]);
                valid = &valid[at + 2..];
            }
            out.extend_from_slice(valid);
            let Some(error) = error else { break };
            let Some(len) = error.error_len() else {
                self.carry.extend_from_slice(&rest[consumed..]);
                break;
            };
            out.extend_from_slice("\u{fffd}".as_bytes());
            rest = &rest[consumed + len..];
        }
    }
    /// The length of the prefix that needs no sanitizing, when the rest is
    /// only an incomplete character, which is kept for the next chunk.
    fn clean(&mut self, bytes: &[u8]) -> Option<usize> {
        if !self.carry.is_empty() {
            return None;
        }
        let valid = match std::str::from_utf8(bytes) {
            Ok(_) => bytes.len(),
            Err(e) if e.error_len().is_none() => e.valid_up_to(),
            Err(_) => return None,
        };
        if c1(&bytes[..valid]).is_some() {
            return None;
        }
        self.carry.extend_from_slice(&bytes[valid..]);
        Some(valid)
    }
    pub fn push(&mut self, bytes: &[u8], f: &mut impl FnMut(Token<'_>)) {
        if let Some(n) = self.clean(bytes) {
            return self.lex(&bytes[..n], f);
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        self.sanitize(bytes, &mut scratch);
        self.lex(&scratch, f);
        scratch.clear();
        self.scratch = scratch;
    }
    fn lex(&mut self, bytes: &[u8], f: &mut impl FnMut(Token<'_>)) {
        let mut i = 0;
        while i < bytes.len() {
            let rest = &bytes[i..];
            // Runs that need no per-byte decision are passed on as slices.
            let n = match self.state {
                State::Text => {
                    let n = run(rest, |b| b < 32 || b == 127);
                    if n > 0 {
                        f(Token::Text(&rest[..n]));
                    }
                    n
                }
                State::LongString { osc, escape: false } => {
                    let n = run(rest, |b| matches!(b, 24 | 26 | 27) || osc && b == 7);
                    if n > 0 {
                        f(Token::Opaque(&rest[..n]));
                    }
                    n
                }
                State::LongCsi => {
                    let n = run(rest, |b| matches!(b, 24 | 26 | 27 | 0x40..=0x7e));
                    if n > 0 {
                        f(Token::Opaque(&rest[..n]));
                    }
                    n
                }
                State::DiscardString { escape: false } => {
                    run(rest, |b| matches!(b, 7 | 24 | 26 | 27))
                }
                _ => 0,
            };
            if n > 0 {
                i += n;
            } else {
                self.byte(bytes[i], f);
                i += 1;
            }
        }
    }
    fn restart(&mut self) {
        self.state = State::Escape;
        self.pending.clear();
        self.pending.push(27);
    }
    fn flush(&mut self, f: &mut impl FnMut(Token<'_>), kind: Kind) {
        let pending = std::mem::take(&mut self.pending);
        f(match kind {
            Kind::Control => Token::Control(&pending),
            Kind::String => Token::String(&pending),
            Kind::Opaque => Token::Opaque(&pending),
        });
        self.pending = pending;
        self.pending.clear();
    }
    fn byte(&mut self, b: u8, f: &mut impl FnMut(Token<'_>)) {
        // An ESC inside a string or control sequence ends it and starts a
        // new sequence, as in the standard terminal parser.
        match self.state {
            State::DiscardString { escape } => {
                if escape && b != b'\\' {
                    self.restart();
                } else {
                    self.state = if (escape && b == b'\\') || matches!(b, 7 | 24 | 26) {
                        State::Text
                    } else {
                        State::DiscardString { escape: b == 27 }
                    };
                    return;
                }
            }
            State::Text => {
                if b == 27 {
                    self.pending.push(b);
                    self.state = State::Escape;
                } else {
                    f(Token::Control(&[b]));
                }
                return;
            }
            State::LongCsi => {
                if b == 27 {
                    self.restart();
                } else {
                    f(Token::Opaque(&[b]));
                    self.state = State::Text;
                }
                return;
            }
            State::LongString { osc, escape } => {
                if escape {
                    if b == b'\\' {
                        f(Token::Opaque(b"\x1b\\"));
                        self.state = State::Text;
                        return;
                    }
                    self.restart();
                } else if b == 27 {
                    self.state = State::LongString { osc, escape: true };
                    return;
                } else {
                    f(Token::Opaque(&[b]));
                    self.state = State::Text;
                    return;
                }
            }
            // Drop the aborted string; it was never forwarded.
            State::String { escape: true, .. } if b != b'\\' => self.restart(),
            _ => {}
        }
        if matches!(self.state, State::Escape) && b == 27 {
            // ESC restarts an escape sequence; the abandoned prefix has no effect.
            return self.restart();
        }
        self.pending.push(b);
        if matches!(b, 24 | 26) {
            if matches!(self.state, State::String { allowed: false, .. }) {
                self.pending.clear();
                self.pending.push(b);
            }
            self.flush(f, Kind::Opaque);
            self.state = State::Text;
            return;
        }
        match self.state {
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
                    self.flush(f, Kind::Control);
                    self.state = State::Text;
                }
            },
            State::Csi => {
                if (0x40..=0x7e).contains(&b) {
                    self.flush(f, Kind::Control);
                    self.state = State::Text;
                } else if b == 27 {
                    self.pending.pop();
                    self.flush(f, Kind::Opaque);
                    self.restart();
                }
            }
            State::String {
                osc,
                escape,
                allowed,
            } => {
                if (escape && b == b'\\') || (osc && b == 7) {
                    let body = self.pending.len() - if b == 7 { 1 } else { 2 };
                    if allowed || allowed_string(&self.pending[..body], true) == Some(true) {
                        self.flush(f, Kind::String);
                    } else {
                        self.pending.clear();
                    }
                    self.state = State::Text;
                } else {
                    let allowed = allowed
                        || b != 27
                            && match allowed_string(&self.pending, false) {
                                Some(true) => true,
                                Some(false) => {
                                    self.pending.clear();
                                    self.state = State::DiscardString { escape: false };
                                    return;
                                }
                                None => false,
                            };
                    self.state = State::String {
                        osc,
                        escape: b == 27,
                        allowed,
                    };
                }
            }
            _ => {}
        }
        let limit = match self.state {
            State::String { allowed: true, .. } => STRING,
            _ => SEQUENCE,
        };
        if self.pending.len() > limit {
            let held = match self.state {
                State::String {
                    allowed: false,
                    escape,
                    ..
                } => {
                    // Too long to classify safely.
                    self.pending.clear();
                    self.state = State::DiscardString { escape };
                    return;
                }
                State::String { escape, .. } => escape,
                _ => false,
            };
            if held {
                self.pending.pop();
            }
            self.flush(f, Kind::Opaque);
            self.state = match self.state {
                State::String { osc, escape, .. } => State::LongString { osc, escape },
                _ => State::LongCsi,
            };
        }
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
