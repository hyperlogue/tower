use std::time::{Duration, Instant};
const MAX_PENDING: usize = 1024 * 1024;
#[derive(Default)]
pub struct Input {
    pending: Vec<u8>,
    paste: bool,
    deadline: Option<Instant>,
}
impl Input {
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub fn push(&mut self, bytes: &[u8], now: Instant) -> Result<Vec<Vec<u8>>, ()> {
        let mut out = Vec::new();
        let mut plain = Vec::new();
        for &b in bytes {
            if self.pending.is_empty() && b != 27 && b < 128 {
                plain.push(b);
                continue;
            }
            if !plain.is_empty() {
                out.push(std::mem::take(&mut plain));
            }
            self.pending.push(b);
            self.deadline = Some(now + Duration::from_millis(50));
            if self.pending.len() > MAX_PENDING {
                return Err(());
            }
            let done = if self.paste {
                self.pending.ends_with(b"\x1b[201~")
            } else {
                let p = &self.pending;
                match p.as_slice() {
                    [27] => false,
                    [27, b'[', ..] => p.len() > 2 && (0x40..=0x7e).contains(&b),
                    [27, b'O', ..] => p.len() > 2,
                    [27, b']', ..] => p.len() > 2 && (b == 7 || p.ends_with(b"\x1b\\")),
                    [27, b'P' | b'_' | b'^' | b'X', ..] => p.len() > 2 && p.ends_with(b"\x1b\\"),
                    [27, first, ..] => p.len() > utf8_len(*first),
                    [first, ..] => p.len() >= utf8_len(*first),
                    [] => false,
                }
            };
            if done {
                if !self.paste && self.pending == b"\x1b[200~" {
                    self.paste = true;
                } else {
                    out.push(std::mem::take(&mut self.pending));
                    self.paste = false;
                    self.deadline = None;
                }
            }
        }
        if !plain.is_empty() {
            out.push(plain)
        }
        Ok(out)
    }
    pub fn expire(&mut self, now: Instant) -> Option<Vec<u8>> {
        if self.deadline.is_some_and(|d| now >= d) {
            self.deadline = None;
            self.paste = false;
            Some(std::mem::take(&mut self.pending))
        } else {
            None
        }
    }
}
fn utf8_len(b: u8) -> usize {
    match b {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frames_keys_strings_paste_and_utf8_without_interleaving() {
        let now = Instant::now();
        let mut a = Input::default();
        for bytes in [
            b"\x1bOA".as_slice(),
            b"\x1b]title\x07",
            b"\x1bPdata\x1b\\",
            b"\x1b]title\x1b\\",
        ] {
            for &byte in &bytes[..bytes.len() - 1] {
                assert!(a.push(&[byte], now).unwrap().is_empty());
            }
            assert_eq!(
                a.push(&bytes[bytes.len() - 1..], now).unwrap(),
                vec![bytes.to_vec()]
            );
        }
        assert!(a.push(b"\x1b[", now).unwrap().is_empty());
        assert_eq!(a.push(b"A", now).unwrap(), vec![b"\x1b[A".to_vec()]);
        assert!(a.push(b"\x1b[200~hello\x1b[", now).unwrap().is_empty());
        assert_eq!(
            a.push(b"201~", now).unwrap(),
            vec![b"\x1b[200~hello\x1b[201~".to_vec()]
        );
        assert!(a.push(&[0xe4, 0xb8], now).unwrap().is_empty());
        assert_eq!(
            a.push(&[0xad], now).unwrap(),
            vec!["中".as_bytes().to_vec()]
        );
        a.push(b"\x1bO", now).unwrap();
        assert_eq!(
            a.expire(now + Duration::from_millis(51)),
            Some(b"\x1bO".to_vec())
        );
    }
}
