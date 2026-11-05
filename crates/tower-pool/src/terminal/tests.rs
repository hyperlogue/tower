use super::*;
fn terminal() -> Terminal {
    Terminal::new(Size { rows: 4, cols: 12 })
}
fn text(t: &Terminal) -> String {
    t.screens[t.active]
        .lines
        .iter()
        .map(|line| {
            line.iter()
                .filter(|c| c.width > 0)
                .map(|c| {
                    if c.text.is_empty() {
                        " "
                    } else {
                        c.text.as_str()
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}
#[test]
fn reattach_restores_both_keyboard_stacks_and_follows_later_pops() {
    let mut original = terminal();
    original.process(b"\x1b[>7u\x1b[>3u\x1b[?2004hPRIMARY\x1b[?1049h\x1b[>5u\x1b[>1uALT");
    let mut attached = terminal();
    attached.process(&original.restore());
    for (input, expected) in [
        (b"\x1b[<u".as_slice(), 5),
        (b"\x1b[?1049l", 3),
        (b"\x1b[<u", 7),
        (b"\x1b[<65535u", 0),
    ] {
        original.process(input);
        attached.process(input);
        assert_eq!(attached.keys[attached.active].flags, expected);
        assert_eq!(original.active, attached.active);
        assert_eq!(
            original.keys[original.active].flags,
            attached.keys[attached.active].flags
        );
        assert_eq!(
            original.keys[original.active].saved,
            attached.keys[attached.active].saved
        );
    }
    assert!(attached.mode(2004));
}
#[test]
fn snapshots_and_reattach_never_replay_side_effects_or_queries() {
    let mut t = terminal();
    t.process(b"hello\x1b]52;c;PRIVATE_CLIPBOARD\x07\x1b]0;PRIVATE_TITLE\x1b\\\x1b[?u\x1b[6n");
    for bytes in [t.snapshot(), t.restore()] {
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.contains("hello"));
        assert!(!s.contains("PRIVATE"));
        assert!(!s.contains("\x1b[?u"));
        assert!(!s.contains("\x1b[6n"));
    }
    let out = t.process(b"\x1b[?u\x1b[6n\x1b]11;?\x07");
    assert!(out.bytes.is_empty());
    assert!(String::from_utf8(out.replies).unwrap().contains("rgb:"));
}
#[test]
fn arbitrary_read_splits_preserve_utf8_cells_modes_and_queries() {
    let bytes = "a中e\u{301}\x1b[31mred\x1b[>7u\x1b[?u\x1b]52;c;nope\x07\x1b[2;3H!".as_bytes();
    let mut expected = terminal();
    let baseline = expected.process(bytes);
    for cut in 0..=bytes.len() {
        let mut actual = terminal();
        let mut output = actual.process(&bytes[..cut]);
        let last = actual.process(&bytes[cut..]);
        output.bytes.extend(last.bytes);
        output.replies.extend(last.replies);
        assert_eq!(actual.snapshot(), expected.snapshot(), "cut {cut}");
        assert_eq!(actual.restore(), expected.restore(), "cut {cut}");
        assert_eq!(output.bytes, baseline.bytes);
        assert_eq!(output.replies, baseline.replies);
    }
}
#[test]
fn reattach_screen_and_future_text_agree_including_pending_wrap() {
    let mut a = terminal();
    a.process(b"123456789012");
    let mut b = terminal();
    b.process(&a.restore());
    a.process(b"next");
    b.process(b"next");
    assert_eq!(text(&a), text(&b));
    a.process(b"\x1b[2;2H\x1b[32mgreen\x1b[?1049hALT\x1b[3;3Hend");
    let mut b = terminal();
    b.process(&a.restore());
    assert_eq!(text(&a), text(&b));
    a.process(b"\x1b[?1049l");
    b.process(b"\x1b[?1049l");
    assert_eq!(text(&a), text(&b));
}
#[test]
fn history_is_bounded_and_survives_reconstruction() {
    let mut t = Terminal::new(Size { rows: 2, cols: 5 });
    t.process(b"one\r\ntwo\r\nthree\r\nfour");
    assert_eq!(t.history.len(), 2);
    let mut copy = Terminal::new(t.size());
    copy.process(&t.restore());
    assert_eq!(text(&t), text(&copy));
    assert_eq!(t.history.len(), copy.history.len());
    for _ in 0..10_100 {
        t.process(b"\r\nx");
    }
    assert!(t.truncated);
    assert_eq!(t.history.len(), MAX_HISTORY);
}
#[test]
fn oversized_strings_are_bounded_and_do_not_enter_the_screen() {
    let mut t = terminal();
    t.process(b"\x1bPq");
    t.process(&vec![b'x'; 100_000]);
    let mut copy = terminal();
    copy.process(&t.restore());
    let end = b"tail\x1b\\OK";
    t.process(end);
    copy.process(end);
    assert_eq!(text(&t), text(&copy));
    assert!(!text(&t).contains('x'));
    assert!(!text(&t).contains("tail"));
    assert!(text(&t).contains("OK"));
}

#[test]
fn cursor_style_saved_positions_and_history_erase_follow_vt_semantics() {
    let mut t = terminal();
    t.process(
        b"first\r\nsecond\r\nthird\r\nfourth\r\nfifth\x1b[2;3H\x1b[31m\x1b7\x1b[1;8H\x1b[2 q",
    );
    assert!(!t.history.is_empty());
    let screen = text(&t);
    t.process(b"\x1b[3J");
    assert!(t.history.is_empty());
    assert_eq!(text(&t), screen);
    let mut copy = terminal();
    copy.process(&t.restore());
    t.process(b"\x1b8SAVED");
    copy.process(b"\x1b8SAVED");
    assert_eq!(text(&t), text(&copy));
    assert_eq!(copy.cursor_style, 2);
    assert!(!copy.limited);
}
#[test]
fn cancelled_csi_is_not_duplicated_on_the_live_stream() {
    let mut t = terminal();
    let bytes = b"\x1b[12;\x1b[31mred";
    assert_eq!(t.process(bytes).bytes, bytes);
    assert!(text(&t).contains("red"));
}

#[test]
fn clipboard_queries_and_writes_are_blocked_before_the_client_stream() {
    let mut t = terminal();
    for byte in b"\x1b]52;c;?\x1b\\\x1b]52;c;" {
        assert!(t.process(&[*byte]).bytes.is_empty());
    }
    assert!(t.process(&vec![b'x'; 100_000]).bytes.is_empty());
    let out = t.process(b"\x07SAFE");
    assert_eq!(out.bytes, b"SAFE");
    assert!(out.replies.is_empty());
}

#[test]
fn clipboard_sequence_variants_never_reach_the_client_stream() {
    let long_csi = [b"\x1b[".as_slice(), &[b'1'; 600], b"\x1b]52;c;?\x07"].concat();
    let long_osc = [b"\x1b]0;".as_slice(), &[b'x'; 600], b"\x1b]52;c;?\x07"].concat();
    let long_number = [b"\x1b]".as_slice(), &[b'0'; 600], b"52;c;?\x07"].concat();
    for seq in [
        b"\x1b]052;c;?\x07".as_slice(),
        b"\x1b]0000052;c;?\x1b\\",
        b"\x1b]5522;type=read\x1b\\",
        "\u{9d}52;c;?\x07".as_bytes(),
        "\x1b]0;x\u{9c}\u{9d}52;c;?\x07".as_bytes(),
        "\x1b[1\u{9d}52;c;?\x07".as_bytes(),
        b"\x1bPtmux;\x1b\x1b]52;c;?\x07\x1b\\",
        b"\x1bP\x1b]52;c;?\x07\x1b\\",
        b"\x1b]0;title\x1b]52;c;?\x07",
        b"\x1b\x1b]52;c;?\x07",
        &long_csi,
        &long_osc,
        &long_number,
    ] {
        let mut whole = terminal();
        let mut split = terminal();
        let mut forwarded = whole.process(seq).bytes;
        let mut pieces = Vec::new();
        for byte in seq {
            pieces.extend(split.process(&[*byte]).bytes);
        }
        forwarded.extend(whole.process(b"SAFE").bytes);
        pieces.extend(split.process(b"SAFE").bytes);
        for out in [&forwarded, &pieces] {
            let text = String::from_utf8_lossy(out);
            let clipboard = text.split("\x1b]").skip(1).any(|osc| {
                let osc = osc.trim_start_matches('0');
                osc.starts_with("52;") || osc.starts_with("5522;")
            });
            assert!(!clipboard, "{seq:?} forwarded {text:?}");
            assert!(
                !text.contains("\x1b\x1b")
                    && !text.chars().any(|c| ('\u{80}'..='\u{9f}').contains(&c)),
                "{seq:?} forwarded {text:?}"
            );
        }
    }
}

#[test]
fn ordinary_strings_and_text_still_reach_the_client_stream() {
    let mut t = terminal();
    let seq = "\x1b]0;tit✜le\x07\x1b]8;;https://x\x1b\\é\x1b]520;x\x07\x1bP$q m\x1b\\";
    assert_eq!(t.process(seq.as_bytes()).bytes, seq.as_bytes());
    let mut t = terminal();
    let mut out = Vec::new();
    for byte in seq.as_bytes() {
        out.extend(t.process(&[*byte]).bytes);
    }
    assert_eq!(out, seq.as_bytes());
}

#[test]
fn attach_during_split_string_terminator_keeps_live_stream_aligned() {
    let mut original = terminal();
    original.process(b"\x1bPq");
    original.process(&vec![b'x'; 1000]);
    original.process(b"\x1b");
    let mut attached = terminal();
    attached.process(&original.restore());
    let out = original.process(b"\\visible");
    attached.process(&out.bytes);
    assert_eq!(text(&original), text(&attached));
    assert!(text(&attached).contains("visible"));
}

#[test]
fn resizing_smaller_still_counts_the_allocated_cell_storage() {
    let mut t = Terminal::new(Size {
        rows: 256,
        cols: 512,
    });
    let old_line = t.screens[0].lines[0].clone();
    t.resize(Size { rows: 256, cols: 1 });
    for _ in 0..1500 {
        t.remember(vec![old_line.clone()]);
    }
    let allocated = t
        .screens
        .iter()
        .flat_map(|s| &s.lines)
        .map(screen::line_bytes)
        .sum::<usize>()
        + t.history_bytes;
    assert!(allocated <= MAX_BYTES);
    assert!(t.truncated);
}
