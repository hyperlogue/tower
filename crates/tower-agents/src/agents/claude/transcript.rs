use std::{
    io::{self, Read, Seek, SeekFrom},
    path::PathBuf,
};
use tower_protocol::Patch;

#[derive(Clone, Default)]
pub(super) struct Facts {
    pub tokens: Patch<u64>,
    pub model: Patch<String>,
    pub cwd: Patch<String>,
}
pub(super) struct Reader {
    pub path: PathBuf,
    identity: Option<(u64, u64)>,
    offset: u64,
    line: Vec<u8>,
    skipping: bool,
    facts: Facts,
    pub more: bool,
}
impl Reader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            identity: None,
            offset: 0,
            line: Vec::new(),
            skipping: false,
            facts: Facts::default(),
            more: false,
        }
    }
    pub fn read(&mut self, model_after: u64, usage_after: u64) -> io::Result<Facts> {
        self.more = false;
        let mut file = tower_pal::open_regular(&self.path)?;
        let identity = tower_pal::file_identity(&self.path)?;
        let len = file.metadata()?.len();
        if self.identity != Some(identity) || len < self.offset {
            self.offset = 0;
            self.line.clear();
            self.skipping = false;
            self.facts = Facts::default();
            self.identity = Some(identity);
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.take(1024 * 1024).read_to_end(&mut bytes)?;
        for byte in bytes {
            self.offset += 1;
            if byte == b'\n' {
                if !self.skipping {
                    self.fold(model_after, usage_after);
                } else {
                    self.facts.tokens = Patch::Clear;
                }
                self.skipping = false;
                self.line.clear();
            } else if self.line.len() < 256 * 1024 && !self.skipping {
                self.line.push(byte);
            } else {
                self.skipping = true;
                self.line.clear();
            }
        }
        self.more = self.offset < len;
        Ok(self.facts.clone())
    }
    fn fold(&mut self, model_after: u64, usage_after: u64) {
        let Ok(row) = serde_json::from_slice::<serde_json::Value>(&self.line) else {
            return;
        };
        if row["isCompactSummary"] == true {
            self.facts.tokens = Patch::Clear;
            return;
        }
        if row["type"] != "assistant" || row["isApiErrorMessage"] == true {
            return;
        }
        if let Some(cwd) = row["cwd"].as_str().filter(|s| {
            s.len() <= 4096
                && std::path::Path::new(s).is_absolute()
                && !s.chars().any(char::is_control)
        }) {
            self.facts.cwd = Patch::Set(cwd.into());
        }
        let message = &row["message"];
        if self.offset > model_after
            && let Some(model) = message["model"]
                .as_str()
                .filter(|s| !s.is_empty() && *s != "<synthetic>")
        {
            self.facts.model = Patch::Set(
                model
                    .chars()
                    .filter(|c| !c.is_control())
                    .take(128)
                    .collect(),
            );
        }
        if self.offset > usage_after {
            let tokens = [
                "input_tokens",
                "cache_creation_input_tokens",
                "cache_read_input_tokens",
            ]
            .iter()
            .filter_map(|key| message["usage"][key].as_u64())
            .fold(0u64, u64::saturating_add);
            if tokens > 0 {
                self.facts.tokens = Patch::Set(tokens);
            }
        }
    }
    pub fn invalidate(&mut self, model: bool, usage: bool) {
        if model {
            self.facts.model = Patch::Missing;
        }
        if usage {
            self.facts.tokens = Patch::Clear;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn incremental_metadata_handles_partial_records_compaction_and_model_switch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let row = r#"{"type":"assistant","cwd":"/work","message":{"model":"old","usage":{"input_tokens":10,"cache_read_input_tokens":20,"output_tokens":999}}}"#;
        std::fs::write(&path, row).unwrap();
        let mut r = Reader::new(path.clone());
        assert_eq!(r.read(0, 0).unwrap().tokens, Patch::Missing);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(f).unwrap();
        assert_eq!(r.read(0, 0).unwrap().tokens, Patch::Set(30));
        let cut = f.metadata().unwrap().len();
        r.invalidate(true, true);
        writeln!(f, "{{\"type\":\"local-command-stdout\"}}").unwrap();
        let facts = r.read(cut, cut).unwrap();
        assert_eq!(facts.model, Patch::Missing);
        assert_eq!(facts.tokens, Patch::Clear);
        writeln!(f, "{row}").unwrap();
        assert_eq!(r.read(cut, cut).unwrap().model, Patch::Set("old".into()));
        writeln!(f, "{{\"isCompactSummary\":true}}").unwrap();
        assert_eq!(r.read(cut, cut).unwrap().tokens, Patch::Clear);
        std::fs::write(&path, format!("{row}\n")).unwrap();
        assert_eq!(r.read(0, 0).unwrap().tokens, Patch::Set(30));
    }
}
