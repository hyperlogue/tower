use std::{io, path::PathBuf};
use tower_pool_proto::{Size, valid_term};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Profile {
    pub term: String,
    pub size: Size,
}
impl Default for Profile {
    fn default() -> Self {
        Self {
            term: "xterm-256color".into(),
            size: Size::default(),
        }
    }
}
pub struct Preferences {
    path: PathBuf,
    profile: Profile,
}
impl Preferences {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let profile: Profile = match tower_pal::read_private(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Profile::default(),
            Err(e) => return Err(e),
        };
        if !valid_term(&profile.term) || profile.size.rows == 0 || profile.size.cols == 0 {
            return Err(io::Error::other("invalid saved terminal preferences"));
        }
        profile.size.bounded()?;
        Ok(Self { path, profile })
    }
    pub fn current(&self) -> Profile {
        self.profile.clone()
    }
    pub fn remember(&mut self, term: String, size: Size) -> io::Result<()> {
        let profile = Profile { term, size };
        tower_pal::write_private(&self.path, &serde_json::to_vec(&profile)?)?;
        self.profile = profile;
        Ok(())
    }
}
