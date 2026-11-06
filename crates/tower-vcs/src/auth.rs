use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    #[default]
    Host,
    SshAgent,
}

pub struct Authentication {
    socket: Option<(PathBuf, (u64, u64))>,
}
impl Authentication {
    pub fn host() -> Self {
        Self { socket: None }
    }
    pub fn ssh_agent(path: &Path) -> Result<Self> {
        if !path.is_absolute() || path.as_os_str().len() > 4096 {
            return Err(unavailable());
        }
        let (path, identity) =
            tower_pal::private_socket_identity(path).map_err(|_| unavailable())?;
        Ok(Self {
            socket: Some((path, identity)),
        })
    }
    pub fn mode(&self) -> AuthMode {
        if self.socket.is_some() {
            AuthMode::SshAgent
        } else {
            AuthMode::Host
        }
    }
    pub fn validate(&self) -> Result<()> {
        if let Some((path, identity)) = &self.socket {
            let (current, current_identity) =
                tower_pal::private_socket_identity(path).map_err(|_| unavailable())?;
            if &current != path || current_identity != *identity {
                return Err(unavailable());
            }
        }
        Ok(())
    }
    pub(crate) fn apply(&self, env: &mut tower_pal::Environment) -> Result<()> {
        self.validate()?;
        if let Some((path, _)) = &self.socket {
            env.insert(
                "SSH_AUTH_SOCK".into(),
                path.to_str().ok_or_else(unavailable)?.into(),
            );
        }
        Ok(())
    }
}
fn unavailable() -> Error {
    Error::new("auth_unavailable", false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn agent_socket_identity_cannot_follow_replacement_or_non_socket_paths() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("agent");
        let first = std::os::unix::net::UnixListener::bind(&path).unwrap();
        tower_pal::private_socket(&path).unwrap();
        let auth = Authentication::ssh_agent(&path).unwrap();
        auth.validate().unwrap();
        assert!(Authentication::ssh_agent(Path::new("relative-agent")).is_err());
        assert!(Authentication::ssh_agent(root.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        let second = std::os::unix::net::UnixListener::bind(&path).unwrap();
        tower_pal::private_socket(&path).unwrap();
        assert_eq!(auth.validate().unwrap_err().code, "auth_unavailable");
        Authentication::ssh_agent(&path)
            .unwrap()
            .validate()
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "not a socket").unwrap();
        assert!(Authentication::ssh_agent(&path).is_err());
        drop((first, second));
    }
}
