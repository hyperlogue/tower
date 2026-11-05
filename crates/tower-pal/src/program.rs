use std::{
    io,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Program {
    Server,
    Cli,
    TerminalPool,
}
impl Program {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Server => "tower",
            Self::Cli => "tower-cli",
            Self::TerminalPool => "tower-terminal-pool",
        }
    }
    pub fn beside(self, executable: &Path) -> PathBuf {
        executable.with_file_name(self.name())
    }
    // Resolve companions from the running package, never from an agent's PATH.
    pub fn path(self) -> io::Result<PathBuf> {
        Ok(self.beside(&crate::executable()?))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_role_resolves_to_the_same_package_directory() {
        let root = Path::new("/opt/tower package/bin");
        for current in [Program::Cli, Program::Server, Program::TerminalPool] {
            for target in [Program::Cli, Program::Server, Program::TerminalPool] {
                assert_eq!(
                    target.beside(&root.join(current.name())),
                    root.join(target.name())
                );
            }
        }
    }
}
