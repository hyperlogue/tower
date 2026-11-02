use std::{io, path::Path, time::Duration};
use tower_pal::Environment;

pub async fn project_environment(
    cwd: &Path,
    exe: &Path,
    mut env: Environment,
) -> io::Result<Environment> {
    let cwd = std::fs::canonicalize(cwd)?;
    let has_envrc = cwd.ancestors().any(|d| d.join(".envrc").is_file());
    if has_envrc {
        let (code, bytes) = tower_pal::capture(
            &["direnv".into(), "status".into(), "--json".into()],
            &env,
            &cwd,
            Duration::from_secs(3),
            65536,
        )
        .await?;
        let status: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| io::Error::other("cannot verify direnv approval"))?;
        if code != 0
            || status
                .pointer("/state/foundRC/allowed")
                .and_then(|v| v.as_u64())
                != Some(0)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "direnv approval required; review the project and run direnv allow",
            ));
        }
    }
    if has_envrc || env.contains_key("DIRENV_DIR") {
        let (code, bytes) = tower_pal::capture(
            &[
                "direnv".into(),
                "exec".into(),
                cwd.to_string_lossy().into_owned(),
                exe.to_string_lossy().into_owned(),
                "__agent".into(),
                "env".into(),
            ],
            &env,
            &cwd,
            Duration::from_secs(30),
            1024 * 1024,
        )
        .await?;
        if code != 0 {
            return Err(io::Error::other("project environment failed to load"));
        }
        env = serde_json::from_slice(&bytes)
            .map_err(|_| io::Error::other("invalid project environment"))?;
    }
    Ok(env)
}
