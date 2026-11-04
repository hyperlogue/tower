use crate::{Result, client, error, internal};
use serde_json::json;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    time::Duration,
};
use tower_protocol::{CallerContext, ClipboardImage, ImageFormat, Secret};

pub fn invoked_as(program: &str) -> bool {
    matches!(
        Path::new(program).file_name().and_then(OsStr::to_str),
        Some("xclip" | "wl-paste" | "clipboard-paste")
    )
}
enum Operation {
    Targets,
    Read(ImageFormat),
    Path,
    Delegate,
}
fn operation(program: &str, args: &[String]) -> Operation {
    let argv: Vec<_> = args.iter().map(String::as_str).collect();
    if program == "clipboard-paste" && argv.is_empty() {
        return Operation::Path;
    }
    let mime = match (program, argv.as_slice()) {
        ("xclip", ["-selection", "clipboard", "-t", mime, "-o"])
        | ("xclip", ["-selection", "clipboard", "-o", "-t", mime])
        | ("xclip", ["-o", "-selection", "clipboard", "-t", mime]) => Some(*mime),
        ("wl-paste", ["--type", mime]) | ("wl-paste", ["-t", mime]) => Some(*mime),
        ("wl-paste", ["--list-types"]) => return Operation::Targets,
        _ => None,
    };
    match mime {
        Some("TARGETS") => Operation::Targets,
        Some("image/png") => Operation::Read(ImageFormat::Png),
        Some("image/bmp") => Operation::Read(ImageFormat::Bmp),
        _ => Operation::Delegate,
    }
}
pub fn install(
    root: &Path,
    executable: &Path,
    env: &mut tower_pal::Environment,
    generation: &str,
) -> std::io::Result<()> {
    let directory = root.join("clipboard-bin");
    tower_pal::private_dir(&directory)?;
    for name in ["xclip", "wl-paste", "clipboard-paste"] {
        let path = directory.join(name);
        match std::fs::symlink_metadata(&path) {
            Ok(meta)
                if meta.file_type().is_symlink() && std::fs::read_link(&path)? == executable => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tower_pal::symlink(executable, &path)?
            }
            _ => return Err(std::io::Error::other("Unexpected clipboard shim entry")),
        }
    }
    env.insert("TOWER_CLIPBOARD_GENERATION".into(), generation.into());
    env.insert(
        "TOWER_CLIPBOARD_SHIMS".into(),
        directory.to_string_lossy().into_owned(),
    );
    env.insert(
        "PATH".into(),
        format!(
            "{}:{}",
            directory.display(),
            env.get("PATH").map(String::as_str).unwrap_or("")
        ),
    );
    Ok(())
}
fn helper_context() -> Result<(String, String, PathBuf, CallerContext)> {
    let get = |key| {
        std::env::var(key).map_err(|_| {
            error(
                "clipboard_unavailable",
                "This process has no managed clipboard identity",
            )
        })
    };
    Ok((
        get("TOWER_AGENT_ID")?,
        get("TOWER_CLIPBOARD_GENERATION")?,
        PathBuf::from(get("TOWER_SOCKET")?),
        CallerContext {
            agent_token: Some(Secret(get("TOWER_AGENT_TOKEN")?)),
            ..Default::default()
        },
    ))
}
pub async fn read_image(format: ImageFormat) -> Result<ClipboardImage> {
    let (id, generation, socket, context) = helper_context()?;
    client::call(
        &socket,
        "clipboard.read",
        json!({"id":id,"generation":generation,"format":format}),
        &context,
    )
    .await
}
pub async fn consumed(image: &ClipboardImage) -> Result<()> {
    let (id, generation, socket, context) = helper_context()?;
    client::call(
        &socket,
        "clipboard.consumed",
        json!({"id":id,"generation":generation,"paste_id":image.paste_id}),
        &context,
    )
    .await
}
fn staged_bytes(image: &ClipboardImage) -> Result<Vec<u8>> {
    if image.bytes > tower_protocol::MAX_CLIPBOARD_BYTES as u64 {
        return Err(internal("invalid staged image size"));
    }
    let data = tower_pal::read_private_bounded(
        Path::new(&image.path),
        tower_protocol::MAX_CLIPBOARD_BYTES,
    )
    .map_err(internal)?;
    if data.len() as u64 != image.bytes {
        return Err(internal("invalid staged image size"));
    }
    Ok(data)
}

pub async fn run(program: &str, args: Vec<String>) -> Result<i32> {
    let program = Path::new(program)
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| internal("program name"))?;
    match operation(program, &args) {
        Operation::Delegate => {
            let mut env = tower_pal::environment();
            if env.contains_key("TOWER_CLIPBOARD_DELEGATED") {
                return Ok(1);
            }
            env.insert("TOWER_CLIPBOARD_DELEGATED".into(), "1".into());
            let own = env.get("TOWER_CLIPBOARD_SHIMS").map(PathBuf::from);
            let executable = tower_pal::executable().map_err(internal)?;
            let path = std::env::split_paths(env.get("PATH").map(String::as_str).unwrap_or(""))
                .filter(|dir| {
                    if own.as_ref() == Some(dir) {
                        return false;
                    }
                    !std::fs::canonicalize(dir.join(program)).is_ok_and(|p| p == executable)
                })
                .collect::<Vec<_>>();
            env.insert(
                "PATH".into(),
                std::env::join_paths(path)
                    .map_err(internal)?
                    .to_string_lossy()
                    .into_owned(),
            );
            let mut argv = vec![program.into()];
            argv.extend(args);
            let cwd = tower_pal::cwd().map_err(internal)?;
            let mut child = tower_pal::spawn_inherited(&argv, &env, &cwd).map_err(internal)?;
            Ok(child.wait().await.map_err(internal)?.code().unwrap_or(1))
        }
        Operation::Targets => {
            println!("image/png\nimage/bmp");
            Ok(0)
        }
        operation => {
            let format = match operation {
                Operation::Read(f) => f,
                _ => ImageFormat::Png,
            };
            let image = read_image(format).await?;
            if matches!(operation, Operation::Path) {
                println!("{}", image.path);
            } else {
                use tokio::io::AsyncWriteExt;
                let data = staged_bytes(&image)?;
                tokio::time::timeout(Duration::from_secs(30), async {
                    let mut out = tokio::io::stdout();
                    out.write_all(&data).await?;
                    out.flush().await
                })
                .await
                .map_err(|_| internal("output timeout"))?
                .map_err(internal)?;
            }
            consumed(&image).await?;
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_image_cannot_exceed_clipboard_limit_or_declared_size() {
        let root = tempfile::tempdir().unwrap();
        tower_pal::private_dir(root.path()).unwrap();
        let path = root.path().join("image.png");
        let file = tower_pal::private_file(&path, true).unwrap();
        file.set_len(tower_protocol::MAX_CLIPBOARD_BYTES as u64 + 1)
            .unwrap();
        let mut image = ClipboardImage {
            paste_id: "test".into(),
            path: path.to_string_lossy().into_owned(),
            format: ImageFormat::Png,
            bytes: tower_protocol::MAX_CLIPBOARD_BYTES as u64,
        };
        assert_eq!(staged_bytes(&image).unwrap_err().data.code, "clipboard_io");
        image.bytes += 1;
        assert_eq!(staged_bytes(&image).unwrap_err().data.code, "clipboard_io");
        file.set_len(1).unwrap();
        image.bytes = 2;
        assert!(staged_bytes(&image).is_err());
    }
}
