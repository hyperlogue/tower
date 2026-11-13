//! The tags a user may attach to a session.

pub const MAX: usize = 64;
pub const INVALID: &str = "Tags must be distinct additions/removals, at most 64 nonempty values of 64 characters without control characters or surrounding whitespace";
pub const TOO_MANY: &str = "A session may have at most 64 tags";

/// Applies a patch to `tags`, or says why it cannot be applied.
pub fn patch(
    tags: &mut std::collections::BTreeSet<String>,
    add: &[String],
    remove: &[String],
) -> Result<(), &'static str> {
    let valid = |tags: &[String]| {
        tags.len() <= MAX
            && tags.iter().all(|tag| {
                !tag.is_empty()
                    && tag.chars().count() <= 64
                    && tag.trim() == tag
                    && !tag.chars().any(char::is_control)
            })
    };
    if !valid(add) || !valid(remove) || add.iter().any(|tag| remove.contains(tag)) {
        return Err(INVALID);
    }
    let mut patched = tags.clone();
    for tag in remove {
        patched.remove(tag);
    }
    patched.extend(add.iter().cloned());
    if patched.len() > MAX {
        return Err(TOO_MANY);
    }
    *tags = patched;
    Ok(())
}
