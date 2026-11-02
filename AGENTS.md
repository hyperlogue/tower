# tower

Keep design documents, plans, review notes, task commentary, and generated
artifacts in the gitignored `workspace/` directory. Existing documentation is
in `workspace/docs/`; use `workspace/<topic>/` for other working material.

Do not add documentation folders or standalone project commentary to the
tracked repository. Do not force-add workspace contents. Only
`workspace/.gitignore` is tracked so new checkouts inherit the ignore rules.

Keep the root README focused on setup and usage, and repository instructions
in `AGENTS.md`. Source comments should explain only non-obvious code constraints;
put design explanations and work history in `workspace/`. Preserve licenses
and required attribution notices.

## Commit practice

- Work on `main` directly. Use small self-contained commits with a Conventional
  Commit subsystem scope, imperative subject at most 72 characters, no final period.
- Stage only your own files by explicit path. Commit after checks pass. Explain
  motivation in a wrapped body when useful. Leave `git push` to the user.
- Preserve existing authorship metadata; do not attribute work to an unrelated
  agent or add personal/machine identifiers, credentials, or home-directory paths
  to source, fixtures, docs, logs, scripts, or commit messages.
