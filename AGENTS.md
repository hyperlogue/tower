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
