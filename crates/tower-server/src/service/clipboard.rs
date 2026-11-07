use super::*;
use tower_clipboard::Session;

#[derive(Clone, Deserialize)]
pub(crate) struct ClipboardOperation {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub generation: String,
    pub lease: Option<Secret>,
    pub format: Option<ImageFormat>,
    #[serde(default)]
    pub paste_id: String,
    #[serde(default)]
    pub success: bool,
}
impl Service {
    pub(crate) fn clipboard_authorize_provider(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<(), RpcError> {
        let actor = self.actor(&request.context, peer)?;
        require(&actor, "admin")
    }
    pub(crate) fn clipboard_target(&self, id: &str) -> std::result::Result<Session, RpcError> {
        let record = self.resolve(id)?.record;
        if record.ownership != "owned" {
            return Err(app(
                "not_owned",
                "Clipboard access requires a managed session",
            ));
        }
        if record.status == "exited" {
            return Err(app("clipboard_unavailable", "Session has exited"));
        }
        let session = self.store.managed_session(&record.id).map_err(storage)?;
        let target = self
            .backends
            .select(Some(&session.protocol))?
            .clipboard(&session)?
            .ok_or_else(|| app("unsupported", "This backend has no clipboard input"))?;
        Ok(Session {
            id: record.id,
            generation: target.generation,
        })
    }
    pub(super) fn clipboard_attach_info(&self, p: &Value) -> std::result::Result<Value, RpcError> {
        #[derive(Deserialize)]
        struct Attach {
            id: String,
            client_id: Option<String>,
            client_token: Option<Secret>,
        }
        let p: Attach = params(p)?;
        let mut info: AttachInfo = params(&self.terminal_attach_info(&p.id)?)?;
        match (p.client_id, p.client_token) {
            (Some(client_id), Some(client_token)) => {
                info.clipboard = Some(self.clipboard.ticket(
                    self.clipboard_target(&p.id)?,
                    &ClipboardClient {
                        client_id,
                        client_token,
                    },
                )?);
            }
            (None, None) => {}
            _ => {
                return Err(invalid(
                    "Client ID and credential must be supplied together",
                ));
            }
        }
        value(info)
    }
    pub(crate) fn clipboard_prepare(
        &self,
        r: &Request,
        peer: Peer,
    ) -> std::result::Result<(ClipboardOperation, Session), RpcError> {
        let p: ClipboardOperation = params(&r.params)?;
        let session = if let Some(lease) = &p.lease {
            let session = self.clipboard.session_for_lease(lease)?;
            if self.clipboard_target(&session.id)? != session {
                return Err(app(
                    "clipboard_unavailable",
                    "Attachment generation changed",
                ));
            }
            session
        } else {
            let actor = self.actor(&r.context, peer)?;
            let target = self.clipboard_target(&p.id)?;
            match actor {
                Actor::Agent {
                    record,
                    strong: true,
                } if record.id == target.id => {}
                _ => require(&actor, "admin")?,
            }
            if p.generation != target.generation && r.method != "clipboard.status" {
                return Err(app("clipboard_unavailable", "Session generation changed"));
            }
            target
        };
        Ok((p, session))
    }
    pub(super) fn maintain_clipboard(&self) -> std::result::Result<(), RpcError> {
        let mut sessions = Vec::new();
        for session in self.store.managed_sessions().map_err(storage)? {
            if self
                .store
                .agent(&session.id)
                .map_err(storage)?
                .record
                .status
                == "exited"
            {
                continue;
            }
            if let Some(backend) = self.backends.get(&session.protocol)
                && let Some(target) = backend.clipboard(&session)?
            {
                sessions.push(Session {
                    id: session.id,
                    generation: target.generation,
                });
            }
        }
        self.clipboard.retain(&sessions)
    }
}
