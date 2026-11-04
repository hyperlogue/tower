use super::*;

impl ServiceWorker {
    pub(super) async fn vcs_request(
        &self,
        r: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        use crate::vcs::{decode, encode, failure};
        use tower_protocol::*;
        let check = r.clone();
        let actor = self
            .execute(move |s| s.vcs_authorize(&check, peer))
            .await??;
        let vcs = self.vcs();
        let mut permit = if matches!(
            r.method.as_str(),
            "checkout.register" | "checkout.bind" | "vcs.status" | "vcs.prepare" | "vcs.execute"
        ) {
            Some(
                vcs.gate
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| RpcError::application("rate_limited", "VCS workers are busy"))?,
            )
        } else {
            None
        };
        let result = match r.method.as_str() {
            "catalog.get" => self.execute(|s| s.catalog_view()).await.map_err(failure)?,
            "checkout.list" => {
                let p: CheckoutFilter = decode(&r.params)?;
                encode(
                    vcs.store
                        .checkouts(&p)
                        .map_err(failure)?
                        .into_iter()
                        .map(|c| c.record)
                        .collect::<Vec<_>>(),
                )
            }
            "checkout.get" => {
                let p: CheckoutParams = decode(&r.params)?;
                encode(vcs.checkout(&p.checkout_id)?.record)
            }
            "checkout.register" => {
                let p: CheckoutRegister = decode(&r.params)?;
                vcs.register_request(p, actor).await
            }
            "checkout.bind" => {
                let p: CheckoutBind = decode(&r.params)?;
                let mut c = vcs.checkout(&p.checkout_id)?;
                if let Some(repo) = p.repository_id {
                    encode(
                        vcs.observe(crate::vcs::Vcs::binding(&c)?, Some(&repo))
                            .await?,
                    )
                } else {
                    c.record.repository_id = None;
                    c.association_manual = true;
                    encode(vcs.store.save_checkout(c).map_err(failure)?)
                }
            }
            "checkout.forget" => {
                let p: CheckoutParams = decode(&r.params)?;
                self.execute(move |s| s.forget_checkout(&p.checkout_id))
                    .await
                    .map_err(failure)??;
                encode(serde_json::json!({"forgotten": true}))
            }
            "vcs.status" => {
                let p: CheckoutParams = decode(&r.params)?;
                encode(vcs.refresh(&p.checkout_id).await?)
            }
            "vcs.prepare" => {
                let p: VcsPrepare = decode(&r.params)?;
                encode(vcs.prepare(p, actor).await?)
            }
            "vcs.operation" => {
                let p: VcsPlanParams = decode(&r.params)?;
                encode(vcs.operation(&p.plan_id)?.record)
            }
            "vcs.release" => {
                #[derive(serde::Deserialize)]
                struct Release {
                    plan_id: String,
                    #[serde(default)]
                    confirm: bool,
                }
                let p: Release = decode(&r.params)?;
                if !p.confirm {
                    return Ok(Err(RpcError::application(
                        "confirmation_required",
                        "Inspect native Git state and processes, then confirm release of this uncertain operation",
                    )));
                }
                encode(
                    vcs.store
                        .release_vcs_operation(&p.plan_id)
                        .map_err(failure)?,
                )
            }
            "vcs.execute" => {
                let p: VcsPlanParams = decode(&r.params)?;
                let request = r.clone();
                let (op, claimed) = self
                    .execute(move |s| s.vcs_claim(&request, peer, &p.plan_id))
                    .await
                    .map_err(failure)??;
                let record = op.record.clone();
                if claimed {
                    let permit = permit.take().expect("execution permit");
                    tokio::spawn(vcs.clone().execute(
                        self.service.clone(),
                        r.clone(),
                        peer,
                        op,
                        permit,
                    ));
                }
                encode(record)
            }
            _ => Err(RpcError::protocol(
                -32601,
                "method_not_found",
                "Unknown checkout method",
            )),
        };
        Ok(result)
    }
    pub(super) async fn checkout_open(
        &self,
        mut r: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        use tower_protocol::*;
        let mut p: SessionOpenParams = crate::vcs::decode(&r.params)?;
        let mut check = r.clone();
        check.method = "daemon.service_access".into();
        self.execute(move |s| s.dispatch(&check, peer)).await??;
        if let Some(id) = &p.checkout_id {
            let vcs = self.vcs();
            let _permit = vcs
                .gate
                .clone()
                .try_acquire_owned()
                .map_err(|_| RpcError::application("rate_limited", "VCS workers are busy"))?;
            if !vcs.refresh(id).await?.available {
                return Ok(Err(RpcError::application(
                    "checkout_unavailable",
                    "Checkout is not available",
                )));
            }
        }
        let p = self
            .execute(move |s| {
                s.resolve_checkout_launch(&mut p)?;
                Ok::<_, RpcError>(p)
            })
            .await??;
        if p.checkout_id.is_none()
            && let Ok(_permit) = self.vcs().gate.clone().try_acquire_owned()
        {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.vcs().register(&p.cwd, None),
            )
            .await;
        }
        r.params = crate::vcs::encode(&p)?;
        self.execute(move |s| s.dispatch(&r, peer)).await
    }
}
