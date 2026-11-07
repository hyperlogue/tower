use axum::serve::Listener;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Sleep,
};

pub(super) struct Limited<L> {
    pub listener: L,
    pub permits: Arc<Semaphore>,
}
pub(super) struct Connection<I> {
    io: I,
    _permit: OwnedSemaphorePermit,
    deadline: Pin<Box<Sleep>>,
    headers: Option<Vec<u8>>,
}
impl<L: Listener> Listener for Limited<L> {
    type Io = Connection<L::Io>;
    type Addr = L::Addr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("HTTP admission remains open");
        let (io, addr) = self.listener.accept().await;
        (
            Connection {
                io,
                _permit: permit,
                deadline: Box::pin(tokio::time::sleep(Duration::from_secs(10))),
                headers: Some(Vec::new()),
            },
            addr,
        )
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}
impl<I: AsyncRead + Unpin> AsyncRead for Connection<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use std::future::Future;
        let this = self.get_mut();
        if this.headers.is_some() && this.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP headers timed out",
            )));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut this.io).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(())))
            && let Some(headers) = &mut this.headers
        {
            headers.extend(
                buf.filled()[before..]
                    .iter()
                    .take((16 * 1024 + 1usize).saturating_sub(headers.len())),
            );
            if headers.windows(4).any(|s| s == b"\r\n\r\n") {
                this.headers = None;
            } else if headers.len() > 16 * 1024 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP headers exceed limit",
                )));
            }
        }
        result
    }
}
impl<I: AsyncWrite + Unpin> AsyncWrite for Connection<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
