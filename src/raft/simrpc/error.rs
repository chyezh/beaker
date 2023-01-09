#[cfg(test)]
#[derive(thiserror::Error, Debug)]
pub enum TestError {
    #[error("connection loss")]
    ConnectionLoss,

    #[error("packet loss")]
    PacketLoss,

    #[error("service loss")]
    ServiceLoss,

    #[error("channel loss")]
    ChannelLoss,
}
