#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("serde")]
    Serde {
        #[from]
        source: serde_json::Error,
    },
}
