//! Conversion helpers from internal error sources into `tonic::Status`.

use tonic::{Code, Status};

pub trait IntoStatus<T> {
    fn into_status(self) -> Result<T, Status>;
}

/// `sqlx::Error` → `tonic::Status::internal`. Logs the underlying error
/// because we never want to leak DB internals over the wire.
impl<T> IntoStatus<T> for Result<T, sqlx::Error> {
    fn into_status(self) -> Result<T, Status> {
        self.map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("database error")
        })
    }
}

pub fn bad_request(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg.into())
}
