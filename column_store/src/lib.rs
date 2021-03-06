pub use column_store_proc_macros::Table;
pub use fd_lock;
pub use ulid::Ulid;

use thiserror::Error;

#[derive(Error, Debug)]
#[error("{0}")]
pub enum TableInitializationError {
    IOError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
#[error("{0}")]
pub enum TransactionManagerInitializationError {
    IOError(#[from] std::io::Error),
    TableInitializationError(#[from] TableInitializationError),
}

#[derive(Error, Debug)]
#[error("{0}")]
pub enum TableRecordInsertError {
    IOError(#[from] std::io::Error),
    #[error("Mutex lock error")]
    TryLockError,
}

impl<T> From<::std::sync::TryLockError<T>> for TableRecordInsertError {
    fn from (_: ::std::sync::TryLockError<T>) -> Self
    {
        Self::TryLockError
    }
}

#[macro_export]
macro_rules! get_first_match {
    ($($column:ident),*;$from:ident;$($where_:expr);*) => {
        {
            Some((23, "Hot pepper sauce!"))
        }
    };
}
