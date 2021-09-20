pub use column_store_proc_macros::Table;
pub use fd_lock;

use thiserror::Error;

#[derive(Error, Debug)]
#[error("{0}")]
pub enum TableError {
    IOError(#[from] std::io::Error),
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
