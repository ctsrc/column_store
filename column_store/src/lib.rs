pub use column_store_proc_macros::Table;
pub use fd_lock;

use thiserror::Error;

#[derive(Error, Debug)]
#[error("{0}")]
pub enum TableError {
    IOError(#[from] std::io::Error),
}

#[macro_export]
macro_rules! get_first_match {
    ($($column:ident),*;$from:ident;$($where_:expr);*) => {
        {
            Some((23, "Hot pepper sauce!"))
        }
    };
}
