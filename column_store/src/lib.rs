pub use column_store_proc_macros::Table;

#[macro_export]
macro_rules! get_first_match {
    ($($column:ident),*;$from:ident;$($where_:expr);*) => {
        {
            Some((23, "Hot pepper sauce!"))
        }
    };
}
