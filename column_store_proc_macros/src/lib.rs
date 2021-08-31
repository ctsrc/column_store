use proc_macro::TokenStream;

#[proc_macro_derive(Table)]
pub fn derive_table (_item: TokenStream) -> TokenStream {
    "struct ExampleTable {\
    }\
    impl ExampleTable {\
        pub fn new (table_dir: impl AsRef<::std::path::Path>) -> Self\
        {\
            Self {\
            }\
        }\
        pub fn insert_one (&mut self, a: u64, b: u64, c: u8, d: String)\
        {\
        }\
    }".parse().unwrap()
}
