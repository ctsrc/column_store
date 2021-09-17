use proc_macro2::{Ident, Span};
use proc_macro::TokenStream;
use proc_macro_error::{proc_macro_error, abort};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Table)]
#[proc_macro_error]
pub fn derive_table (item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let record_struct_ident = input.ident;
    let record_struct_name = record_struct_ident.to_string();
    // XXX: We require the identifier of the proc macro consumer's struct declaration
    //      to end with "TableRecord". We do this for several reasons, mainly namely:
    //      1. We later chop off a portion of the suffix to clearly distinguish between
    //         proc macro consumer provided struct declaration and proc macro output.
    //      2. Requiring the "Table" portion of the suffix to always be present in the
    //         name makes for consistency between proc macro consumer's struct declaration
    //         and our crate documentation.
    //      3. Requiring the "Table" portion of the suffix to always be present in the
    //         name instead of tacking it on to the end inside of our proc macro ensures that
    //         proc macro consumers don't end up with identifiers like "FooBarTableTable",
    //         unless they themselves actually name their records "FooBarTableTableRecord".
    //         Whereas if we asked people to declare names like "FooBarRecord", and we output
    //         names like "FooBarTable", then someone using names like "FooBarTableRecord"
    //         would get output with identifiers like "FooBarTableTable".
    if !record_struct_name.ends_with("TableRecord") {
        abort!(record_struct_ident, r#"Identifier must end with "TableRecord"."#;
            note = format!(r#"Identifier was "{}"."#, record_struct_name);
            help = r#"Change the identifier in question to something ending with "TableRecord"."#)
    }
    // XXX: We do not allow the input ident as a whole to be simply "TableRecord".
    //      The input ident must consist of a non-zero length name even without
    //      the suffix as a whole. This way all tables are required to be given
    //      actual names. (Of course anyone could still name it something "meaningless" like
    //      "MyTableRecord" that still says nothing about what it contains. But that is outside
    //      of our area of concern. Our concern around naming ends at *encouraging* good naming.)
    if record_struct_name == "TableRecord" {
        abort!(record_struct_ident, r#"Identifier must have a non-zero length name leading up to the "TableRecord" suffix."#;
            note = format!(r#"Identifier was "{}"."#, record_struct_name);
            help = r#"Change the identifier in question to have a non-zero length name leading up to the "TableRecord" suffix."#)
    }
    // XXX: We strip the "Record" portion of the "TableRecord" suffix to get the table name.
    let table_name = &record_struct_name[..record_struct_name.len() - "Record".len()];
    let table_ident = Ident::new(table_name, Span::call_site());

    let output = quote! {
        struct #table_ident {
            f: ::column_store::fd_lock::RwLock<::std::fs::File>,
        }
        impl #table_ident {
            pub fn try_new (db_dir: impl AsRef<::std::path::Path>) -> ::std::result::Result<Self, ::column_store::TableError>
            {
                let table_dir = db_dir.as_ref().join(#table_name).clone();
                ::std::fs::create_dir_all(&table_dir)?;
                let f = ::column_store::fd_lock::RwLock::new(
                    ::std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(table_dir.join("rows.cst"))?);
                Ok(Self {
                    f,
                })
            }
            pub fn insert_one (&mut self, record: #record_struct_ident)
            {
            }
        }
    };

    TokenStream::from(output)
}
