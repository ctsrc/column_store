use convert_case::{Case, Casing};
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
    //      The input ident must consist of a non-zero length name even without the suffix
    //      as a whole. This way all tables are required to be given "actual names". (Of course,
    //      anyone could still name it something "meaningless" like "MyTableRecord" that still says
    //      nothing about what it contains. But that is outside of our area of concern. Our concern
    //      around naming ends at *encouraging* good naming.)
    if record_struct_name == "TableRecord" {
        abort!(record_struct_ident, r#"Identifier must have a non-zero length name leading up to the "TableRecord" suffix."#;
            note = format!(r#"Identifier was "{}"."#, record_struct_name);
            help = r#"Change the identifier in question to have a non-zero length name leading up to the "TableRecord" suffix."#)
    }
    // XXX: We strip the "Record" portion of the "TableRecord" suffix to get the table name.
    let table_name = &record_struct_name[..record_struct_name.len() - "Record".len()];
    let table_ident = Ident::new(table_name, Span::call_site());

    let rows_ident = Ident::new(&format!("{}Rows", table_name), Span::call_site());
    let txn_log_file_lock_ident = Ident::new(&format!("{}TransactionLogFileLock", table_name), Span::call_site());
    let txn_manager_ident = Ident::new(&format!("{}TransactionManager", table_name), Span::call_site());
    let mod_ident = Ident::new(&table_name.to_case(Case::Snake), Span::call_site());

    let output = quote! {
        mod #mod_ident {
            use super::#record_struct_ident;
            #[derive(Debug)]
            struct #rows_ident {
                // TODO: Generate from the item TokenStream
                a: ::std::sync::Arc<::std::sync::Mutex<Vec<u64>>>,
                b: ::std::sync::Arc<::std::sync::Mutex<Vec<u64>>>,
                c: ::std::sync::Arc<::std::sync::Mutex<Vec<u8>>>,
                d: ::std::sync::Arc<::std::sync::Mutex<Vec<String>>>,
            }
            #[derive(Debug)]
            pub(crate) struct #table_ident<'a> {
                records_file_lock_guard: ::column_store::fd_lock::RwLockWriteGuard<'a, ::std::fs::File>,
                // TODO: Indexes from UNIQUE constraints
                rows: #rows_ident,
            }
            impl<'a> #table_ident<'a> {
                // TODO: Dedicated struct [...]RecordsFileLock
                pub fn try_open_records_file (db_dir: impl AsRef<::std::path::Path>) -> std::io::Result<::column_store::fd_lock::RwLock<::std::fs::File>> {
                    let table_dir = db_dir.as_ref().join(#table_name).clone();
                    ::std::fs::create_dir_all(&table_dir)?;
                    let f = ::std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(table_dir.join("rows.cst"))?;
                    Ok(::column_store::fd_lock::RwLock::new(f))
                }
                fn try_new (records_file_lock: &'a mut ::column_store::fd_lock::RwLock<::std::fs::File>) -> Result<Self, ::column_store::TableInitializationError>
                {
                    let records_file_lock_guard = records_file_lock.try_write()?;

                    let rows = {
                        // TODO: Read records from file
                        let a = vec![];
                        let b = vec![];
                        let c = vec![];
                        let d = vec![];

                        #rows_ident{
                            a: ::std::sync::Arc::new(::std::sync::Mutex::new(a)),
                            b: ::std::sync::Arc::new(::std::sync::Mutex::new(b)),
                            c: ::std::sync::Arc::new(::std::sync::Mutex::new(c)),
                            d: ::std::sync::Arc::new(::std::sync::Mutex::new(d)),
                        }
                    };

                    Ok(Self {
                        records_file_lock_guard,
                        rows,
                    })
                }
                fn try_insert_one (&mut self, record: #record_struct_ident) -> Result<(), ::column_store::TableRecordInsertError>
                {
                    let mut a_guard = self.rows.a.try_lock()?;
                    let mut b_guard = self.rows.b.try_lock()?;
                    let mut c_guard = self.rows.c.try_lock()?;
                    let mut d_guard = self.rows.d.try_lock()?;

                    // TODO: Checkpoint Vec len and use for rollback

                    a_guard.push(record.a);
                    b_guard.push(record.b);
                    c_guard.push(record.c);
                    d_guard.push(record.d);

                    // TODO: Write to file
                    // TODO: Rollback if write fails

                    Ok(())
                }
                fn try_insert_many (&mut self, records: &[#record_struct_ident]) -> Result<(), ::column_store::TableRecordInsertError>
                {
                    let a_guard = self.rows.a.try_lock()?;
                    let b_guard = self.rows.b.try_lock()?;
                    let c_guard = self.rows.c.try_lock()?;
                    let d_guard = self.rows.d.try_lock()?;

                    // TODO: Checkpoint Vec len and use for rollback

                    // TODO: Push the fields of each record to Vec's and write records to file
                    // TODO: Rollback all records inserted if write fails

                    Ok(())
                }
            }
            #[derive(Debug)]
            /// The purpose of the transaction manager is to write transaction log entries for inserts, updates and deletes.
            /// The transaction log is intended to allow for fine-grained auditing, recovery, debugging, performance tuning, etc.
            pub(crate) struct #txn_manager_ident<'a> {
                txn_log_file_lock_guard: ::column_store::fd_lock::RwLockWriteGuard<'a, ::std::fs::File>,
                table: #table_ident<'a>,
            }
            impl<'a> #txn_manager_ident<'a> {
                // TODO: Dedicated struct [...]TransactionLogFileLock
                pub fn try_open_txn_log_file (db_dir: impl AsRef<::std::path::Path>) -> std::io::Result<::column_store::fd_lock::RwLock<::std::fs::File>> {
                    let table_dir = db_dir.as_ref().join(#table_name).clone();
                    ::std::fs::create_dir_all(&table_dir)?;
                    let f = ::std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(table_dir.join("txn_log"))?;
                    Ok(::column_store::fd_lock::RwLock::new(f))
                }
                pub fn try_new (
                    txn_log_file_lock: &'a mut ::column_store::fd_lock::RwLock<::std::fs::File>,
                    records_file_lock: &'a mut ::column_store::fd_lock::RwLock<::std::fs::File>
                )
                    -> Result<Self, ::column_store::TransactionManagerInitializationError>
                {
                    let txn_log_file_lock_guard = txn_log_file_lock.try_write()?;
                    let table = #table_ident::try_new(records_file_lock)?;

                    Ok(Self {
                        txn_log_file_lock_guard,
                        table,
                    })
                }
                pub fn try_insert_one (&mut self, record: #record_struct_ident) -> Result<(), ::column_store::TableRecordInsertError>
                {
                    // TODO: Transaction ID

                    let txn_result = self.table.try_insert_one(record);

                    // TODO: Transaction log entry

                    Ok(())
                }
                pub fn try_insert_many (&mut self, records: &[#record_struct_ident]) -> Result<(), ::column_store::TableRecordInsertError>
                {
                    // TODO: Transaction ID

                    let txn_result = self.table.try_insert_many(records);

                    // TODO: Transaction log entry

                    Ok(())
                }
            }
        }
    };

    TokenStream::from(output)
}
