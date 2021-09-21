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

    let pk_ident = Ident::new(&format!("{}PrimaryKey", table_name), Span::call_site());
    let rows_ident = Ident::new(&format!("{}Rows", table_name), Span::call_site());
    let records_file_lock_ident = Ident::new(&format!("{}RecordsFileLock", table_name), Span::call_site());
    let txn_log_file_lock_ident = Ident::new(&format!("{}TransactionLogFileLock", table_name), Span::call_site());
    let txn_manager_ident = Ident::new(&format!("{}TransactionManager", table_name), Span::call_site());
    let mod_ident = Ident::new(&table_name.to_case(Case::Snake), Span::call_site());

    let output = quote! {
        mod #mod_ident {
            use super::#record_struct_ident;

            // TODO: Eventually we might want to let crate users define their own primary keys,
            //       including allowing the crate users defined value type, and having primary keys
            //       span multiple columns. For now primary keys are internally generated ULIDs.
            #[derive(Copy, Clone, Debug)]
            struct #pk_ident {
                value: ::column_store::Ulid,
            }
            impl #pk_ident {
                fn new () -> Self {
                    Self {
                        value: ::column_store::Ulid::new(),
                    }
                }
            }

            #[derive(Debug)]
            struct #rows_ident {
                // TODO: Generate from the item TokenStream
                // TODO: Disallow names "pk" and "pks", in order to avoid users confusing
                //       our defined pks and their own defined fields. Since the real pk column
                //       of the table is internally managed, and externally meaningful, we
                //       don't want to allow anyone to refer to any of their own columns as
                //       "pk" or "pks". Maybe even go so far as to disallow those
                //       as prefix and/or suffix of any of their columns too.
                a: ::std::sync::Arc<::std::sync::Mutex<Vec<u64>>>,
                b: ::std::sync::Arc<::std::sync::Mutex<Vec<u64>>>,
                c: ::std::sync::Arc<::std::sync::Mutex<Vec<u8>>>,
                d: ::std::sync::Arc<::std::sync::Mutex<Vec<String>>>,
            }

            pub(crate) struct #records_file_lock_ident {
                rw_lock: ::column_store::fd_lock::RwLock<::std::fs::File>,
            }
            impl #records_file_lock_ident {
                pub fn try_new (db_dir: impl AsRef<::std::path::Path>) -> std::io::Result<Self> {
                    let table_dir = db_dir.as_ref().join(#table_name).clone();
                    ::std::fs::create_dir_all(&table_dir)?;
                    let f = ::std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(table_dir.join("records.csr"))?;
                    Ok(Self {
                        rw_lock: ::column_store::fd_lock::RwLock::new(f)
                    })
                }
            }

            #[derive(Debug)]
            struct #table_ident<'a> {
                records_file_lock_guard: ::column_store::fd_lock::RwLockWriteGuard<'a, ::std::fs::File>,
                // TODO: Indexes from UNIQUE constraints
                rows: #rows_ident,
                pks: ::std::sync::Arc<::std::sync::Mutex<Vec<#pk_ident>>>,
            }
            impl<'a> #table_ident<'a> {
                fn try_new (records_file_lock: &'a mut #records_file_lock_ident) -> Result<Self, ::column_store::TableInitializationError>
                {
                    let records_file_lock_guard = records_file_lock.rw_lock.try_write()?;

                    let pks = vec![];

                    let rows = {
                        // TODO: Read (pk, tombstone, record) tuples from file
                        let a_values = vec![];
                        let b_values = vec![];
                        let c_values = vec![];
                        let d_values = vec![];

                        #rows_ident {
                            a: ::std::sync::Arc::new(::std::sync::Mutex::new(a_values)),
                            b: ::std::sync::Arc::new(::std::sync::Mutex::new(b_values)),
                            c: ::std::sync::Arc::new(::std::sync::Mutex::new(c_values)),
                            d: ::std::sync::Arc::new(::std::sync::Mutex::new(d_values)),
                        }
                    };

                    let pks = ::std::sync::Arc::new(::std::sync::Mutex::new(pks));

                    Ok(Self {
                        records_file_lock_guard,
                        rows,
                        pks,
                    })
                }
                fn try_insert_one (&mut self, record: #record_struct_ident) -> Result<#pk_ident, ::column_store::TableRecordInsertError>
                {
                    let mut pks_guard = self.pks.try_lock()?;
                    let mut a_values_guard = self.rows.a.try_lock()?;
                    let mut b_values_guard = self.rows.b.try_lock()?;
                    let mut c_values_guard = self.rows.c.try_lock()?;
                    let mut d_values_guard = self.rows.d.try_lock()?;

                    // TODO: Enforce UNIQUE constraints.
                    //       I.e. scan indexed columns for existence of values we are trying to insert.
                    //       I.e. perform lookup in BTreeMap or HashMap that we are using.

                    // TODO: Checkpoint Vec len and use for rollback

                    let pk = #pk_ident::new();

                    pks_guard.push(pk);
                    a_values_guard.push(record.a);
                    b_values_guard.push(record.b);
                    c_values_guard.push(record.c);
                    d_values_guard.push(record.d);

                    // TODO: Write (pk, tombstone, record) tuple to file
                    // TODO: Rollback if write fails

                    // TODO: Return PK of inserted record.
                    Ok(pk)
                }
                fn try_insert_many (&mut self, records: &[#record_struct_ident]) -> Result<Vec<#pk_ident>, ::column_store::TableRecordInsertError>
                {
                    let mut pks_guard = self.pks.try_lock()?;
                    let a_values_guard = self.rows.a.try_lock()?;
                    let b_values_guard = self.rows.b.try_lock()?;
                    let c_values_guard = self.rows.c.try_lock()?;
                    let d_values_guard = self.rows.d.try_lock()?;

                    // TODO: Enforce UNIQUE constraints.
                    //       I.e. scan indexed columns for existence of values we are trying to insert.
                    //       I.e. perform lookup in BTreeMap or HashMap that we are using.

                    // TODO: Checkpoint Vec len and use for rollback

                    let pks = vec![];
                    // TODO: Generate pk values

                    // TODO: Push the fields of each record to Vec's and
                    //       write (pk, tombstone, record) tuples to file
                    // TODO: Rollback all records inserted if write fails

                    Ok(pks)
                }
            }

            pub(crate) struct #txn_log_file_lock_ident {
                rw_lock: ::column_store::fd_lock::RwLock<::std::fs::File>,
            }
            impl #txn_log_file_lock_ident {
                pub fn try_new (db_dir: impl AsRef<::std::path::Path>) -> std::io::Result<Self> {
                    let table_dir = db_dir.as_ref().join(#table_name).clone();
                    ::std::fs::create_dir_all(&table_dir)?;
                    let f = ::std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(table_dir.join("transaction_log.cstl"))?;
                    Ok(Self {
                        rw_lock: ::column_store::fd_lock::RwLock::new(f)
                    })
                }
            }

            /// The purpose of the transaction manager is to write transaction log entries for inserts, updates and deletes.
            /// The transaction log is intended to allow for fine-grained auditing, recovery, debugging, performance tuning, etc.
            #[derive(Debug)]
            pub(crate) struct #txn_manager_ident<'a> {
                txn_log_file_lock_guard: ::column_store::fd_lock::RwLockWriteGuard<'a, ::std::fs::File>,
                table: #table_ident<'a>,
            }
            impl<'a> #txn_manager_ident<'a> {
                pub fn try_new (
                    txn_log_file_lock: &'a mut #txn_log_file_lock_ident,
                    records_file_lock: &'a mut #records_file_lock_ident
                )
                    -> Result<Self, ::column_store::TransactionManagerInitializationError>
                {
                    let txn_log_file_lock_guard = txn_log_file_lock.rw_lock.try_write()?;
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

                    // TODO: Transaction log entry. When txn_result is Ok(...), include pk in log entry.

                    // TODO MAYBE: Further return pk
                    Ok(())
                }
                pub fn try_insert_many (&mut self, records: &[#record_struct_ident]) -> Result<(), ::column_store::TableRecordInsertError>
                {
                    // TODO: Transaction ID

                    let txn_result = self.table.try_insert_many(records);

                    // TODO: Transaction log entry. When txn_result is Ok(...), include pks in log entry.

                    // TODO MAYBE: Further return pks
                    Ok(())
                }
            }
        }
        use #mod_ident::{#records_file_lock_ident, #txn_log_file_lock_ident, #txn_manager_ident};
    };

    TokenStream::from(output)
}
