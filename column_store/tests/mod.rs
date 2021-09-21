use derive_new::new;
use tempfile::tempdir;

use column_store::get_first_match;
use column_store::Table;

#[derive(Table, new)]
struct Example1TableRecord {
    a: u64,
    b: u64,
    c: u8,
    d: String,
}

#[test]
fn test_get_first_match_1 () {
    let db_dir = tempdir().unwrap();
    let mut records_l = Example1TableRecordsFileLock::try_new(db_dir.path()).unwrap();
    let mut txn_log_l = Example1TableTransactionLogFileLock::try_new(db_dir.path()).unwrap();
    let mut example_table_txn_mgr = Example1TableTransactionManager::try_new(&mut txn_log_l, &mut records_l).unwrap();
    example_table_txn_mgr.try_insert_one(Example1TableRecord::new(13, 37, 42, "Hello World!".into())).unwrap();
    example_table_txn_mgr.try_insert_one(Example1TableRecord::new(23, 23, 90, "Hot pepper sauce!".into())).unwrap();

    let (a1, d1) = get_first_match!(a, d; example_table_txn_mgr; a > 20).unwrap();
    assert_eq!(a1, 23);
    assert_eq!(d1, "Hot pepper sauce!");
}

#[test]
fn test_get_first_match_2 () {
    let db_dir = tempdir().unwrap();
    let mut records_l = Example1TableRecordsFileLock::try_new(db_dir.path()).unwrap();
    let mut txn_log_l = Example1TableTransactionLogFileLock::try_new(db_dir.path()).unwrap();
    let mut example_table_txn_mgr = Example1TableTransactionManager::try_new(&mut txn_log_l, &mut records_l).unwrap();
    example_table_txn_mgr.try_insert_one(Example1TableRecord::new(13, 37, 42, "Hello World!".into())).unwrap();
    example_table_txn_mgr.try_insert_one(Example1TableRecord::new(23, 23, 90, "Hot pepper sauce!".into())).unwrap();

    let (c2, d2) = get_first_match!(c, d; example_table_txn_mgr; b > 20).unwrap();
    assert_eq!(c2, 42);
    assert_eq!(d2, "Hello World!");
}

#[test]
fn test_lock_exclusive_twice () {
    let db_dir = tempdir().unwrap();

    let mut records_l = Example1TableRecordsFileLock::try_new(db_dir.path()).unwrap();
    let mut txn_log_l = Example1TableTransactionLogFileLock::try_new(db_dir.path()).unwrap();
    let mut example_table_txn_mgr = Example1TableTransactionManager::try_new(&mut txn_log_l, &mut records_l).unwrap();

    let mut records_l = Example1TableRecordsFileLock::try_new(db_dir.path()).unwrap();
    let mut txn_log_l = Example1TableTransactionLogFileLock::try_new(db_dir.path()).unwrap();
    Example1TableTransactionManager::try_new(&mut txn_log_l, &mut records_l)
        .expect_err("Was able to open already-locked table file");

    example_table_txn_mgr.try_insert_one(Example1TableRecord::new(13, 37, 42, "Hello World!".into())).unwrap();
}
