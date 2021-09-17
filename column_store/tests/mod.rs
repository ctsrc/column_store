use tempfile::tempdir;
use derive_new::new;

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
    let mut example_table = Example1Table::try_new(db_dir.path()).unwrap();
    example_table.insert_one(Example1TableRecord::new(13, 37, 42, "Hello World!".into()));
    example_table.insert_one(Example1TableRecord::new(23, 23, 90, "Hot pepper sauce!".into()));

    let (a1, d1) = get_first_match!(a, d; example_table; a > 20).unwrap();
    assert_eq!(a1, 23);
    assert_eq!(d1, "Hot pepper sauce!");
}

#[test]
fn test_get_first_match_2 () {
    let db_dir = tempdir().unwrap();
    let mut example_table = Example1Table::try_new(db_dir.path()).unwrap();
    example_table.insert_one(Example1TableRecord::new(13, 37, 42, "Hello World!".into()));
    example_table.insert_one(Example1TableRecord::new(23, 23, 90, "Hot pepper sauce!".into()));

    let (c2, d2) = get_first_match!(c, d; example_table; b > 20).unwrap();
    assert_eq!(c2, 42);
    assert_eq!(d2, "Hello World!");
}
