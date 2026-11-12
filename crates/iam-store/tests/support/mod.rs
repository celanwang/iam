use iam_protocol::generate_id128;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::{ConnectOptions, Connection, MySqlConnection};
use url::Url;

pub async fn isolated_database() -> Url {
    assert_eq!(
        std::env::var("IAM_TIDB_ALLOW_TEST_WRITES").as_deref(),
        Ok("1"),
        "set IAM_TIDB_ALLOW_TEST_WRITES=1 for an isolated local test database"
    );
    let mut url =
        Url::parse(&std::env::var("IAM_TIDB_TEST_URL").expect("IAM_TIDB_TEST_URL is required"))
            .expect("invalid IAM_TIDB_TEST_URL");
    assert_eq!(url.scheme(), "mysql", "expected a mysql:// TiDB URL");
    let options = MySqlConnectOptions::from_url(&url).expect("invalid TiDB connection options");
    assert!(
        matches!(
            options.get_host(),
            "127.0.0.1" | "localhost" | "::1" | "[::1]"
        ) && options.get_socket().is_none(),
        "local TCP test endpoint required"
    );
    assert!(
        options
            .get_database()
            .is_some_and(|database| database.starts_with("iam_test")),
        "test database name must start with iam_test"
    );
    let mut connection = MySqlConnection::connect_with(&options.disable_statement_logging())
        .await
        .expect("failed to connect to the local test database");
    let _: String = sqlx::query_scalar("SELECT tidb_version()")
        .fetch_one(&mut connection)
        .await
        .expect("real TiDB required (tidb_version must succeed)");

    // Every case gets its own catalog, including concurrent test runs.
    let database = format!("iam_test_{}", hex::encode(generate_id128()));
    sqlx::query(&format!("CREATE DATABASE `{database}`"))
        .execute(&mut connection)
        .await
        .expect("failed to create an isolated test database");
    url.set_path(&database);
    url
}
