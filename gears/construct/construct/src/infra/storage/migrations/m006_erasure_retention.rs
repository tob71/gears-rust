use sea_orm_migration::prelude::*;

/// What Erasure and retention need of the tables other features made: on
/// `construct__subject_settings`, the subject's profile generation, which an
/// erasure moves on so that a later profile gets a new root key, when the
/// erasure under way was marked, and when the leader's job last tried it; an
/// index that serves retention's delete of
/// the record identities received before a time; and `construct__tenants`,
/// the tenants Construct has held a row for, which retention keeps sweeping
/// after their rows are gone.
///
/// @cpt-dod:cpt-cf-construct-dod-erasure-storage:p1
#[derive(DeriveMigrationName)]
pub struct Migration;

const SETTINGS: &str = "construct__subject_settings";
const GENERATION: &str = "profile_generation";
const MARKED_AT: &str = "erasure_marked_at";
const ATTEMPTED_AT: &str = "erasure_attempted_at";
const RECORD_IDS: &str = "construct__record_ids";
const RECEIVED: &str = "idx_construct__record_ids__received";
const TENANTS: &str = "construct__tenants";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// Safe to run twice: a column or an index that exists is skipped. A row
    /// stored before takes generation 0, the root key every profile has had.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column(SETTINGS, GENERATION).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(SETTINGS))
                        .add_column(
                            ColumnDef::new(Alias::new(GENERATION))
                                .big_integer()
                                .not_null()
                                .default(0),
                        )
                        .to_owned(),
                )
                .await?;
        }
        for column in [MARKED_AT, ATTEMPTED_AT] {
            if !manager.has_column(SETTINGS, column).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(SETTINGS))
                            .add_column(
                                ColumnDef::new(Alias::new(column))
                                    .timestamp_with_time_zone()
                                    .null(),
                            )
                            .to_owned(),
                    )
                    .await?;
            }
        }
        if !manager.has_index(RECORD_IDS, RECEIVED).await? {
            manager
                .create_index(
                    Index::create()
                        .name(RECEIVED)
                        .table(Alias::new(RECORD_IDS))
                        .col(Alias::new("tenant_id"))
                        .col(Alias::new("received_at"))
                        .to_owned(),
                )
                .await?;
        }
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(TENANTS))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Alias::new("tenant_id"))
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("first_seen"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(TENANTS))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        if manager.has_index(RECORD_IDS, RECEIVED).await? {
            manager
                .drop_index(
                    Index::drop()
                        .name(RECEIVED)
                        .table(Alias::new(RECORD_IDS))
                        .to_owned(),
                )
                .await?;
        }
        for column in [ATTEMPTED_AT, MARKED_AT, GENERATION] {
            if manager.has_column(SETTINGS, column).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(SETTINGS))
                            .drop_column(Alias::new(column))
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::sea_orm::{ConnectionTrait, Database, Statement};
    use sea_orm_migration::{MigrationTrait, MigratorTrait};

    use super::{ATTEMPTED_AT, GENERATION, MARKED_AT, RECEIVED, RECORD_IDS, SETTINGS, TENANTS};
    use crate::infra::storage::migrations::Migrator;

    /// On a real `SQLite` database: a settings row stored before the upgrade
    /// takes generation 0 and no mark, the identity table gets its index, and
    /// running the step again changes nothing.
    #[tokio::test]
    async fn a_row_stored_before_takes_generation_zero_and_a_second_run_is_a_no_op() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite connects");
        Migrator::up(&db, Some(5)).await.expect("up to m005");
        db.execute_unprepared(&format!(
            "INSERT INTO {SETTINGS} (tenant_id, subject_id, personalization_enabled) \
             VALUES ('tenant-a', 'subject-a', 1)"
        ))
        .await
        .expect("a row under m005");

        Migrator::up(&db, None).await.expect("the upgrade");
        let manager = sea_orm_migration::SchemaManager::new(&db);

        let row = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                format!("SELECT {GENERATION}, {MARKED_AT}, {ATTEMPTED_AT} FROM {SETTINGS}"),
            ))
            .await
            .expect("query")
            .expect("the row survives");
        assert_eq!(row.try_get::<i64>("", GENERATION).expect(GENERATION), 0);
        assert_eq!(
            row.try_get::<Option<String>>("", MARKED_AT)
                .expect(MARKED_AT),
            None
        );
        assert_eq!(
            row.try_get::<Option<String>>("", ATTEMPTED_AT)
                .expect(ATTEMPTED_AT),
            None
        );
        assert!(
            manager
                .has_index(RECORD_IDS, RECEIVED)
                .await
                .expect("schema")
        );

        assert!(manager.has_table(TENANTS).await.expect("schema"));

        super::Migration
            .up(&manager)
            .await
            .expect("a second run is a no-op");
    }
}
