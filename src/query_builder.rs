use diesel::{query_builder::QueryBuilder, QueryResult};

use crate::Rds;

#[derive(Debug, Default)]
pub struct RdsQueryBuilder {
    sql: String,
    bind_idx: u32,
}

impl RdsQueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl QueryBuilder<Rds> for RdsQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> QueryResult<()> {
        self.push_sql("\"");
        self.push_sql(&identifier.replace('"', "\"\""));
        self.push_sql("\"");
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        let sql = format!("${}", self.bind_idx);
        self.push_sql(&sql);
    }

    fn finish(self) -> String {
        self.sql
    }
}
