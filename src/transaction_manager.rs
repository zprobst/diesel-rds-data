use std::cell::Cell;

use diesel::{
    connection::{SimpleConnection, TransactionManager},
    QueryResult,
};

use crate::connection::RdsConnection;

pub struct RdsTransactionManager {
    transaction_depth: Cell<i32>,
}

impl RdsTransactionManager {
    fn change_transaction_depth(&self, by: i32, query: QueryResult<()>) -> QueryResult<()> {
        if query.is_ok() {
            self.transaction_depth
                .set(self.transaction_depth.get() + by)
        }
        query
    }
}

impl TransactionManager<RdsConnection> for RdsTransactionManager {
    fn begin_transaction(&self, conn: &RdsConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        self.change_transaction_depth(
            1,
            if transaction_depth == 0 {
                conn.begin_transaction()
            } else {
                conn.batch_execute(&format!("SAVEPOINT diesel_savepoint_{}", transaction_depth))
            },
        )
    }

    fn rollback_transaction(&self, conn: &RdsConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        self.change_transaction_depth(
            -1,
            if transaction_depth == 1 {
                conn.rollback_transaction()
            } else {
                conn.batch_execute(&format!(
                    "ROLLBACK TO SAVEPOINT diesel_savepoint_{}",
                    transaction_depth - 1
                ))
            },
        )
    }

    fn commit_transaction(&self, conn: &RdsConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        if transaction_depth <= 1 {
            conn.commit_transaction()
        } else {
            self.change_transaction_depth(
                -1,
                conn.batch_execute(&format!(
                    "RELEASE SAVEPOINT diesel_savepoint_{}",
                    transaction_depth - 1
                )),
            )
        }
    }

    fn get_transaction_depth(&self) -> u32 {
        return self.transaction_depth.get() as u32;
    }
}
