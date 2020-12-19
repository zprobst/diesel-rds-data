use std::cell::RefCell;

use diesel::{
    backend::Backend,
    connection::{Connection, SimpleConnection},
    query_builder::{AsQuery, QueryBuilder, QueryFragment, QueryId},
    query_source::QueryableByName,
    result::Error,
    row::NamedRow,
    types::HasSqlType,
    ConnectionResult, QueryResult, Queryable,
};
use rusoto_rds_data::{
    BeginTransactionRequest, ColumnMetadata, CommitTransactionRequest, ExecuteStatementRequest,
    ExecuteStatementResponse, Field, RdsData, RdsDataClient, RollbackTransactionRequest,
};

use crate::{query_builder::RdsQueryBuilder, transaction_manager::RdsTransactionManager, Rds};

pub struct RdsConnection {
    rds: RdsDataClient,
    transaction_manager: RdsTransactionManager,
    transaction_id: RefCell<Option<String>>,
    runtime: tokio::runtime::Runtime,
    database: Option<String>,
    resource_arn: String,
    schema: Option<String>,
    secret_arn: String,
}

impl RdsConnection {
    pub fn begin_transaction(&self) -> QueryResult<()> {
        let request = BeginTransactionRequest {
            database: self.database.clone(),
            resource_arn: self.resource_arn.clone(),
            schema: self.schema.clone(),
            secret_arn: self.secret_arn.clone(),
        };
        let transaction_id = self
            .runtime
            .block_on(async { self.rds.begin_transaction(request).await })
            .map_err(|_| Error::AlreadyInTransaction)?
            .transaction_id;
        self.transaction_id.replace(transaction_id);
        Ok(())
    }

    pub fn rollback_transaction(&self) -> QueryResult<()> {
        let request = RollbackTransactionRequest {
            secret_arn: self.secret_arn.clone(),
            resource_arn: self.resource_arn.clone(),
            transaction_id: self
                .transaction_id
                .clone()
                .into_inner()
                .expect("should be in transaction"),
        };
        self.runtime
            .block_on(async { self.rds.rollback_transaction(request).await })
            .map_err(|_| Error::RollbackTransaction)?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> QueryResult<()> {
        let request = CommitTransactionRequest {
            secret_arn: self.secret_arn.clone(),
            resource_arn: self.resource_arn.clone(),
            transaction_id: self
                .transaction_id
                .clone()
                .into_inner()
                .expect("should be in transaction"),
        };
        self.runtime
            .block_on(async { self.rds.commit_transaction(request).await })
            .map_err(|_| Error::RollbackTransaction)?;
        Ok(())
    }

    fn prepare_query<T>(&self, source: &T) -> QueryResult<ExecuteStatementRequest>
    where
        T: QueryFragment<Rds> + QueryId,
    {
        let database = self.database.clone();
        let resource_arn = self.resource_arn.clone();
        let schema = self.schema.clone();
        let secret_arn = self.secret_arn.clone();
        let include_result_metadata = Some(true);
        let transaction_id = self.transaction_id.clone().into_inner();
        let continue_after_timeout = Some(true);
        let result_set_options = None;

        let mut qb = RdsQueryBuilder::new();
        source.to_sql(&mut qb)?;
        let sql = qb.finish();

        let mut bind_collector = <Rds as Backend>::BindCollector::new();
        source.collect_binds(&mut bind_collector, &())?;
        let meta = bind_collector.metadata;
        let binds = bind_collector.binds;
        let paramters = binds
            .into_iter()
            .zip(meta.into_iter())
            .map(|(b, m)| m.read_bytes(b.unwrap_or_default()))
            .collect();

        let result = ExecuteStatementRequest {
            sql,
            continue_after_timeout,
            transaction_id,
            include_result_metadata,
            result_set_options,
            secret_arn,
            schema,
            resource_arn,
            database,
            parameters: Some(paramters),
        };

        Ok(result)
    }

    fn execute_inner(
        &self,
        input: ExecuteStatementRequest,
    ) -> QueryResult<ExecuteStatementResponse> {
        // TODO: Report the error correctly here.
        self.runtime
            .block_on(async { self.rds.execute_statement(input).await })
            .map_err(|_| Error::NotFound)
    }
}

impl SimpleConnection for RdsConnection {
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        self.execute(query).map(|_| ())
    }
}

impl Connection for RdsConnection {
    type Backend = Rds;
    type TransactionManager = RdsTransactionManager;

    fn establish(_: &str) -> ConnectionResult<Self> {
        todo!()
    }

    fn execute(&self, sql: &str) -> QueryResult<usize> {
        let query = ExecuteStatementRequest {
            sql: sql.to_owned(),
            transaction_id: self.transaction_id.clone().into_inner(),
            secret_arn: self.secret_arn.clone(),
            schema: self.schema.clone(),
            resource_arn: self.resource_arn.clone(),
            database: self.database.clone(),
            include_result_metadata: Some(true),
            ..Default::default()
        };
        self.execute_inner(query)
            .map(|r| r.number_of_records_updated.unwrap_or_default() as usize)
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Rds> + QueryId,
        Rds: HasSqlType<T::SqlType>,
        U: Queryable<T::SqlType, Rds>,
    {
        todo!()
    }

    fn query_by_name<T, U>(&self, source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Rds> + QueryId,
        U: QueryableByName<Rds>,
    {
        let query = self.prepare_query(source)?;
        let response = self.execute_inner(query)?;
        match response.records {
            None => Ok(Vec::with_capacity(0)),
            Some(records) => {
                let meta = &response.column_metadata.expect("column_metadata");
                let mut results = Vec::with_capacity(records.len());
                for record in records {
                    let row = (meta, &record);
                    results.push(U::build(&row).map_err(|e| Error::DeserializationError(e))?);
                }
                Ok(results)
            }
        }
    }

    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Rds> + QueryId,
    {
        let query = self.prepare_query(source)?;
        self.execute_inner(query)
            .map(|r| r.number_of_records_updated.unwrap_or_default() as usize)
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self.transaction_manager
    }
}

// In order for us to re-assemble the query into a rust type
// using `QueryableByName`, we must include the column metadata that
// can be returned by the api. This is not on the field itself, but rather
// on the response. So, we must include both as part of this diesel `NamedRow`
// concept. This implements the trait to wrap the ordered lists of these to
// data types.
impl NamedRow<Rds> for (&Vec<ColumnMetadata>, &Vec<Field>) {
    fn index_of(&self, column_name: &str) -> Option<usize> {
        self.0.iter().position(|m| match &m.name {
            Some(n) => n == column_name,
            None => false,
        })
    }
    fn get_raw_value(&self, index: usize) -> Option<&Field> {
        self.1.get(index)
    }
}
