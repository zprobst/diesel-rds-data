mod connection;
mod query_builder;
mod transaction_manager;
mod types;

use diesel::{
    backend::{Backend, TypeMetadata},
    query_builder::bind_collector::RawBytesBindCollector,
};

use query_builder::RdsQueryBuilder;
use types::RdsTypes;

use rusoto_rds_data::Field;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Rds;

impl Backend for Rds {
    type QueryBuilder = RdsQueryBuilder;
    type BindCollector = RawBytesBindCollector<Rds>;
    type RawValue = Field;
    type ByteOrder = byteorder::NetworkEndian;
}

impl TypeMetadata for Rds {
    type TypeMetadata = RdsTypes;
    type MetadataLookup = ();
}
