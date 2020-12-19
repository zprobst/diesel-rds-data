use crate::Rds;

use diesel::{
    deserialize::{self, FromSql},
    sql_types::*,
};

use rusoto_rds_data::{Field, SqlParameter};

pub enum RdsTypes {
    // Array,
    Blob,
    Boolean,
    Date,
    Time,
    TimeStamp,
    Double,
    Null,
    Long,
    String,
}

impl RdsTypes {
    pub fn read_bytes(&self, _: Vec<u8>) -> SqlParameter {
        todo!()
    }
}

impl HasSqlType<Bool> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Boolean
    }
}

impl HasSqlType<SmallInt> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Long
    }
}

impl HasSqlType<Integer> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Long
    }
}

impl HasSqlType<BigInt> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Long
    }
}

impl HasSqlType<Float> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Double
    }
}

impl HasSqlType<Double> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Double
    }
}

impl HasSqlType<Text> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::String
    }
}

impl HasSqlType<Binary> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Blob
    }
}

impl HasSqlType<Date> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Date
    }
}

impl HasSqlType<Time> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::Time
    }
}

impl HasSqlType<Timestamp> for Rds {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        Self::TypeMetadata::TimeStamp
    }
}

impl FromSql<SmallInt, Rds> for i16 {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").long_value.unwrap_or_default() as i16)
    }
}

impl FromSql<Integer, Rds> for i32 {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").long_value.unwrap_or_default() as i32)
    }
}

impl FromSql<BigInt, Rds> for i64 {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").long_value.unwrap_or_default())
    }
}

impl FromSql<Float, Rds> for f32 {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").double_value.unwrap_or_default() as f32)
    }
}

impl FromSql<Double, Rds> for f64 {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").double_value.unwrap_or_default())
    }
}

impl FromSql<Bool, Rds> for bool {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        Ok(val.expect("field").boolean_value.unwrap_or_default())
    }
}

impl FromSql<Text, Rds> for String {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        let val = val
            .expect("field")
            .string_value
            .as_ref()
            .expect("converting to string value")
            .to_owned();
        Ok(val)
    }
}

impl FromSql<Binary, Rds> for Vec<u8> {
    fn from_sql(val: Option<&Field>) -> deserialize::Result<Self> {
        let val = val
            .expect("field")
            .blob_value
            .as_ref()
            .expect("converting to string value")
            .to_vec();
        Ok(val)
    }
}

// PG types and chrono types, uuid.
