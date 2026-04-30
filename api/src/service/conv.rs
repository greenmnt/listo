//! Tiny conversion helpers shared across RPC implementations.
//! - DaKind → proto enum
//! - chrono types → ISO 8601 strings (the schema uses strings to dodge the
//!   prost-types dependency on google.protobuf.Timestamp).

use chrono::{NaiveDate, NaiveDateTime};

use crate::classify;
use crate::pb;

pub fn kind_to_proto(k: classify::DaKind) -> i32 {
    match k {
        classify::DaKind::Granny => pb::DaKind::Granny as i32,
        classify::DaKind::Duplex => pb::DaKind::Duplex as i32,
        classify::DaKind::BigDev => pb::DaKind::BigDev as i32,
        classify::DaKind::Other => pb::DaKind::Other as i32,
    }
}

#[inline]
pub fn date_str(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

#[inline]
pub fn datetime_str(d: NaiveDateTime) -> String {
    // ISO 8601 with seconds + Z suffix. We treat NaiveDateTime as UTC because
    // every DateTime stored by the scraper is already UTC.
    d.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[inline]
pub fn opt_date_str(d: Option<NaiveDate>) -> Option<String> {
    d.map(date_str)
}

#[inline]
pub fn opt_datetime_str(d: Option<NaiveDateTime>) -> Option<String> {
    d.map(datetime_str)
}
