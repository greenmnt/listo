use sqlx::{MySqlPool, Row};
use tonic::Status;

use crate::classify::{classify, kind_sql_filter, DaKind};
use crate::error::IntoStatus;
use crate::geo::dummy_latlng;
use crate::pb;

use super::conv::kind_to_proto;

pub async fn points(
    pool: &MySqlPool,
    req: pb::MapRequest,
) -> Result<pb::MapResponse, Status> {
    let limit = req.limit.unwrap_or(2000).clamp(1, 5000);

    let mut sql = String::from(
        "SELECT council_slug, application_id, suburb, status, decision_outcome,
                approved_units, raw_address, description, type_code
           FROM council_applications
          WHERE 1=1",
    );
    let mut binds: Vec<String> = Vec::new();

    let kind_filter = req.kind.as_deref().and_then(DaKind::parse);
    if let Some(k) = kind_filter {
        sql.push_str(" AND ");
        sql.push_str(kind_sql_filter(k));
    } else {
        // No specific kind requested — only show interesting rows on the map
        // (granny + duplex + big-dev) so the UI isn't overwhelmed by OPW/OPV.
        sql.push_str(" AND (");
        sql.push_str(kind_sql_filter(DaKind::Granny));
        sql.push_str(" OR ");
        sql.push_str(kind_sql_filter(DaKind::Duplex));
        sql.push_str(" OR ");
        sql.push_str(kind_sql_filter(DaKind::BigDev));
        sql.push_str(")");
    }
    if let Some(s) = &req.suburb {
        sql.push_str(" AND UPPER(suburb) = ?");
        binds.push(s.to_uppercase());
    }
    sql.push_str(" ORDER BY lodged_date DESC, id DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for b in &binds {
        query = query.bind(b);
    }
    let rows = query.bind(limit).fetch_all(pool).await.into_status()?;

    let points = rows
        .into_iter()
        .map(|r| {
            let application_id: String = r.get("application_id");
            let suburb: Option<String> = r.try_get("suburb").ok();
            let (lat, lng) = dummy_latlng(&application_id, suburb.as_deref());
            let description: Option<String> = r.try_get("description").ok();
            let type_code: Option<String> = r.try_get("type_code").ok();
            let approved_units: Option<i32> = r.try_get("approved_units").ok();
            let kind = classify(description.as_deref(), type_code.as_deref(), approved_units);
            pb::MapPoint {
                application_id,
                council_slug: r.get("council_slug"),
                kind: kind_to_proto(kind),
                lat,
                lng,
                suburb,
                status: r.try_get("status").ok(),
                decision_outcome: r.try_get("decision_outcome").ok(),
                approved_units,
                raw_address: r.try_get("raw_address").ok(),
            }
        })
        .collect();

    Ok(pb::MapResponse { points })
}
