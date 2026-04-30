use chrono::NaiveDate;
use sqlx::{MySqlPool, Row};
use tonic::Status;

use crate::classify::{kind_sql_filter, DaKind};
use crate::error::{bad_request, IntoStatus};
use crate::pb;

use super::conv::date_str;

pub async fn suburbs(
    pool: &MySqlPool,
    req: pb::SuburbStatsRequest,
) -> Result<pb::SuburbStatsResponse, Status> {
    let limit = req.limit.unwrap_or(40).clamp(1, 200);
    let kind_filter = req.kind.as_deref().and_then(DaKind::parse);
    let kind_sql = kind_filter.map(kind_sql_filter).unwrap_or("1=1");

    let sql = format!(
        "SELECT
            UPPER(suburb) AS suburb,
            COUNT(*) AS n_total,
            SUM(CASE WHEN {filter} THEN 1 ELSE 0 END) AS n_kind,
            SUM(CASE WHEN lodged_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND ({filter}) THEN 1 ELSE 0 END) AS n_last_30d,
            SUM(CASE WHEN lodged_date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
                      AND lodged_date <  DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                      AND ({filter}) THEN 1 ELSE 0 END) AS n_prev_30d
         FROM council_applications
         WHERE suburb IS NOT NULL AND suburb <> ''
         GROUP BY UPPER(suburb)
         HAVING n_kind > 0
         ORDER BY n_kind DESC, n_last_30d DESC
         LIMIT ?",
        filter = kind_sql,
    );

    let rows = sqlx::query(&sql)
        .bind(limit)
        .fetch_all(pool)
        .await
        .into_status()?;

    let items = rows
        .into_iter()
        .map(|r| pb::SuburbStat {
            suburb: r.get("suburb"),
            n_total: r.try_get::<i64, _>("n_total").unwrap_or(0),
            n_kind: r
                .try_get::<Option<i64>, _>("n_kind")
                .ok()
                .flatten()
                .unwrap_or(0),
            n_last_30d: r
                .try_get::<Option<i64>, _>("n_last_30d")
                .ok()
                .flatten()
                .unwrap_or(0),
            n_prev_30d: r
                .try_get::<Option<i64>, _>("n_prev_30d")
                .ok()
                .flatten()
                .unwrap_or(0),
        })
        .collect();

    Ok(pb::SuburbStatsResponse { items })
}

pub async fn trends(
    pool: &MySqlPool,
    req: pb::TrendStatsRequest,
) -> Result<pb::TrendStatsResponse, Status> {
    let mut sql = String::from(
        "SELECT
            DATE_FORMAT(lodged_date, '%Y-%m-01') AS bucket_start,
            SUM(CASE WHEN ",
    );
    sql.push_str(kind_sql_filter(DaKind::Granny));
    sql.push_str(" THEN 1 ELSE 0 END) AS n_granny, SUM(CASE WHEN ");
    sql.push_str(kind_sql_filter(DaKind::Duplex));
    sql.push_str(" THEN 1 ELSE 0 END) AS n_duplex, SUM(CASE WHEN ");
    sql.push_str(kind_sql_filter(DaKind::BigDev));
    sql.push_str(" THEN 1 ELSE 0 END) AS n_big_dev, SUM(CASE WHEN ");
    sql.push_str(kind_sql_filter(DaKind::Other));
    sql.push_str(
        " THEN 1 ELSE 0 END) AS n_other,
           SUM(CASE WHEN decision_outcome IN ('Approved','Approved with conditions') THEN 1 ELSE 0 END) AS n_approved,
           COUNT(*) AS n_total
         FROM council_applications
         WHERE lodged_date IS NOT NULL",
    );

    let mut binds: Vec<String> = Vec::new();
    if let Some(s) = &req.suburb {
        sql.push_str(" AND UPPER(suburb) = ?");
        binds.push(s.to_uppercase());
    }
    if let Some(s) = &req.date_from {
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request("date_from must be YYYY-MM-DD"))?;
        sql.push_str(" AND lodged_date >= ?");
        binds.push(d.to_string());
    }
    if let Some(s) = &req.date_to {
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request("date_to must be YYYY-MM-DD"))?;
        sql.push_str(" AND lodged_date <= ?");
        binds.push(d.to_string());
    }
    sql.push_str(" GROUP BY bucket_start ORDER BY bucket_start ASC");

    let mut query = sqlx::query(&sql);
    for b in &binds {
        query = query.bind(b);
    }
    let rows = query.fetch_all(pool).await.into_status()?;

    let buckets = rows
        .into_iter()
        .filter_map(|r| {
            let bucket_str: String = r.try_get("bucket_start").ok()?;
            let bucket_date = NaiveDate::parse_from_str(&bucket_str, "%Y-%m-%d").ok()?;
            Some(pb::TrendBucket {
                bucket_start: date_str(bucket_date),
                n_granny: r
                    .try_get::<Option<i64>, _>("n_granny")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                n_duplex: r
                    .try_get::<Option<i64>, _>("n_duplex")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                n_big_dev: r
                    .try_get::<Option<i64>, _>("n_big_dev")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                n_other: r
                    .try_get::<Option<i64>, _>("n_other")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                n_approved: r
                    .try_get::<Option<i64>, _>("n_approved")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                n_total: r.try_get::<i64, _>("n_total").unwrap_or(0),
            })
        })
        .collect();

    Ok(pb::TrendStatsResponse { buckets })
}
