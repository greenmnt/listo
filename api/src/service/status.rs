use sqlx::{MySqlPool, Row};
use tonic::Status;

use crate::error::IntoStatus;
use crate::pb;

use super::conv::{date_str, datetime_str, opt_datetime_str};

pub async fn get(pool: &MySqlPool) -> Result<pb::ServerStatus, Status> {
    let counts = sqlx::query(
        "SELECT
            COUNT(*) AS apps_total,
            SUM(detail_fetched_at IS NOT NULL) AS apps_with_detail,
            SUM(docs_fetched_at IS NOT NULL) AS apps_with_docs
         FROM council_applications",
    )
    .fetch_one(pool)
    .await
    .into_status()?;

    let apps_total: i64 = counts.try_get("apps_total").unwrap_or(0);
    let apps_with_detail: i64 = counts
        .try_get::<Option<i64>, _>("apps_with_detail")
        .ok()
        .flatten()
        .unwrap_or(0);
    let apps_with_docs: i64 = counts
        .try_get::<Option<i64>, _>("apps_with_docs")
        .ok()
        .flatten()
        .unwrap_or(0);

    let docs = sqlx::query(
        "SELECT
            COUNT(*) AS docs_total,
            SUM(file_path IS NOT NULL) AS docs_downloaded,
            COALESCE(SUM(file_size), 0) AS docs_total_bytes
         FROM council_application_documents",
    )
    .fetch_one(pool)
    .await
    .into_status()?;

    let docs_total: i64 = docs.try_get("docs_total").unwrap_or(0);
    let docs_downloaded: i64 = docs
        .try_get::<Option<i64>, _>("docs_downloaded")
        .ok()
        .flatten()
        .unwrap_or(0);
    // SUM() on a non-null column with COALESCE → DECIMAL/NUMERIC under the
    // hood, comes back as Option<i64> when the table is empty.
    let docs_total_bytes: i64 = docs
        .try_get::<Option<i64>, _>("docs_total_bytes")
        .ok()
        .flatten()
        .unwrap_or(0);

    // Latest scrape window — best-effort, table only exists if the scraper
    // has been migrated past 0007. Swallow the error so the bottom bar still
    // renders on a fresh DB.
    let latest_scrape = sqlx::query(
        "SELECT council_slug, backend_name, date_from, date_to, status,
                started_at, finished_at, apps_yielded, files_downloaded
         FROM council_scrape_windows
         ORDER BY started_at DESC
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|r| pb::LatestScrape {
        council_slug: r.get("council_slug"),
        backend_name: r.get("backend_name"),
        date_from: date_str(r.get("date_from")),
        date_to: date_str(r.get("date_to")),
        status: r.get("status"),
        started_at: datetime_str(r.get("started_at")),
        finished_at: opt_datetime_str(r.try_get("finished_at").ok()),
        apps_yielded: r.try_get("apps_yielded").unwrap_or(0),
        files_downloaded: r.try_get("files_downloaded").unwrap_or(0),
    });

    // Latest discounted variable owner-occupier rate, if RBA F5 has been
    // ingested. Falls through to None on a fresh DB.
    let current_var_rate_pct: Option<f64> = sqlx::query_scalar(
        "SELECT CAST(rate_pct AS DOUBLE)
           FROM mortgage_rates
          WHERE series_id IN ('FILRHLBVD','FILRHL3YF','FILRHLBVS')
          ORDER BY month DESC,
                   FIELD(series_id, 'FILRHLBVD','FILRHL3YF','FILRHLBVS')
          LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    Ok(pb::ServerStatus {
        apps_total,
        apps_with_detail,
        apps_with_docs,
        docs_total,
        docs_downloaded,
        docs_total_bytes,
        latest_scrape,
        current_var_rate_pct,
        server_time: datetime_str(chrono::Utc::now().naive_utc()),
    })
}
