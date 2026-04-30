use sqlx::{MySqlPool, Row};
use tonic::Status;

use crate::error::IntoStatus;
use crate::pb;

use super::conv::opt_date_str;

pub async fn current(pool: &MySqlPool) -> Result<pb::CurrentRates, Status> {
    let row = sqlx::query(
        // mortgage_rates.rate_pct is DECIMAL — CAST to DOUBLE so sqlx can
        // decode it into f64 without pulling in the bigdecimal feature.
        "SELECT
            MAX(month) AS as_of_month,
            CAST(MAX(CASE WHEN series_id='FILRHLBVD'  THEN rate_pct END) AS DOUBLE) AS var_oo,
            CAST(MAX(CASE WHEN series_id='FILRHLBVDI' THEN rate_pct END) AS DOUBLE) AS var_inv,
            CAST(MAX(CASE WHEN series_id='FILRHL3YF'  THEN rate_pct END) AS DOUBLE) AS fix_oo,
            CAST(MAX(CASE WHEN series_id='FILRHL3YFI' THEN rate_pct END) AS DOUBLE) AS fix_inv
         FROM mortgage_rates
         WHERE month = (SELECT MAX(month) FROM mortgage_rates WHERE series_id='FILRHLBVD')",
    )
    .fetch_one(pool)
    .await
    .into_status()?;

    Ok(pb::CurrentRates {
        variable_oo_pct: row.try_get("var_oo").ok(),
        variable_inv_pct: row.try_get("var_inv").ok(),
        fixed_3yr_oo_pct: row.try_get("fix_oo").ok(),
        fixed_3yr_inv_pct: row.try_get("fix_inv").ok(),
        as_of_month: opt_date_str(row.try_get("as_of_month").ok()),
    })
}
