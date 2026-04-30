use sqlx::MySqlPool;
use tonic::Status;

use crate::error::{bad_request, IntoStatus};
use crate::pb;

pub async fn calc(
    pool: &MySqlPool,
    req: pb::CalcProfitabilityRequest,
) -> Result<pb::ProfitOutputs, Status> {
    if req.purchase_price <= 0.0 || req.sale_price <= 0.0 || req.hold_months <= 0.0 {
        return Err(bad_request(
            "purchase_price, sale_price and hold_months must all be > 0",
        ));
    }

    // Resolve the rate. Caller-supplied wins; otherwise pull the most recent
    // owner-occupier discounted variable rate from RBA F5; fall back to 6%
    // if the table is empty.
    let rate_pct: f64 = match req.rate_pct {
        Some(r) => r,
        None => sqlx::query_scalar(
            // CAST to DOUBLE because mortgage_rates.rate_pct is DECIMAL(p,s)
            // and sqlx doesn't decode DECIMAL into f64 without the
            // bigdecimal/rust_decimal features (which we deliberately avoid
            // — they roughly double the cold compile time).
            "SELECT CAST(rate_pct AS DOUBLE)
               FROM mortgage_rates
              WHERE series_id = 'FILRHLBVD'
              ORDER BY month DESC LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .into_status()?
        .unwrap_or(6.0),
    };

    let acq_pct = req.acquisition_costs_pct.unwrap_or(5.5);
    let sale_pct = req.sale_costs_pct.unwrap_or(2.5);

    let acquisition_cost = req.purchase_price * (acq_pct / 100.0);
    let principal_for_interest = req.purchase_price + req.build_cost + acquisition_cost;
    let years = req.hold_months / 12.0;
    let interest_cost = principal_for_interest * (rate_pct / 100.0) * years;
    let sale_cost = req.sale_price * (sale_pct / 100.0);
    let total_cost =
        req.purchase_price + req.build_cost + acquisition_cost + interest_cost + sale_cost;
    let profit = req.sale_price - total_cost;
    let margin_pct = profit / total_cost * 100.0;
    let annualised_return_pct = if years > 0.0 {
        (profit / total_cost) / years * 100.0
    } else {
        0.0
    };
    let breakeven_sale_price = total_cost - sale_cost + (total_cost * (sale_pct / 100.0));

    let (verdict, verdict_reason) = if profit < 0.0 || margin_pct < 5.0 {
        (
            pb::Verdict::Grasshopper,
            format!(
                "🦗 Tight: ${:.0} profit on ${:.0} costs ({:.1}% margin). Walk away.",
                profit, total_cost, margin_pct
            ),
        )
    } else if margin_pct >= 18.0 && annualised_return_pct >= 15.0 {
        (
            pb::Verdict::Bull,
            format!(
                "🐂 Strong: ${:.0} profit, {:.1}% margin, {:.1}%/yr annualised.",
                profit, margin_pct, annualised_return_pct
            ),
        )
    } else {
        (
            pb::Verdict::Marginal,
            format!(
                "🤔 Marginal: ${:.0} profit, {:.1}% margin. Stress-test rate + sale.",
                profit, margin_pct
            ),
        )
    };

    Ok(pb::ProfitOutputs {
        rate_pct_used: rate_pct,
        acquisition_cost,
        interest_cost,
        sale_cost,
        total_cost,
        profit,
        margin_pct,
        annualised_return_pct,
        breakeven_sale_price,
        verdict: verdict as i32,
        verdict_reason,
    })
}
