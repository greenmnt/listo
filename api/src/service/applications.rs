use chrono::NaiveDate;
use sqlx::{mysql::MySqlRow, MySqlPool, Row};
use tonic::Status;

use crate::classify::{classify, kind_sql_filter, DaKind};
use crate::error::{bad_request, IntoStatus};
use crate::pb;

use super::conv::{datetime_str, kind_to_proto, opt_date_str, opt_datetime_str};

/// SQL fragment that derives a dwelling count without depending on the LLM.
/// Priority: council-reported approved_units → description regex → NULL.
/// Mirrors the kind regexes in `classify.rs`.
const DWELLING_COUNT_DERIVED: &str = "
    CASE
        WHEN ca.approved_units IS NOT NULL THEN ca.approved_units
        WHEN ca.description REGEXP '(?i)dual[[:space:]]+occupancy|duplex' THEN 2
        WHEN ca.description REGEXP '(?i)triplex' THEN 3
        WHEN ca.description REGEXP '(?i)fourplex|quadruplex' THEN 4
        WHEN ca.description REGEXP '(?i)secondary[[:space:]]+dwelling|granny[[:space:]]+flat|auxiliary[[:space:]]+dwelling|ancillary[[:space:]]+dwelling' THEN 1
        ELSE NULL
    END
";

/// SQL fragment for derived dwelling kind classification (matches classify.rs).
const DWELLING_KIND_DERIVED: &str = "
    CASE
        WHEN ca.description REGEXP '(?i)secondary[[:space:]]+dwelling|granny[[:space:]]+flat|auxiliary[[:space:]]+dwelling|ancillary[[:space:]]+dwelling' THEN 'granny'
        WHEN ca.approved_units >= 3 OR ca.description REGEXP '(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling|townhouse' THEN 'big_dev'
        WHEN ca.description REGEXP '(?i)dual[[:space:]]+occupancy|duplex' THEN 'duplex'
        ELSE NULL
    END
";

pub async fn list(
    pool: &MySqlPool,
    req: pb::ListApplicationsRequest,
) -> Result<pb::ListApplicationsResponse, Status> {
    let limit = req.limit.unwrap_or(50).clamp(1, 500);
    let offset = req.offset.unwrap_or(0);

    // Two LEFT JOIN aggregates replace the old LEFT JOIN da_summaries:
    //   `ents`     — applicant / builder / architect / owner / agent rolled up
    //                from application_entities ⋈ companies (regex-harvested,
    //                no LLM in the loop).
    //   `docs_agg` — counts and total bytes from council_application_documents
    //                bucketed by doc_kind (migration 0021).
    // The street_address used in every property-prefix-LIKE join is now
    // `ca.street_address`, populated by the council scraper at ingest via
    // split_council_address(raw_address) — 99.9% covered for cogc.
    // dwelling_count and dwelling_kind are derived via the SQL fragments
    // above instead of read from da_summaries.
    let join_kind = if req.analyzed_only.unwrap_or(false) { "INNER" } else { "LEFT" };
    let mut sql = format!(
        "SELECT ca.id, ca.council_slug, ca.application_id, ca.type_code, ca.application_type, ca.status,
                ca.decision_outcome, ca.lodged_date, ca.decision_date, ca.approved_units,
                ca.raw_address, ca.suburb, ca.postcode, ca.description, ca.application_url,
                ents.applicant_name        AS llm_applicant_name,
                ents.applicant_acn         AS llm_applicant_acn,
                ents.applicant_entity_type AS llm_applicant_entity_type,
                ents.applicant_agent_name  AS llm_applicant_agent_name,
                -- developer_project_count: distinct DAs where this applicant
                -- company appears as the applicant. Uses application_entities
                -- (regex-harvested) instead of da_summaries.applicant_company_id.
                (SELECT COUNT(DISTINCT ae2.application_id)
                   FROM application_entities ae2
                  WHERE ae2.role = 'applicant'
                    AND ae2.company_id IS NOT NULL
                    AND ae2.company_id = ents.applicant_company_id) AS llm_developer_project_count,
                ents.builder_name          AS llm_builder_name,
                ents.architect_name        AS llm_architect_name,
                {dwelling_count}           AS llm_dwelling_count,
                {dwelling_kind}            AS llm_dwelling_kind,
                ca.description             AS llm_project_description,
                ca.lot_on_plan             AS llm_lot_on_plan,
                CASE WHEN ents.applicant_company_id IS NOT NULL
                     OR docs_agg.n_docs > 0 THEN 'derived'
                     ELSE NULL END         AS llm_status,
                docs_agg.n_docs            AS llm_n_docs,
                docs_agg.n_info_requests   AS llm_n_info_requests,
                docs_agg.n_amendments      AS llm_n_amendments,
                docs_agg.total_bytes       AS llm_total_bytes,
                DATEDIFF(ca.decision_date, ca.lodged_date) AS llm_days_lodge_to_decide,
                CAST(COALESCE(
                  (SELECT MAX(dp.land_area_m2)
                     FROM domain_properties dp
                    WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND dp.unit_number = ''),
                  (SELECT MAX(rp.land_area_m2)
                     FROM realestate_properties rp
                    WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND rp.unit_number = ''),
                  (SELECT MAX(bf.site_area_m2)
                     FROM da_build_features bf
                    WHERE bf.application_id = ca.id
                      AND bf.site_area_m2 > 0)
                ) AS UNSIGNED)                                                        AS site_area_m2,
                CASE
                  WHEN EXISTS (SELECT 1 FROM domain_properties dp
                                WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                                  AND dp.unit_number = ''
                                  AND dp.land_area_m2 IS NOT NULL) THEN 'domain'
                  WHEN EXISTS (SELECT 1 FROM realestate_properties rp
                                WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                                  AND rp.unit_number = ''
                                  AND rp.land_area_m2 IS NOT NULL) THEN 'realestate'
                  WHEN EXISTS (SELECT 1 FROM da_build_features bf
                                WHERE bf.application_id = ca.id
                                  AND bf.site_area_m2 > 0) THEN 'da_docs'
                  ELSE NULL
                END                                                                   AS site_area_source,
                -- Pre-redev price: most-recent (not MAX) sale on file before
                -- lodgement, gated to within 10 years of lodgement. Older sales
                -- predate the developer's likely purchase (lots get held,
                -- inherited, family-transferred without a recorded sale), so
                -- showing them as 'site cost' is misleading. >10y → NULL.
                CAST(COALESCE(
                  (SELECT s.event_price
                     FROM domain_sales s
                     JOIN domain_properties dp ON dp.id = s.domain_property_id
                    WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND s.event_date < ca.lodged_date
                      AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                      AND s.is_sold = 1
                      AND s.event_price IS NOT NULL
                    ORDER BY s.event_date DESC LIMIT 1),
                  (SELECT s.event_price
                     FROM realestate_sales s
                     JOIN realestate_properties rp ON rp.id = s.realestate_property_id
                    WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND s.event_date < ca.lodged_date
                      AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                      AND s.event_type = 'sold'
                      AND s.event_price IS NOT NULL
                    ORDER BY s.event_date DESC LIMIT 1)
                ) AS UNSIGNED)                                                        AS pre_price,
                COALESCE(
                  (SELECT s.event_date
                     FROM domain_sales s
                     JOIN domain_properties dp ON dp.id = s.domain_property_id
                    WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND s.event_date < ca.lodged_date
                      AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                      AND s.is_sold = 1
                      AND s.event_price IS NOT NULL
                    ORDER BY s.event_date DESC LIMIT 1),
                  (SELECT s.event_date
                     FROM realestate_sales s
                     JOIN realestate_properties rp ON rp.id = s.realestate_property_id
                    WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND s.event_date < ca.lodged_date
                      AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                      AND s.event_type = 'sold'
                      AND s.event_price IS NOT NULL
                    ORDER BY s.event_date DESC LIMIT 1)
                )                                                                     AS pre_date,
                CASE
                  WHEN EXISTS (SELECT 1 FROM domain_sales s
                                 JOIN domain_properties dp ON dp.id = s.domain_property_id
                                WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                                  AND s.event_date < ca.lodged_date
                                  AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                                  AND s.is_sold = 1
                                  AND s.event_price IS NOT NULL) THEN 'domain'
                  WHEN EXISTS (SELECT 1 FROM realestate_sales s
                                 JOIN realestate_properties rp ON rp.id = s.realestate_property_id
                                WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                                  AND s.event_date < ca.lodged_date
                                  AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
                                  AND s.event_type = 'sold'
                                  AND s.event_price IS NOT NULL) THEN 'realestate'
                  ELSE NULL
                END                                                                   AS pre_source,
                -- EXISTS rather than JOIN so duplicate parent rows in
                -- domain_properties don't multiply the unit-sale count.
                CAST(COALESCE((SELECT SUM(s.event_price)
                   FROM domain_sales s
                   JOIN domain_properties unit ON unit.id = s.domain_property_id
                  WHERE unit.unit_number <> ''
                    AND EXISTS (SELECT 1 FROM domain_properties parent
                                 WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                   AND parent.street_number = unit.street_number
                                   AND parent.street_name   = unit.street_name
                                   AND parent.suburb        = unit.suburb
                                   AND parent.unit_number   = '')
                    AND s.event_date > ca.decision_date
                    AND s.is_sold = 1
                    AND s.event_price IS NOT NULL), 0) AS UNSIGNED)                  AS post_total,
                (SELECT COUNT(*)
                   FROM domain_sales s
                   JOIN domain_properties unit ON unit.id = s.domain_property_id
                  WHERE unit.unit_number <> ''
                    AND EXISTS (SELECT 1 FROM domain_properties parent
                                 WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                   AND parent.street_number = unit.street_number
                                   AND parent.street_name   = unit.street_name
                                   AND parent.suburb        = unit.suburb
                                   AND parent.unit_number   = '')
                    AND s.event_date > ca.decision_date
                    AND s.is_sold = 1
                    AND s.event_price IS NOT NULL)                                  AS n_post_sales,
                -- Pre-redev parent room counts. Domain first; realestate fallback
                -- when domain has the row but the field is NULL (e.g. MCU 50 Dolphin).
                CAST(COALESCE(
                  (SELECT MAX(dp.bedrooms) FROM domain_properties dp
                    WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND dp.unit_number = ''),
                  (SELECT MAX(rp.bedrooms) FROM realestate_properties rp
                    WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND rp.unit_number = '')
                ) AS SIGNED)                                                        AS pre_bedrooms,
                CAST(COALESCE(
                  (SELECT MAX(dp.bathrooms) FROM domain_properties dp
                    WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND dp.unit_number = ''),
                  (SELECT MAX(rp.bathrooms) FROM realestate_properties rp
                    WHERE rp.display_address LIKE CONCAT(ca.street_address, '%')
                      AND rp.unit_number = '')
                ) AS SIGNED)                                                        AS pre_bathrooms,
                -- Pre-redev parcel type (Domain only — Realestate doesn't list
                -- 'Land'). 'Land' / 'Vacant' indicates the original site had no
                -- dwelling: the supply delta starts from 0, not 1.
                (SELECT MAX(dp.property_type) FROM domain_properties dp
                  WHERE dp.display_address LIKE CONCAT(ca.street_address, '%')
                    AND dp.unit_number = '')                                          AS pre_property_type,
                -- Post-redev bedrooms/bathrooms. Priority chain:
                --   1. domain: sum across distinct unit children — but only
                --      if the count of unit children >= dwelling_count
                --      (otherwise we have partial data that misleads).
                --   2. realestate: same shape, same completeness gate.
                --   3. da_docs: LLM-extracted per-unit count from drawings,
                --      times dwelling_count. Used as a projection when no
                --      complete listing data is available.
                CAST(COALESCE(
                  (SELECT SUM(t.b) FROM (
                    SELECT MAX(unit.bedrooms) AS b
                      FROM domain_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM domain_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                     GROUP BY unit.unit_number
                  ) t HAVING COUNT(t.b) >= ({dwelling_count})),
                  (SELECT SUM(t.b) FROM (
                    SELECT MAX(unit.bedrooms) AS b
                      FROM realestate_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM realestate_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                     GROUP BY unit.unit_number
                  ) t HAVING COUNT(t.b) >= ({dwelling_count})),
                  (SELECT MAX(bf.bedrooms) FROM da_build_features bf
                    WHERE bf.application_id = ca.id
                      AND bf.template_key = 'build_features_drawings'
                      AND bf.bedrooms > 0) * NULLIF(({dwelling_count}), 0)
                ) AS SIGNED)                                                        AS post_bedrooms,
                CAST(COALESCE(
                  (SELECT SUM(t.b) FROM (
                    SELECT MAX(unit.bathrooms) AS b
                      FROM domain_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM domain_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                     GROUP BY unit.unit_number
                  ) t HAVING COUNT(t.b) >= ({dwelling_count})),
                  (SELECT SUM(t.b) FROM (
                    SELECT MAX(unit.bathrooms) AS b
                      FROM realestate_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM realestate_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                     GROUP BY unit.unit_number
                  ) t HAVING COUNT(t.b) >= ({dwelling_count})),
                  (SELECT MAX(bf.bathrooms) FROM da_build_features bf
                    WHERE bf.application_id = ca.id
                      AND bf.template_key = 'build_features_drawings'
                      AND bf.bathrooms > 0) * NULLIF(({dwelling_count}), 0)
                ) AS SIGNED)                                                        AS post_bathrooms,
                CASE
                  WHEN (SELECT COUNT(DISTINCT unit.unit_number) FROM domain_properties unit
                         WHERE unit.unit_number <> ''
                           AND EXISTS (SELECT 1 FROM domain_properties parent
                                        WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                          AND parent.street_number = unit.street_number
                                          AND parent.street_name   = unit.street_name
                                          AND parent.suburb        = unit.suburb
                                          AND parent.unit_number   = '')) >= ({dwelling_count}) THEN 'domain'
                  WHEN (SELECT COUNT(DISTINCT unit.unit_number) FROM realestate_properties unit
                         WHERE unit.unit_number <> ''
                           AND EXISTS (SELECT 1 FROM realestate_properties parent
                                        WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                          AND parent.street_number = unit.street_number
                                          AND parent.street_name   = unit.street_name
                                          AND parent.suburb        = unit.suburb
                                          AND parent.unit_number   = '')) >= ({dwelling_count}) THEN 'realestate'
                  WHEN EXISTS (SELECT 1 FROM da_build_features bf
                                WHERE bf.application_id = ca.id
                                  AND bf.template_key = 'build_features_drawings'
                                  AND (bf.bedrooms > 0 OR bf.bathrooms > 0))
                       AND ({dwelling_count}) > 0 THEN 'da_docs'
                  ELSE NULL
                END                                                                   AS post_rooms_source,
                -- Project lifecycle for approved DAs:
                --   built_sold      = at least one post-decision unit sale
                --   built_unsold    = unit-prefixed property record exists,
                --                     OR Google evidence for the unit address
                --   abandoned_likely = decision_date >3 years old, no signals
                --   unknown         = approved but too early to tell
                -- NULL for non-approved DAs.
                CASE
                  WHEN LOWER(COALESCE(ca.decision_outcome, '')) NOT LIKE '%approv%' THEN NULL
                  WHEN (SELECT COUNT(*)
                          FROM domain_sales s
                          JOIN domain_properties unit ON unit.id = s.domain_property_id
                         WHERE unit.unit_number <> ''
                           AND EXISTS (SELECT 1 FROM domain_properties parent
                                        WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                          AND parent.street_number = unit.street_number
                                          AND parent.street_name   = unit.street_name
                                          AND parent.suburb        = unit.suburb
                                          AND parent.unit_number   = '')
                           AND s.event_date > ca.decision_date
                           AND s.is_sold = 1
                           AND s.event_price IS NOT NULL) > 0 THEN 'built_sold'
                  WHEN EXISTS (
                    SELECT 1 FROM domain_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM domain_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                       AND (
                         EXISTS (SELECT 1 FROM domain_sales s
                                  WHERE s.domain_property_id = unit.id
                                    AND s.event_date > ca.decision_date)
                         OR EXISTS (SELECT 1 FROM domain_listings dl
                                     WHERE dl.domain_property_id = unit.id
                                       AND dl.fetched_at > ca.decision_date)
                       )
                  ) THEN 'built_unsold'
                  WHEN EXISTS (
                    SELECT 1 FROM realestate_properties unit
                     WHERE unit.unit_number <> ''
                       AND EXISTS (SELECT 1 FROM realestate_properties parent
                                    WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                                      AND parent.street_number = unit.street_number
                                      AND parent.street_name   = unit.street_name
                                      AND parent.suburb        = unit.suburb
                                      AND parent.unit_number   = '')
                       AND (
                         EXISTS (SELECT 1 FROM realestate_sales s
                                  WHERE s.realestate_property_id = unit.id
                                    AND s.event_date > ca.decision_date)
                         OR EXISTS (SELECT 1 FROM realestate_listings rl
                                     WHERE rl.realestate_property_id = unit.id
                                       AND rl.fetched_at > ca.decision_date)
                       )
                  ) THEN 'built_unsold'
                  WHEN EXISTS (SELECT 1 FROM discovered_urls du
                                WHERE du.search_address REGEXP CONCAT('^[0-9]+/', SUBSTRING_INDEX(ca.street_address, ',', 1))
                                  AND du.url_kind IS NOT NULL
                                  AND du.discovered_at > ca.decision_date) THEN 'built_unsold'
                  WHEN ca.decision_date IS NOT NULL
                       AND ca.decision_date < DATE_SUB(CURDATE(), INTERVAL 3 YEAR) THEN 'abandoned_likely'
                  ELSE 'unknown'
                END                                                                   AS built_status
           FROM council_applications ca
           {join_kind} JOIN (
             SELECT ae.application_id,
                    MAX(CASE WHEN ae.role='applicant' THEN c.display_name END) AS applicant_name,
                    MAX(CASE WHEN ae.role='applicant' THEN c.acn          END) AS applicant_acn,
                    MAX(CASE WHEN ae.role='applicant' THEN c.entity_type  END) AS applicant_entity_type,
                    MAX(CASE WHEN ae.role='applicant' THEN ae.company_id  END) AS applicant_company_id,
                    MAX(CASE WHEN ae.role='agent'     THEN c.display_name END) AS applicant_agent_name,
                    MAX(CASE WHEN ae.role='builder'   THEN c.display_name END) AS builder_name,
                    MAX(CASE WHEN ae.role='architect' THEN c.display_name END) AS architect_name,
                    MAX(CASE WHEN ae.role='owner'     THEN c.display_name END) AS owner_name
               FROM application_entities ae
               LEFT JOIN companies c ON c.id = ae.company_id
              GROUP BY ae.application_id
           ) ents ON ents.application_id = ca.id
           LEFT JOIN (
             SELECT application_id,
                    CAST(COUNT(*) AS UNSIGNED) AS n_docs,
                    CAST(COALESCE(SUM(file_size), 0) AS UNSIGNED) AS total_bytes,
                    CAST(SUM(doc_kind IN ('ir_council','ir_response')) AS UNSIGNED) AS n_info_requests,
                    CAST(SUM(doc_kind IN ('amendment','further_info')) AS UNSIGNED) AS n_amendments
               FROM council_application_documents
              GROUP BY application_id
           ) docs_agg ON docs_agg.application_id = ca.id
          WHERE 1=1",
        join_kind = join_kind,
        dwelling_count = DWELLING_COUNT_DERIVED,
        dwelling_kind = DWELLING_KIND_DERIVED,
    );
    let mut binds: Vec<String> = Vec::new();

    if let Some(kind_str) = &req.kind {
        if let Some(kind) = DaKind::parse(kind_str) {
            sql.push_str(" AND ");
            // kind_sql_filter() emits unqualified column names; rewrite
            // to qualify against `ca` since we now have a JOIN with
            // overlapping column names (description, etc.).
            sql.push_str(&kind_sql_filter(kind).replace("description", "ca.description"));
        }
    }
    if let Some(suburb) = &req.suburb {
        sql.push_str(" AND UPPER(ca.suburb) = ?");
        binds.push(suburb.to_uppercase());
    }
    if let Some(type_code) = &req.type_code {
        sql.push_str(" AND ca.type_code = ?");
        binds.push(type_code.to_uppercase());
    }
    if let Some(s) = &req.date_from {
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request("date_from must be YYYY-MM-DD"))?;
        sql.push_str(" AND ca.lodged_date >= ?");
        binds.push(d.to_string());
    }
    if let Some(s) = &req.date_to {
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request("date_to must be YYYY-MM-DD"))?;
        sql.push_str(" AND ca.lodged_date <= ?");
        binds.push(d.to_string());
    }
    if let Some(needle) = &req.q {
        sql.push_str(
            " AND (ca.description LIKE ? OR ca.raw_address LIKE ? OR ca.application_id LIKE ?)",
        );
        let pattern = format!("%{}%", needle);
        binds.push(pattern.clone());
        binds.push(pattern.clone());
        binds.push(pattern);
    }
    // Drop rows where pre_price is NULL — the recency cap on pre_price
    // means these have no usable parent sale on file (>10y old or none
    // at all), so the redev margin can't be computed and the row isn't
    // actionable for the dataset's core purpose. HAVING references the
    // SELECT alias since pre_price is a derived column.
    sql.push_str(" HAVING pre_price IS NOT NULL");
    sql.push_str(" ORDER BY ca.lodged_date DESC, ca.id DESC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql);
    for b in &binds {
        query = query.bind(b);
    }
    let rows = query
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
        .into_status()?;

    // Build the base list, then enrich each with per-unit sales for any
    // app that has a SaleStory. We do this in a second pass to keep the
    // already-complex outer query manageable.
    let mut items: Vec<pb::Application> =
        rows.into_iter().map(row_to_application).collect();
    enrich_unit_sales(pool, &mut items).await?;
    Ok(pb::ListApplicationsResponse { items })
}

/// For each application that already has a SaleStory, find the latest
/// sale per unit-prefixed child of its parent property and attach
/// `SaleStory.unit_sales`. Skips apps without sale data — keeps the
/// extra round-trip count low.
async fn enrich_unit_sales(
    pool: &MySqlPool,
    items: &mut [pb::Application],
) -> Result<(), Status> {
    let app_ids: Vec<i64> = items
        .iter()
        .filter(|a| a.sale_story.is_some())
        .map(|a| a.id)
        .collect();
    if app_ids.is_empty() {
        return Ok(());
    }

    // Build IN (...) placeholder list. Uses ca.street_address now (no
    // da_summaries dependency).
    let placeholders = std::iter::repeat("?").take(app_ids.len()).collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT DISTINCT ca.id AS app_pk,
                unit.unit_number,
                unit.display_address,
                CAST(latest.sold_price AS UNSIGNED) AS sold_price,
                latest.sold_date
           FROM council_applications ca
           JOIN domain_properties unit
             ON unit.unit_number <> ''
           JOIN (
             SELECT s.domain_property_id,
                    MAX(s.event_date) AS sold_date,
                    MAX(s.event_price) AS sold_price
               FROM domain_sales s
              WHERE s.is_sold = 1 AND s.event_price IS NOT NULL
              GROUP BY s.domain_property_id
           ) latest ON latest.domain_property_id = unit.id
          WHERE ca.id IN ({placeholders})
            AND latest.sold_date > ca.decision_date
            AND EXISTS (SELECT 1 FROM domain_properties parent
                         WHERE parent.display_address LIKE CONCAT(ca.street_address, '%')
                           AND parent.street_number = unit.street_number
                           AND parent.street_name   = unit.street_name
                           AND parent.suburb        = unit.suburb
                           AND parent.unit_number   = '')
          ORDER BY ca.id, unit.unit_number"
    );
    let mut q = sqlx::query(&sql);
    for id in &app_ids {
        q = q.bind(*id as u64);
    }
    let rows = q.fetch_all(pool).await.into_status()?;

    use std::collections::HashMap;
    let mut by_app: HashMap<i64, Vec<pb::UnitSale>> = HashMap::new();
    for r in rows {
        let app_pk: u64 = r.get("app_pk");
        let unit_number: String = r.try_get("unit_number").unwrap_or_default();
        let display_address: String = r.try_get("display_address").unwrap_or_default();
        let sold_price_u: Option<u64> = r.try_get("sold_price").ok();
        let sold_date: Option<chrono::NaiveDate> = r.try_get("sold_date").ok();
        by_app.entry(app_pk as i64).or_default().push(pb::UnitSale {
            unit_number,
            display_address,
            sold_price: sold_price_u.map(|v| v as i64),
            sold_date: sold_date.map(|d| d.format("%Y-%m-%d").to_string()),
        });
    }

    for app in items.iter_mut() {
        if let Some(sale_story) = app.sale_story.as_mut() {
            if let Some(unit_sales) = by_app.remove(&app.id) {
                sale_story.unit_sales = unit_sales;
            }
        }
    }
    Ok(())
}

pub async fn detail(
    pool: &MySqlPool,
    req: pb::GetApplicationRequest,
) -> Result<pb::ApplicationDetail, Status> {
    let row = sqlx::query(
        "SELECT id, council_slug, vendor, application_id, type_code, application_type, status,
                decision_outcome, lodged_date, decision_date, approved_units,
                raw_address, suburb, postcode, description, application_url,
                applicant_name, builder_name, architect_name, owner_name,
                internal_property_id, lot_on_plan,
                list_first_seen_at, detail_fetched_at, docs_fetched_at
           FROM council_applications
          WHERE council_slug = ? AND application_id = ?
          LIMIT 1",
    )
    .bind(&req.council_slug)
    .bind(&req.application_id)
    .fetch_optional(pool)
    .await
    .into_status()?
    .ok_or_else(|| Status::not_found("application not found"))?;

    // council_applications.id is BIGINT UNSIGNED — read as u64, cast for the
    // FK bind below and for the proto i64 field at the boundary.
    let app_pk: u64 = row.get("id");
    let base = Some(row_to_application_ref(&row));

    let docs = sqlx::query(
        "SELECT id, doc_oid, doc_type, title, source_url, mime_type, file_size,
                page_count, published_at, downloaded_at,
                (file_path IS NOT NULL) AS has_file
           FROM council_application_documents
          WHERE application_id = ?
          ORDER BY published_at IS NULL, published_at ASC, id ASC",
    )
    .bind(app_pk)
    .fetch_all(pool)
    .await
    .into_status()?
    .into_iter()
    .map(|r| {
        let has_file_int: i64 = r.try_get("has_file").unwrap_or(0);
        let id_u: u64 = r.get("id");
        pb::Document {
            id: id_u as i64,
            doc_oid: r.try_get("doc_oid").ok(),
            doc_type: r.try_get("doc_type").ok(),
            title: r.try_get("title").ok(),
            source_url: r.try_get("source_url").ok(),
            mime_type: r.try_get("mime_type").ok(),
            file_size: r.try_get("file_size").ok(),
            page_count: r.try_get("page_count").ok(),
            published_at: opt_datetime_str(r.try_get("published_at").ok()),
            downloaded_at: opt_datetime_str(r.try_get("downloaded_at").ok()),
            has_file: has_file_int != 0,
        }
    })
    .collect();

    Ok(pb::ApplicationDetail {
        base,
        vendor: row.get("vendor"),
        applicant_name: row.try_get("applicant_name").ok(),
        builder_name: row.try_get("builder_name").ok(),
        architect_name: row.try_get("architect_name").ok(),
        owner_name: row.try_get("owner_name").ok(),
        internal_property_id: row.try_get("internal_property_id").ok(),
        lot_on_plan: row.try_get("lot_on_plan").ok(),
        list_first_seen_at: row
            .try_get("list_first_seen_at")
            .ok()
            .map(datetime_str),
        detail_fetched_at: row.try_get("detail_fetched_at").ok().map(datetime_str),
        docs_fetched_at: row.try_get("docs_fetched_at").ok().map(datetime_str),
        documents: docs,
    })
}

fn row_to_application(r: MySqlRow) -> pb::Application {
    row_to_application_ref(&r)
}

fn row_to_application_ref(r: &MySqlRow) -> pb::Application {
    let description: Option<String> = r.try_get("description").ok();
    let type_code: Option<String> = r.try_get("type_code").ok();
    let approved_units: Option<i32> = r.try_get("approved_units").ok();
    let kind = classify(description.as_deref(), type_code.as_deref(), approved_units);
    let id_u: u64 = r.get("id");

    let insight = build_insight(r);
    let sale_story = build_sale_story(r);

    pb::Application {
        id: id_u as i64,
        council_slug: r.get("council_slug"),
        application_id: r.get("application_id"),
        kind: kind_to_proto(kind),
        type_code,
        application_type: r.try_get("application_type").ok(),
        status: r.try_get("status").ok(),
        decision_outcome: r.try_get("decision_outcome").ok(),
        lodged_date: opt_date_str(r.try_get("lodged_date").ok()),
        decision_date: opt_date_str(r.try_get("decision_date").ok()),
        approved_units,
        raw_address: r.try_get("raw_address").ok(),
        suburb: r.try_get("suburb").ok(),
        postcode: r.try_get("postcode").ok(),
        description,
        application_url: r.try_get("application_url").ok(),
        insight,
        sale_story,
    }
}

fn build_insight(r: &MySqlRow) -> Option<pb::DaInsight> {
    // Sourced from regex-harvested application_entities + derived dwelling
    // fields + council_application_documents aggregate (no LLM in the
    // loop). Returns None if the DA has no entity/doc signal at all
    // (preserves the old "blank insight = no data" UX).
    let status: Option<String> = r.try_get("llm_status").ok();
    let status = status?;

    let total_bytes_u: Option<u64> = r.try_get("llm_total_bytes").ok();
    let total_bytes: Option<i64> = total_bytes_u.map(|v| v as i64);
    let n_docs_u: Option<u64> = r.try_get("llm_n_docs").ok();
    let n_info_u: Option<u64> = r.try_get("llm_n_info_requests").ok();
    let n_amend_u: Option<u64> = r.try_get("llm_n_amendments").ok();
    let dev_count: Option<i64> = r.try_get("llm_developer_project_count").ok();

    // dwelling_count comes back as DECIMAL (CASE branches mix integer
    // literals with approved_units INT). Decode as i64 then narrow.
    let dwelling_count: Option<i32> = r
        .try_get::<Option<i64>, _>("llm_dwelling_count")
        .ok()
        .flatten()
        .map(|v| v as i32);
    // DATEDIFF returns INT — decodes directly as i32.
    let days: Option<i32> = r.try_get("llm_days_lodge_to_decide").ok();

    Some(pb::DaInsight {
        applicant_name: r.try_get("llm_applicant_name").ok(),
        applicant_acn: r.try_get("llm_applicant_acn").ok(),
        applicant_entity_type: r.try_get("llm_applicant_entity_type").ok(),
        applicant_agent_name: r.try_get("llm_applicant_agent_name").ok(),
        builder_name: r.try_get("llm_builder_name").ok(),
        architect_name: r.try_get("llm_architect_name").ok(),
        dwelling_count,
        dwelling_kind: r.try_get("llm_dwelling_kind").ok(),
        project_description: r.try_get("llm_project_description").ok(),
        lot_on_plan: r.try_get("llm_lot_on_plan").ok(),
        status,
        n_docs: n_docs_u.unwrap_or(0) as i32,
        n_information_requests: n_info_u.unwrap_or(0) as i32,
        n_amendments: n_amend_u.unwrap_or(0) as i32,
        total_bytes,
        days_lodge_to_decide: days,
        developer_project_count: dev_count.map(|v| v as i32),
    })
}

fn build_sale_story(r: &MySqlRow) -> Option<pb::SaleStory> {
    // pre_price is u32 (event_price column is INT UNSIGNED).
    // post_total is u64 because we CAST(... AS UNSIGNED) in SQL — that
    // type is decodable as u64 by sqlx without bigdecimal.
    let pre_price_u: Option<u32> = r.try_get("pre_price").ok();
    let pre_date: Option<chrono::NaiveDate> = r.try_get("pre_date").ok();
    let post_total_u: Option<u64> = r.try_get("post_total").ok();
    let n_post: Option<i64> = r.try_get("n_post_sales").ok();

    let pre_price = pre_price_u.map(|v| v as i64);
    let post_total: Option<i64> = post_total_u.map(|v| v as i64).filter(|&v| v > 0);
    let n_post_sales = n_post.unwrap_or(0) as i32;

    let pre_bedrooms: Option<i64> = r.try_get("pre_bedrooms").ok();
    let pre_bathrooms: Option<i64> = r.try_get("pre_bathrooms").ok();
    let post_bedrooms: Option<i64> = r.try_get("post_bedrooms").ok();
    let post_bathrooms: Option<i64> = r.try_get("post_bathrooms").ok();

    // Skip the message if neither pre/post sale nor any room counts exist.
    if pre_price.is_none()
        && post_total.is_none()
        && pre_bedrooms.is_none()
        && post_bedrooms.is_none()
    {
        return None;
    }

    let gross_spread = match (pre_price, post_total) {
        (Some(pre), Some(post)) => Some(post - pre),
        _ => None,
    };

    let site_area_m2_u: Option<u32> = r.try_get("site_area_m2").ok();
    let site_area_source: Option<String> = r.try_get("site_area_source").ok();
    let pre_source: Option<String> = r.try_get("pre_source").ok();
    let post_rooms_source: Option<String> = r.try_get("post_rooms_source").ok();
    let pre_property_type: Option<String> = r.try_get("pre_property_type").ok();
    let built_status: Option<String> = r.try_get("built_status").ok();

    Some(pb::SaleStory {
        pre_price,
        pre_date: pre_date.map(|d| d.format("%Y-%m-%d").to_string()),
        pre_source,
        post_total,
        n_post_sales: Some(n_post_sales),
        gross_spread,
        unit_sales: vec![],   // populated by enrich_unit_sales() in a 2nd pass
        site_area_m2: site_area_m2_u.map(|v| v as i32),
        pre_bedrooms: pre_bedrooms.map(|v| v as i32),
        pre_bathrooms: pre_bathrooms.map(|v| v as i32),
        post_bedrooms: post_bedrooms.map(|v| v as i32),
        post_bathrooms: post_bathrooms.map(|v| v as i32),
        site_area_source,
        post_rooms_source,
        pre_property_type,
        built_status,
    })
}
