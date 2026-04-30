use once_cell::sync::Lazy;
use regex::Regex;

/// What kind of redevelopment a council application represents — derived
/// from the description, type code, and approved unit count. This is the
/// primary lens the frontend filters / colours by. Translated to the
/// proto `DaKind` at the service boundary via `service::conv::kind_to_proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaKind {
    /// Secondary dwelling on an existing lot — e.g. backyard granny flat.
    Granny,
    /// Two attached dwellings on one lot — duplex / dual occupancy.
    Duplex,
    /// 3+ unit development, multi-residential, townhouse complex.
    BigDev,
    /// Anything else — single house, OPW, minor change, etc.
    Other,
}

impl DaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DaKind::Granny => "granny",
            DaKind::Duplex => "duplex",
            DaKind::BigDev => "big_dev",
            DaKind::Other => "other",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "granny" => Some(DaKind::Granny),
            "duplex" => Some(DaKind::Duplex),
            "big_dev" | "big-dev" | "bigdev" => Some(DaKind::BigDev),
            "other" => Some(DaKind::Other),
            "all" => None,
            _ => None,
        }
    }
}

static GRANNY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(secondary\s+dwelling|granny\s+flat|auxiliary\s+dwelling|ancillary\s+dwelling)\b").unwrap()
});
static DUPLEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(dual\s+occupancy|duplex)\b").unwrap()
});
static BIGDEV_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(triplex|fourplex|quadruplex|multi[\s-]?(unit|dwelling|residential)|townhouse|apartment\s+building|\d+\s*x?\s*(unit|dwelling|townhouse|apartment)s?)\b").unwrap()
});

pub fn classify(
    description: Option<&str>,
    _type_code: Option<&str>,
    approved_units: Option<i32>,
) -> DaKind {
    if let Some(n) = approved_units {
        if n >= 3 {
            return DaKind::BigDev;
        }
    }
    let desc = description.unwrap_or("");
    if GRANNY_RE.is_match(desc) {
        return DaKind::Granny;
    }
    if BIGDEV_RE.is_match(desc) {
        return DaKind::BigDev;
    }
    if DUPLEX_RE.is_match(desc) {
        return DaKind::Duplex;
    }
    DaKind::Other
}

/// SQL fragment that filters `council_applications.description` to rows
/// matching the given DaKind. Wired into the WHERE clause via raw SQL since
/// MySQL's REGEXP supports the same patterns. Keep this in sync with the
/// Rust regexes above.
pub fn kind_sql_filter(kind: DaKind) -> &'static str {
    match kind {
        DaKind::Granny => {
            "(description REGEXP '(?i)secondary[[:space:]]+dwelling|granny[[:space:]]+flat|auxiliary[[:space:]]+dwelling|ancillary[[:space:]]+dwelling')"
        }
        DaKind::Duplex => {
            "(description REGEXP '(?i)dual[[:space:]]+occupancy|duplex' \
              AND description NOT REGEXP '(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling')"
        }
        DaKind::BigDev => {
            "(approved_units >= 3 OR description REGEXP '(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling|townhouse')"
        }
        DaKind::Other => {
            "(description NOT REGEXP '(?i)secondary[[:space:]]+dwelling|granny[[:space:]]+flat|dual[[:space:]]+occupancy|duplex|triplex|fourplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling')"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_granny_flat() {
        assert_eq!(
            classify(Some("Material Change of Use Code Assessment Dwelling House (Secondary Dwelling)"), None, None),
            DaKind::Granny
        );
    }

    #[test]
    fn classifies_duplex() {
        assert_eq!(
            classify(Some("Material Change of Use Code Assessment Dual Occupancy"), None, None),
            DaKind::Duplex
        );
    }

    #[test]
    fn classifies_big_dev_by_unit_count() {
        assert_eq!(classify(Some("Material Change of Use"), None, Some(8)), DaKind::BigDev);
    }

    #[test]
    fn classifies_big_dev_by_text() {
        assert_eq!(
            classify(Some("MATERIAL CHANGE OF USE - 6 Multi-unit Dwelling"), None, None),
            DaKind::BigDev
        );
    }

    #[test]
    fn other_falls_through() {
        assert_eq!(classify(Some("Operational Works"), None, None), DaKind::Other);
    }
}
