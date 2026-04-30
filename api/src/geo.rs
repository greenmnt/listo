//! Deterministic dummy lat/lng generator anchored on Gold Coast suburb
//! centroids. Used until real geocoding lands (`properties.lat`/`lng` are
//! all NULL right now). Same `application_id` always produces the same
//! point so dots stay stable across reloads.

use std::collections::HashMap;
use once_cell::sync::Lazy;

/// (lat, lng) for the centre of a small set of high-volume Gold Coast
/// suburbs. Apps without a known suburb get the GC bbox centre.
static SUBURB_CENTROIDS: Lazy<HashMap<&'static str, (f64, f64)>> = Lazy::new(|| {
    let mut m = HashMap::new();
    let centroids: &[(&str, f64, f64)] = &[
        ("SURFERS PARADISE",  -28.0027, 153.4276),
        ("BROADBEACH",        -28.0319, 153.4310),
        ("BROADBEACH WATERS", -28.0299, 153.4080),
        ("BURLEIGH HEADS",    -28.0886, 153.4521),
        ("BURLEIGH WATERS",   -28.0950, 153.4290),
        ("MERMAID BEACH",     -28.0429, 153.4367),
        ("MERMAID WATERS",    -28.0500, 153.4170),
        ("MIAMI",             -28.0700, 153.4406),
        ("PALM BEACH",        -28.1130, 153.4640),
        ("CURRUMBIN",         -28.1390, 153.4810),
        ("CURRUMBIN WATERS",  -28.1430, 153.4630),
        ("ELANORA",           -28.1290, 153.4520),
        ("TUGUN",             -28.1460, 153.4880),
        ("COOLANGATTA",       -28.1690, 153.5380),
        ("KIRRA",             -28.1665, 153.5260),
        ("BILINGA",           -28.1540, 153.5070),
        ("SOUTHPORT",         -27.9648, 153.4053),
        ("LABRADOR",          -27.9560, 153.4090),
        ("BIGGERA WATERS",    -27.9420, 153.3970),
        ("RUNAWAY BAY",       -27.9300, 153.3970),
        ("PARADISE POINT",    -27.8870, 153.3900),
        ("HOLLYWELL",         -27.9090, 153.3990),
        ("HOPE ISLAND",       -27.8770, 153.3470),
        ("HELENSVALE",        -27.9040, 153.3300),
        ("OXENFORD",          -27.8960, 153.3110),
        ("UPPER COOMERA",     -27.8920, 153.3060),
        ("COOMERA",           -27.8520, 153.3050),
        ("PIMPAMA",           -27.8170, 153.3030),
        ("ORMEAU",            -27.7460, 153.2740),
        ("YATALA",            -27.7300, 153.2150),
        ("ARUNDEL",           -27.9415, 153.3570),
        ("PARKWOOD",          -27.9460, 153.3890),
        ("MOLENDINAR",        -27.9690, 153.3760),
        ("ASHMORE",           -28.0010, 153.3770),
        ("BENOWA",            -27.9974, 153.3878),
        ("BUNDALL",           -28.0050, 153.4108),
        ("CARRARA",           -28.0210, 153.3650),
        ("MUDGEERABA",        -28.0838, 153.3686),
        ("ROBINA",            -28.0760, 153.3960),
        ("VARSITY LAKES",     -28.0820, 153.4150),
        ("CLEAR ISLAND WATERS", -28.0410, 153.3940),
        ("MERRIMAC",          -28.0490, 153.3700),
        ("REEDY CREEK",       -28.1000, 153.3950),
        ("TALLAI",            -28.0700, 153.3370),
        ("MAUDSLAND",         -27.9420, 153.3050),
        ("LOWER BEECHMONT",   -28.0790, 153.2480),
        ("CURRUMBIN VALLEY",  -28.1850, 153.4350),
        ("TALLEBUDGERA",      -28.1420, 153.4060),
        ("TALLEBUDGERA VALLEY", -28.1830, 153.3680),
        ("BONOGIN",           -28.1240, 153.3660),
        ("JACOBS WELL",       -27.7850, 153.3625),
        ("LUSCOMBE",          -27.7680, 153.2410),
        ("ARUNDEL",           -27.9415, 153.3570),
    ];
    for (name, lat, lng) in centroids {
        m.insert(*name, (*lat, *lng));
    }
    m
});

const GC_DEFAULT: (f64, f64) = (-28.005, 153.405);
const SCATTER_RADIUS_DEG: f64 = 0.012; // ~1.3km at this latitude

/// Hash an application id to a deterministic point near the suburb's
/// centroid (or the GC bbox centre when the suburb is unknown).
pub fn dummy_latlng(application_id: &str, suburb: Option<&str>) -> (f64, f64) {
    let centre = suburb
        .map(|s| s.to_ascii_uppercase())
        .and_then(|s| SUBURB_CENTROIDS.get(s.as_str()).copied())
        .unwrap_or(GC_DEFAULT);
    let h = fnv1a64(application_id.as_bytes());
    // Two independent floats in [-1, 1)
    let dx = ((h & 0xFFFF_FFFF) as f64 / u32::MAX as f64) * 2.0 - 1.0;
    let dy = ((h >> 32) as f64 / u32::MAX as f64) * 2.0 - 1.0;
    (
        centre.0 + dy * SCATTER_RADIUS_DEG,
        centre.1 + dx * SCATTER_RADIUS_DEG,
    )
}

#[inline]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let a = dummy_latlng("MCU/2025/64", Some("Tallai"));
        let b = dummy_latlng("MCU/2025/64", Some("Tallai"));
        assert_eq!(a, b);
    }

    #[test]
    fn within_bbox() {
        let (lat, lng) = dummy_latlng("OPW/2025/123", Some("Surfers Paradise"));
        assert!(lat > -28.2 && lat < -27.85);
        assert!(lng > 153.30 && lng < 153.55);
    }
}
