/*
  page 1
  https://www.domain.com.au/sold-listings/burleigh-heads-qld-4220/?excludepricewithheld=1
  https://www.domain.com.au/sold-listings/burleigh-heads-qld-4220/?excludepricewithheld=1&page=2
*/
pub mod model;

#[test]
fn test_deserialize() {
    use serde::{Deserialize, Serialize};
    let json_string = std::fs::read_to_string("fixtures/domain.json").expect("couldnt open file");
    let result: model::PropsWrapper = serde_json::from_str(&json_string).unwrap();
    println!("{:#?}", result);
}

#[test]
fn test_deser_0() {
    use serde::{Serialize, Deserialize};
    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct JsonLdWrapper {
        pub json_ld_items: Vec<model::JsonLdItem>,
    }
    let json_string  = r#"{
    "jsonLdItems": [
        {
            "@context": "http://schema.org",
            "@type": "Organization",
            "name": "Domain",
            "url": "https://www.domain.com.au",
            "sameAs": [
                "https://www.facebook.com/domain.com.au",
                "https://twitter.com/domaincomau",
                "https://plus.google.com/+domaincomau",
                "https://www.linkedin.com/company/domain-com-au"
            ]
        },
        {
            "@context": "http://schema.org",
            "@type": "Event",
            "location": {
                "@type": "Residence",
                "geo": {
                    "@type": "GeoCoordinates",
                    "latitude": -28.09119,
                    "longitude": 153.45746
                },
                "name": "1 Short Street, Burleigh Heads",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "BURLEIGH HEADS",
                    "addressRegion": "QLD",
                    "postalCode": "4220",
                    "streetAddress": "1 Short Street"
                }
            },
            "name": "Auction",
            "description": "Auction at 1 Short Street",
            "url": "https://www.domain.com.au/1-short-street-burleigh-heads-qld-4220-2020418135",
            "image": "https://bucket-api.domain.com.au/v1/bucket/image/2020418135_1_1_251112_063947-w4464-h2970",
            "startDate": "2025-12-12T11:45:00"
        }
    ]
}"#;

    let items: JsonLdWrapper = serde_json::from_str(&json_string).unwrap();
    println!("{:#?}", items);
}

#[test]
fn test_listing() {
    let json_string  = r##"
    {

                                            "id": 2020299115,
                                            "listingType": "listing",
                                            "listingModel": {
                                                "promoType": "elite",
                                                "url": "/802-29-hill-avenue-burleigh-heads-qld-4220-2020299115",
                                                "images": [
                                                    "https://rimh2.domainstatic.com.au/uKibPgsrsS2KByATuREe28KNFrk=/660x440/filters:format(jpeg):quality(80)/2020299115_2_1_250926_025630-w2500-h1666",
                                                    "https://rimh2.domainstatic.com.au/MmRAEZY9VX8bPNJQg8iI5jBPZ88=/660x440/filters:format(jpeg):quality(80)/2020299115_1_1_250926_025630-w2500-h1667"
                                                ],
                                                "skeletonImages": [
                                                    {
                                                        "images": {
                                                            "original": {
                                                                "url": "https://rimh2.domainstatic.com.au/Wy-BLLstiAzBP5XMEnW3GoxE6Y0=/fit-in/1920x1080/filters:format(jpeg):quality(80):no_upscale()/2020299115_2_1_250926_025630-w2500-h1666",
                                                                "width": 1621,
                                                                "height": 1080
                                                            },
                                                            "tablet": {
                                                                "url": "https://rimh2.domainstatic.com.au/wNMF0tTcp9ssy8as5D2z3657LSU=/fit-in/1020x1020/filters:format(jpeg):quality(80):no_upscale()/2020299115_2_1_250926_025630-w2500-h1666",
                                                                "width": 1020,
                                                                "height": 680
                                                            },
                                                            "mobile": {
                                                                "url": "https://rimh2.domainstatic.com.au/46JMLTZNy-G4PPEqguQS8VCCpNM=/fit-in/600x800/filters:format(jpeg):quality(80):no_upscale()/2020299115_2_1_250926_025630-w2500-h1666",
                                                                "width": 600,
                                                                "height": 400
                                                            }
                                                        },
                                                        "mediaType": "image"
                                                    },
                                                    {
                                                            "images": {
                                                                "original": {
                                                                    "url": "https://rimh2.domainstatic.com.au/xc_14lXxbgwEaSnx9WhnT88RW10=/fit-in/1920x1080/filters:format(jpeg):quality(80):no_upscale()/2020299115_12_1_250926_025634-w2500-h1666",
                                                                    "width": 1621,
                                                                    "height": 1080
                                                                },
                                                                "tablet": {
                                                                    "url": "https://rimh2.domainstatic.com.au/r7dSS0I3MjYShgBYxdJCp7kB7iY=/fit-in/1020x1020/filters:format(jpeg):quality(80):no_upscale()/2020299115_12_1_250926_025634-w2500-h1666",
                                                                    "width": 1020,
                                                                    "height": 680
                                                                },
                                                                "mobile": {
                                                                    "url": "https://rimh2.domainstatic.com.au/1smq6HPGZP3mzQd7-XNXOUAl1V8=/fit-in/600x800/filters:format(jpeg):quality(80):no_upscale()/2020299115_12_1_250926_025634-w2500-h1666",
                                                                    "width": 600,
                                                                    "height": 400
                                                                }
                                                            },
                                                            "mediaType": "image"
                                                        }
                                                    ],
                                                    "brandingAppearance": "dark",
                                                    "price": "$915,000",
                                                    "hasVideo": false,
                                                    "branding": {
                                                        "agencyId": 24127,
                                                        "agents": [
                                                            {
                                                                "agentName": "Will West",
                                                                "agentPhoto": "https://rimh2.domainstatic.com.au/o9yeFt62fwHRACKsb5n5IpqKkK4=/90x90/filters:format(jpeg):quality(80)/https://images.domain.com.au/img/24127/contact_1563506.jpeg?mod=260113-114253"
                                                            },
                                                            {
                                                                "agentName": "William Lord",
                                                                "agentPhoto": "https://rimh2.domainstatic.com.au/gHEvpPXyrVGvnR5TBa1YvC8qnJ0=/90x90/filters:format(jpeg):quality(80)/https://images.domain.com.au/img/24127/contact_1967921.jpeg?mod=260108-101030"
                                                            }
                                                        ],
                                                        "agentNames": "Will West, William Lord",
                                                        "brandLogo": "https://rimh2.domainstatic.com.au/b_buIA6f_J4E_9ZL8AIQcuZnUyI=/170x60/filters:format(jpeg):quality(80)/https://images.domain.com.au/img/Agencys/24127/logo_24127.jpeg?buster=2026-01-15",
                                                        "skeletonBrandLogo": "https://rimh2.domainstatic.com.au/UDWdnhy_jeERTH_Ks2Wg2pv9BNg=/120x42/filters:format(jpeg):quality(80):no_upscale()/https://images.domain.com.au/img/Agencys/24127/logo_24127.jpeg?buster=2026-01-15",
                                                        "brandName": "Lacey West Real Estate",
                                                        "brandColor": "#fc5001",
                                                        "agentPhoto": "https://rimh2.domainstatic.com.au/o9yeFt62fwHRACKsb5n5IpqKkK4=/90x90/filters:format(jpeg):quality(80)/https://images.domain.com.au/img/24127/contact_1563506.jpeg?mod=260113-114253",
                                                        "agentName": "Will West"
                                                    },
                                                    "address": {
                                                        "street": "802/29 Hill Avenue",
                                                        "suburb": "BURLEIGH HEADS",
                                                        "state": "QLD",
                                                        "postcode": "4220",
                                                        "lat": -28.091883,
                                                        "lng": 153.44943
                                                    },
                                                    "features": {
                                                        "beds": 2,
                                                        "baths": 1,
                                                        "parking": 1,
                                                        "propertyType": "ApartmentUnitFlat",
                                                        "propertyTypeFormatted": "Apartment / Unit / Flat",
                                                        "isRural": false,
                                                        "landSize": 0,
                                                        "landUnit": "m²",
                                                        "isRetirement": false
                                                    },
                                                    "inspection": {
                                                        "openTime": null,
                                                        "closeTime": null
                                                    },
                                                    "auction": null,
                                                    "tags": {
                                                        "tagText": "Sold by private treaty 04 Dec 2025",
                                                        "tagClassName": "is-sold"
                                                    },
                                                    "displaySearchPriceRange": null,
                                                    "enableSingleLineAddress": true
                                            }}"##;
    let items: model::Listing= serde_json::from_str(&json_string).unwrap();
    println!("{:#?}", items);
}
