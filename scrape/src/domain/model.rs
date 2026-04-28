use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---- Top-level props ----
#[derive(Serialize, Deserialize, Debug)]
pub struct PropsWrapper {
    pub props: Props,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Props {
    pub page_props: PageProps,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PageProps {
    pub layout_props: LayoutProps,
    pub component_props: ComponentProps,
}

// ---- Layout metadata ----
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LayoutProps {
    pub title: String,
    pub description: String,
    pub canonical: String,
    pub metatags: Vec<MetaTag>,
    pub disable_ads: bool,
    pub disable_tracking: bool,
    pub is_from_eu: bool,
    pub digital_data: DigitalData,
    pub json_ld_items: Vec<JsonLdItem>,
    pub raygun_tags: Vec<String>,
    pub feature_flags: FeatureFlags,
    pub ppid: String,
    pub hero_images: HeroImages,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "component", rename_all = "camelCase")]
pub enum MetaTag {
    #[serde(rename = "link")]
    Link { rel: String, href: String },
    #[serde(rename = "meta")]
    Meta { property: String, content: String },
}

// ---- DigitalData ----
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DigitalData {
    pub page: PageInfo,
    pub titan: TitanInfo,
    pub ad_loader: String,
    pub version: String,
    pub events: Vec<serde_json::Value>,
    pub user: UserInfo,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub page_info: PageDetails,
    pub category: PageCategory,
    pub optimize: Optimize,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PageDetails {
    pub page_id: String,
    pub page_name: String,
    pub search: SearchParams,
    pub brand: String,
    pub generator: String,
    pub sys_env: String,
    pub is_embedded_app: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SearchParams {
    pub agency_ids: String,
    pub bathrooms_from: String,
    pub bathrooms_to: String,
    pub bedrooms_from: String,
    pub bedrooms_to: String,
    pub car_spaces: String,
    pub geo_type: String,
    pub map_search: String,
    pub search_type: String,
    pub median_price: u64,
    pub postcode: String,
    pub primary_property_type: String,
    pub secondary_property_type: String,
    pub results_pages: u32,
    pub results_records: String,
    pub search_area: String,
    pub search_data_defaults: SearchDataDefaults,
    pub search_depth: u32,
    pub search_location_cat: String,
    pub search_region: String,
    pub search_result_count: u32,
    pub search_suburb: String,
    pub search_term: String,
    pub search_type_view: String,
    pub sort_by: String,
    pub state: String,
    pub suburb_id: String,
    pub surrounding_suburbs: String,
    pub exclude_price_withheld: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SearchDataDefaults {
    pub sort_by: String,
    pub bedrooms_from: String,
    pub bedrooms_to: String,
    pub bathrooms_from: String,
    pub bathrooms_to: String,
    pub car_spaces: String,
    pub exclude_price_withheld: String,
    pub primary_property_type: String,
    pub surrounding_suburbs: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PageCategory {
    pub primary_category: String,
    pub sub_category1: String,
    pub page_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Optimize {
    pub shortlist_experiment_flag: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TitanInfo {
    pub ad_zone: String,
    pub ad_key_values: HashMap<String, serde_json::Value>,
    pub ad_site: String,
    pub ad_slots: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub membership_type: String,
    pub session_token: String,
    pub membership_state: String,
    pub dhl_membership: String,
    pub ip_address: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum JsonLdItem {
    Organization {
        #[serde(rename = "@context")]
        context: String,
        #[serde(rename = "@type")]
        type_: String,
        name: String,
        url: String,
        #[serde(rename = "sameAs")]
        same_as: Vec<String>,
    },
    Event {
        #[serde(rename = "@context")]
        context: String,
        #[serde(rename = "@type")]
        type_: String,
        location: EventLocation,
        name: String,
        description: String,
        url: String,
        image: String,
        #[serde(rename = "startDate")]
        start_date: String,
    },
    Residence {
        #[serde(rename = "@context")]
        context: String,
        #[serde(rename = "@type")]
        type_: String,
        address: PostalAddress,
    },
    BreadcrumbList {
        #[serde(rename = "@context")]
        context: String,
        #[serde(rename = "@type")]
        type_: String,
        #[serde(rename = "itemListElement")]
        item_list_element: Vec<BreadcrumbItem>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventLocation {
    #[serde(rename = "@type")]
    pub type_: String,
    pub geo: GeoCoordinates,
    pub name: String,
    pub address: PostalAddress,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoCoordinates {
    #[serde(rename = "@type")]
    pub type_: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PostalAddress {
    #[serde(rename = "@type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_locality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postal_code: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BreadcrumbItem {
    #[serde(rename = "@type")]
    pub type_: String,
    pub position: u32,
    pub item: BreadcrumbLink,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BreadcrumbLink {
    #[serde(rename = "@id")]
    pub id: String,
    pub name: String,
}

// ---- Feature Flags ----
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FeatureFlags {
    pub enable_content_snippet: bool,
    pub enable_content_snippet_with_postcodes: bool,
    pub disable_ads_flag: bool,
    pub enable_enquiry_customisations: bool,
    pub shortlist_experiment_flag: bool,
    pub enable_suggested_features: bool,
    pub enable_additional_description_feature: bool,
    pub enable_vertical_gallery: bool,
    pub enable_retirements: bool,
    pub enable_property_next_steps: bool,
    pub enable_property_next_steps_backend: bool,
    pub enable_add_notes: bool,
    pub text_ads_enabled: bool,
    // ... continue adding all flags as bools
}

// ---- Hero Images ----
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HeroImages {
    pub small: String,
    pub medium: String,
    pub large: String,
    pub x_large: String,
}

// ---- Component Props ----
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ComponentProps {
    pub theme: String,
    pub is_map_view: bool,
    pub base_url: String,
    pub signup_path: String,
    pub logout_path: String,
    pub login_path: String,
    pub mode: String,
    pub product_segment: String,
    pub no_ads: bool,
    pub lazy_load_images: bool,
    pub estimated_device_width: String,
    pub estimated_device_height: String,
    pub map_view_url: String,
    pub breadcrumbs: Vec<Breadcrumb>,
    pub current_page: u32,
    pub total_pages: u32,
    pub paginator_template_url: String,
    pub listing_search_result_ids: Vec<u64>,
    pub listings_map: HashMap<u64, Listing>, // <-- reuse your Listing struct here
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Breadcrumb {
    pub title: String,
    pub url: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Listing {
    pub id: u64,
    pub listing_type: String,
    pub listing_model: ListingModel,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListingModel {
    pub promo_type: String,
    pub url: String,
    pub images: Vec<String>,
    pub skeleton_images: Option<Vec<SkeletonImage>>,
    pub branding_appearance: String,
    pub price: String,
    pub has_video: bool,
    pub branding: Branding,
    pub address: Address,
    pub features: Features,
    pub inspection: Option<Inspection>,
    pub auction: Option<String>,
    pub tags: Tags,
    pub display_search_price_range: Option<String>,
    pub enable_single_line_address: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SkeletonImage {
    pub images: ImageUrls,
    pub media_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ImageUrls {
    pub original: ImageSize,
    pub tablet: ImageSize,
    pub mobile: ImageSize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ImageSize {
    pub url: String,
    pub width: u32,
    pub height: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Branding {
    pub agency_id: u64,
    pub agents: Vec<Agent>,
    pub agent_names: String,
    pub brand_logo: String,
    pub skeleton_brand_logo: String,
    pub brand_name: String,
    pub brand_color: String,
    pub agent_photo: Option<String>,
    pub agent_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Agent {
    pub agent_name: String,
    pub agent_photo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Address {
    pub street: String,
    pub suburb: String,
    pub state: String,
    pub postcode: String,
    pub lat: f64,
    pub lng: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Features {
    pub beds: u32,
    pub baths: u32,
    pub parking: u32,
    pub property_type: String,
    pub property_type_formatted: String,
    pub is_rural: bool,
    pub land_size: f64,
    pub land_unit: String,
    pub is_retirement: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Inspection {
    pub open_time: Option<String>,
    pub close_time: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Tags {
    pub tag_text: String,
    pub tag_class_name: String,
}
