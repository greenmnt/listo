use reqwest::blocking::get;
use scraper::{Html, Selector};
use regex::Regex;
use serde_json::Value;

//fn extract_urql_cache(html: &str) -> Option<String> {
    //let document = Html::parse_document(html);
    //let selector = Selector::parse("script").unwrap();

    //let re = Regex::new(r#""urqlClientCache"\s*:\s*"([^"]+)""#).unwrap();

    //for script in document.select(&selector) {
        //let script_text = script.text().collect::<String>();
        //if script_text.contains("urqlClientCache") {
            //if let Some(caps) = re.captures(&script_text) {
                //println!("{:#?}", caps);
                //return Some(caps[1].to_string());
            //}
        //}
    //}
    //None
//}

fn extract_argonaut_exchange(script: &str) -> Option<String> {
    let start = script.find("window.ArgonautExchange")?;
    let after_eq = script[start..].find('{')? + start;

    let mut depth = 0;
    for (i, c) in script[after_eq..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let end = after_eq + i + 1;
                    return Some(script[after_eq..end].to_string());
                }
            }
            _ => {}
        }
    }
    None
}


fn extract_urql_cache(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("script").unwrap();

    for script in document.select(&selector) {
        let script_text = script.text().collect::<String>();

        if script_text.contains("window.ArgonautExchange") {
            let json_text = extract_argonaut_exchange(&script_text)?;
            let parsed: Value = serde_json::from_str(&json_text).ok()?;

            return parsed
                .get("resi-property_listing-experience-web")?
                .get("urqlClientCache")?
                .as_str()
                .map(|s| s.to_string());
        }
    }
    None
}


fn parse_stringified_json(s: &str) -> Value {
    println!("{:#?}", s);
    if let Ok(v) = serde_json::from_str::<Value>(s) {
        return v;
    }
    // Turn escaped string into valid JSON string
    let wrapped = format!("\"{}\"", s);
    println!("{:#?}", wrapped);
    let unescaped: String = serde_json::from_str(&wrapped).unwrap();
    serde_json::from_str(&unescaped).unwrap()
}

fn recursively_parse_json(value: &mut Value) {
    match value {
        Value::String(s) => {
            if s.trim_start().starts_with('{') || s.trim_start().starts_with('[') {
                if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                    *value = parsed;
                    recursively_parse_json(value);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr {
                recursively_parse_json(v);
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                recursively_parse_json(v);
            }
        }
        _ => {}
    }
}

fn write_json_pretty(path: &str, value: &Value) -> std::io::Result<()> {
    use std::io::BufWriter;
    use std::fs::File;
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, value)?;
    Ok(())
}


#[test]
fn test_parse_json() {
    println!("test parse json");
    use std::fs;
    let site = fs::read_to_string("fixtures/realestate").expect("couldnt open file");
    let raw_cache = extract_urql_cache(&site).expect("urqlClientCache not found");
    //println!("rawcache");
    //println!("{:#?}", raw_cache);
    let mut parsed = parse_stringified_json(&raw_cache);
    recursively_parse_json(&mut parsed);
    println!("{:#?}", parsed);
    write_json_pretty("fixtures/realestate.json", &parsed).unwrap();
}

//#[test]
//fn test_parse_json_0() {
    //let s = "{\"5420119257\":{\"hasNext\":false}}";
    //let wrapped = format!("\"{}\"", s);
    //println!("{:#?}", wrapped);
    //let unescaped: String = serde_json::from_str(&wrapped).unwrap();
    //println!("{:#?}", unescaped);
    //let x: Value = serde_json::from_str(&unescaped).unwrap();
    //println!("{:#?}", x);
//}
