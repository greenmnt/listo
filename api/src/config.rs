use std::env;

pub struct Config {
    pub database_url: String,
    pub bind_addr: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        // Match the Python scraper's connection string conventions but accept
        // either DATABASE_URL or LISTO_DATABASE_URL — that's the env var the
        // Python settings layer reads. Strip the SQLAlchemy-only "+pymysql"
        // suffix if present so sqlx can parse it.
        let raw = env::var("DATABASE_URL")
            .or_else(|_| env::var("LISTO_DATABASE_URL"))
            .unwrap_or_else(|_| "mysql://listo:password@localhost/listo".into());
        let database_url = raw.replace("mysql+pymysql://", "mysql://");

        let bind_addr = env::var("LISTO_API_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".into());

        Ok(Self { database_url, bind_addr })
    }
}
