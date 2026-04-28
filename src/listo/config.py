from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


_DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
]


class Settings(BaseSettings):
    database_url: str = "mysql+pymysql://listo:password@localhost/listo?charset=utf8mb4"
    request_min_delay: float = 3.0
    request_max_delay: float = 8.0
    user_agents: list[str] = _DEFAULT_USER_AGENTS
    fetch_timeout: int = 30
    dedup_window_hours: int = 24

    model_config = SettingsConfigDict(env_file=".env", env_prefix="LISTO_", extra="ignore")


settings = Settings()
