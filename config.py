from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    base_url: str = "https://www.reelshort.com"
    csv_path: str = "./reelshort_complete.csv"
    proxies_file: str = "./proxies.txt"
    concurrency: int = 10
    proxy_rotate_every: int = 50


settings = Settings()
