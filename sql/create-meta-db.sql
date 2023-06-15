create table if not exists cache_metadata (
    download_checksum TEXT unique,
    timestamp TEXT,
    response TEXT,
    download_args TEXT,
    template TEXT,
    downloaded_files TEXT,
    processed_files TEXT,
    extra_key TEXT
);