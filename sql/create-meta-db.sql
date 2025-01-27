create table if not exists cache_metadata (
    id TEXT unique,
    download_checksum TEXT unique,
    timestamp TEXT,
    response TEXT,
    download_args TEXT,
    template TEXT,
    downloaded_files TEXT,
    processed_files TEXT,
    extra_key TEXT,
    processing_errors TEXT
);

create table if not exists download_trials (
    cache_id TEXT,
    timestamp TEXT,
    downloaded TEXT
);