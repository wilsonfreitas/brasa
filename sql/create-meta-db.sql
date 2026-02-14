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
    processing_errors TEXT,
    is_invalid_download TEXT,
    invalid_download_reason TEXT
);

create table if not exists download_trials (
    cache_id TEXT,
    timestamp TEXT,
    downloaded TEXT
);
