-- Dataset catalog table for tracking dataset metadata
-- This table stores schema information independently from templates
-- enabling proper schema retrieval for datasets in all layers.

create table if not exists dataset_catalog (
    id TEXT PRIMARY KEY,              -- Unique identifier: layer/dataset_name
    layer TEXT NOT NULL,              -- Data layer: input, staging, curated
    dataset_name TEXT NOT NULL,       -- Name of the dataset
    schema_json TEXT NOT NULL,        -- PyArrow schema serialized as JSON
    partitioning TEXT,                -- Comma-separated partition column names
    source_template TEXT,             -- Source template ID (if applicable)
    created_at TEXT NOT NULL,         -- ISO format timestamp of creation
    updated_at TEXT NOT NULL,         -- ISO format timestamp of last update
    UNIQUE(layer, dataset_name)
);

-- Index for faster lookups by layer
create index if not exists idx_dataset_catalog_layer on dataset_catalog(layer);

-- Index for faster lookups by source template
create index if not exists idx_dataset_catalog_template on dataset_catalog(source_template);
