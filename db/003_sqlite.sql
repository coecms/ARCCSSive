CREATE TABLE IF NOT EXISTS sqlite_instances (
    instance_id BIGINT PRIMARY KEY,
    variable TEXT,
    mip TEXT,
    model TEXT,
    experiment TEXT,
    ensemble TEXT,
    realm TEXT
);
GRANT SELECT ON sqlite_instances TO PUBLIC;

CREATE TABLE IF NOT EXISTS sqlite_versions (
     version_id BIGINT PRIMARY KEY,
     path TEXT,
     version TEXT,
     dataset_id TEXT,
     is_latest BOOLEAN,
     checked_on TEXT,
     to_update BOOLEAN,
     instance_id BIGINT REFERENCES sqlite_instances(instance_id)
);
GRANT SELECT ON sqlite_versions TO PUBLIC;

CREATE TABLE IF NOT EXISTS sqlite_files (
    file_id BIGINT PRIMARY KEY,
    filename TEXT,
    tracking_id TEXT,
    md5 TEXT,
    sha256 TEXT,
    version_id BIGINT REFERENCES sqlite_versions(version_id)
);
CREATE INDEX IF NOT EXISTS sqlite_files_file_id_idx ON sqlite_files(file_id);
CREATE INDEX IF NOT EXISTS sqlite_files_version_id_idx ON sqlite_files(version_id);
GRANT SELECT ON sqlite_files TO PUBLIC;

CREATE TABLE IF NOT EXISTS sqlite_warnings (
     warning_id BIGINT PRIMARY KEY,
     warning TEXT,
     added_by TEXT,
     added_on TEXT,
     version_id BIGINT REFERENCES sqlite_versions(version_id)
);
GRANT SELECT ON sqlite_warnings TO PUBLIC;

CREATE MATERIALIZED VIEW IF NOT EXISTS sqlite_metadata_link AS
    SELECT 
        file_id,
        version_id,
        md_hash
    FROM sqlite_files
    JOIN metadata
    ON md_json->>'sha256' = sha256 
    AND md_type='checksum';
CREATE INDEX IF NOT EXISTS sqlite_metadata_link_file_id_idx ON sqlite_metadata_link(file_id);
CREATE INDEX IF NOT EXISTS sqlite_metadata_link_md_hash_idx ON sqlite_metadata_link(md_hash);
GRANT SELECT ON sqlite_metadata_link TO PUBLIC;
