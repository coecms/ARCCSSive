CREATE TABLE IF NOT EXISTS sqlite_instances (
    instance_id BIGINT PRIMARY KEY,
    variable TEXT,
    mip TEXT,
    model TEXT,
    experiment TEXT,
    ensemble TEXT,
    realm TEXT
);

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

CREATE TABLE IF NOT EXISTS sqlite_warnings (
     warning_id BIGINT PRIMARY KEY,
     warning TEXT,
     added_by TEXT,
     added_on TEXT,
     version_id BIGINT REFERENCES sqlite_versions(version_id)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS sqlite_paths_link AS
    SELECT
        md5(path || '/' || filename)::uuid AS hash
      , file_id
    FROM
        sqlite_files
    JOIN 
        sqlite_versions ON sqlite_files.version_id = sqlite_versions.version_id;
CREATE INDEX IF NOT EXISTS sqlite_paths_link_hash_idx ON sqlite_paths_link(hash);
CREATE INDEX IF NOT EXISTS sqlite_paths_link_file_id_idx ON sqlite_paths_link(file_id);
