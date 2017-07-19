CREATE TABLE sqlite_instances (
    instance_id BIGINT PRIMARY KEY,
    variable TEXT,
    mip TEXT,
    model TEXT,
    experiment TEXT,
    ensemble TEXT,
    realm TEXT
);

CREATE TABLE sqlite_versions (
     version_id BIGINT PRIMARY KEY,
     path TEXT,
     version TEXT,
     dataset_id TEXT,
     is_latest BOOLEAN,
     checked_on TEXT,
     to_update BOOLEAN,
     instance_id BIGINT REFERENCES sqlite_instances(instance_id)
);

CREATE TABLE sqlite_files (
    file_id BIGINT PRIMARY KEY,
    filename TEXT,
    tracking_id TEXT,
    md5 TEXT,
    sha256 TEXT,
    version_id BIGINT REFERENCES sqlite_versions(version_id)
);

CREATE TABLE sqlite_warnings (
     warning_id BIGINT PRIMARY KEY,
     warning TEXT,
     added_by TEXT,
     added_on TEXT,
     version_id BIGINT REFERENCES sqlite_versions(version_id)
);
