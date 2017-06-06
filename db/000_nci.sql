/*
 * Database structure for testing
 *
 * In production these tables come from a remote database
 */


CREATE TYPE path_type ENUM ('file', 'directory', 'link');

CREATE TABLE metadata (
    md_hash UUID PRIMARY KEY,
    md_ingested TIMESTAMP WITH TIME ZONE,
    md_type TEXT,
    md_json JSONB
);

CREATE TABLE paths (
    pa_hash UUID PRIMARY KEY,
    pa_ingested TIMESTAMP WITH TIME ZONE,
    pa_type PATH_TYPE,
    pa_path TEXT,
    pa_parents UUID[]
);

-- Read sample data
\copy metadata from 'db/metadata_sample.txt'
\copy paths    from 'db/paths_sample.txt'
