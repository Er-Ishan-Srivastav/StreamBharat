CREATE DATABASE metabase;
CREATE USER metabase WITH PASSWORD 'metabase_local_dev';
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;
