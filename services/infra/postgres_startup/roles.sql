CREATE ROLE appsmith WITH LOGIN;
ALTER ROLE appsmith WITH PASSWORD 'appsmith';
GRANT CONNECT ON DATABASE personal_finances TO appsmith;
GRANT ALL ON ALL TABLES IN SCHEMA "public" TO appsmith;