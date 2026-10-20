--liquibase formatted sql

--changeset charan:005-create-file-format
CREATE FILE FORMAT IF NOT EXISTS raw.csv_with_header
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE;
--rollback DROP FILE FORMAT IF EXISTS raw.csv_with_header;

--changeset charan:006-create-s3-stage
-- Requires a storage integration created by an account admin (see README).
CREATE STAGE IF NOT EXISTS raw.tca_s3_stage
    URL = 's3://${s3_bucket}/tca/raw/'
    STORAGE_INTEGRATION = ${storage_integration}
    FILE_FORMAT = raw.csv_with_header;
--rollback DROP STAGE IF EXISTS raw.tca_s3_stage;
