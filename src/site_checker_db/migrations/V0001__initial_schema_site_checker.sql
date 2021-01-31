CREATE TABLE check_site_info (
    id BIGSERIAL PRIMARY KEY,
    site_name VARCHAR(50) NOT NULL,
    url VARCHAR(255) NOT NULL,
    data VARCHAR(255),
    http_method VARCHAR(20) NOT NULL DEFAULT 'GET',
    regexp VARCHAR(127),
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP NOT NULL DEFAULT NOW()
);


CREATE TABLE check_result (
    id BIGSERIAL NOT NULL,
    -- ok | error
    check_status varchar(20) NOT NULL,
    -- contain error type in case of failed check
    check_error_code varchar(50),
    site_info_id BIGINT NOT NULL,
    latency_time DOUBLE PRECISION,
    response_time DOUBLE PRECISION,
    http_status SMALLINT,
    check_timestamp TIMESTAMP NOT NULL,
    regexp_check boolean,
    created TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (site_info_id, created),

    CONSTRAINT site_info_id_fk
      FOREIGN KEY(site_info_id) 
	  REFERENCES check_site_info(id)
) 
PARTITION BY RANGE (created);

-- insert initial couple of entries
INSERT INTO check_site_info (site_name, url) VALUES ('opennet home', 'http://opennet.ru');
INSERT INTO Check_site_info (site_name, url, regexp) VALUES ('google home', 'http://google.com', '<html.*');


INSERT INTO ops (op) VALUES ('migration V0001__initial_schema_site_checker.sql');

-- prepare initial partitions
-- NOTE: for production readiness need to develop some tools for managing partitions or
-- use PostgreSQL extensions like https://github.com/postgrespro/pg_pathman
CREATE TABLE check_result_y2021m01 PARTITION OF check_result
    FOR VALUES FROM ('2021-01-01') TO ('2021-02-01');
CREATE TABLE check_result_y2021m02 PARTITION OF check_result
    FOR VALUES FROM ('2021-02-01') TO ('2021-03-01');
CREATE TABLE check_result_y2021m03 PARTITION OF check_result
    FOR VALUES FROM ('2021-03-01') TO ('2021-05-01');
