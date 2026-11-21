--
-- PostgreSQL database dump
--


-- Dumped from database version 14.24 (Homebrew)
-- Dumped by pg_dump version 14.24 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ducklake_column; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column (
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    table_id bigint,
    column_order bigint,
    column_name character varying,
    column_type character varying,
    initial_default character varying,
    default_value character varying,
    nulls_allowed boolean,
    parent_column bigint,
    default_value_type character varying,
    default_value_dialect character varying
);


--
-- Name: ducklake_column_mapping; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column_mapping (
    mapping_id bigint,
    table_id bigint,
    type character varying
);


--
-- Name: ducklake_column_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column_tag (
    table_id bigint,
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


--
-- Name: ducklake_data_file; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_data_file (
    data_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    file_order bigint,
    path character varying,
    path_is_relative boolean,
    file_format character varying,
    record_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    row_id_start bigint,
    partition_id bigint,
    encryption_key character varying,
    mapping_id bigint,
    partial_max bigint
);


--
-- Name: ducklake_delete_file; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_delete_file (
    delete_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    format character varying,
    delete_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    encryption_key character varying,
    partial_max bigint
);


--
-- Name: ducklake_file_column_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_column_stats (
    data_file_id bigint,
    table_id bigint,
    column_id bigint,
    column_size_bytes bigint,
    value_count bigint,
    null_count bigint,
    min_value character varying,
    max_value character varying,
    contains_nan boolean,
    extra_stats character varying
);


--
-- Name: ducklake_file_partition_value; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_partition_value (
    data_file_id bigint,
    table_id bigint,
    partition_key_index bigint,
    partition_value character varying
);


--
-- Name: ducklake_file_variant_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_variant_stats (
    data_file_id bigint,
    table_id bigint,
    column_id bigint,
    variant_path character varying,
    shredded_type character varying,
    column_size_bytes bigint,
    value_count bigint,
    null_count bigint,
    min_value character varying,
    max_value character varying,
    contains_nan boolean,
    extra_stats character varying
);


--
-- Name: ducklake_files_scheduled_for_deletion; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_files_scheduled_for_deletion (
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    schedule_start timestamp with time zone
);


--
-- Name: ducklake_inlined_data_21_21; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_21_21 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    val bytea
);


--
-- Name: ducklake_inlined_data_tables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_tables (
    table_id bigint,
    table_name character varying,
    schema_version bigint
);


--
-- Name: ducklake_macro; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro (
    schema_id bigint,
    macro_id bigint,
    macro_name character varying,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_macro_impl; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro_impl (
    macro_id bigint,
    impl_id bigint,
    dialect character varying,
    sql character varying,
    type character varying
);


--
-- Name: ducklake_macro_parameters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro_parameters (
    macro_id bigint,
    impl_id bigint,
    column_id bigint,
    parameter_name character varying,
    parameter_type character varying,
    default_value character varying,
    default_value_type character varying
);


--
-- Name: ducklake_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_metadata (
    key character varying NOT NULL,
    value character varying NOT NULL,
    scope character varying,
    scope_id bigint
);


--
-- Name: ducklake_name_mapping; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_name_mapping (
    mapping_id bigint,
    column_id bigint,
    source_name character varying,
    target_field_id bigint,
    parent_column bigint,
    is_partition boolean
);


--
-- Name: ducklake_partition_column; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_partition_column (
    partition_id bigint,
    table_id bigint,
    partition_key_index bigint,
    column_id bigint,
    transform character varying
);


--
-- Name: ducklake_partition_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_partition_info (
    partition_id bigint,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_schema; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_schema (
    schema_id bigint NOT NULL,
    schema_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_name character varying,
    path character varying,
    path_is_relative boolean
);


--
-- Name: ducklake_schema_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_schema_versions (
    begin_snapshot bigint,
    schema_version bigint,
    table_id bigint
);


--
-- Name: ducklake_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_snapshot (
    snapshot_id bigint NOT NULL,
    snapshot_time timestamp with time zone,
    schema_version bigint,
    next_catalog_id bigint,
    next_file_id bigint
);


--
-- Name: ducklake_snapshot_changes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_snapshot_changes (
    snapshot_id bigint NOT NULL,
    changes_made character varying,
    author character varying,
    commit_message character varying,
    commit_extra_info character varying
);


--
-- Name: ducklake_sort_expression; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_sort_expression (
    sort_id bigint,
    table_id bigint,
    sort_key_index bigint,
    expression character varying,
    dialect character varying,
    sort_direction character varying,
    null_order character varying
);


--
-- Name: ducklake_sort_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_sort_info (
    sort_id bigint,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_table; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table (
    table_id bigint,
    table_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    table_name character varying,
    path character varying,
    path_is_relative boolean
);


--
-- Name: ducklake_table_column_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table_column_stats (
    table_id bigint,
    column_id bigint,
    contains_null boolean,
    contains_nan boolean,
    min_value character varying,
    max_value character varying,
    extra_stats character varying
);


--
-- Name: ducklake_table_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table_stats (
    table_id bigint,
    record_count bigint,
    next_row_id bigint,
    file_size_bytes bigint
);


--
-- Name: ducklake_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_tag (
    object_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


--
-- Name: ducklake_view; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_view (
    view_id bigint,
    view_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    view_name character varying,
    dialect character varying,
    sql character varying,
    column_aliases character varying
);


--
-- Data for Name: ducklake_column; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_column VALUES (1, 2, NULL, 2, 1, 'c_custkey', 'int64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 2, NULL, 2, 2, 'c_name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 2, NULL, 2, 3, 'c_address', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (4, 2, NULL, 2, 4, 'c_nationkey', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (5, 2, NULL, 2, 5, 'c_phone', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (6, 2, NULL, 2, 6, 'c_acctbal', 'float64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (7, 2, NULL, 2, 7, 'c_mktsegment', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (8, 2, NULL, 2, 8, 'c_comment', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 2, NULL, 3, 1, 'o_orderkey', 'int64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 2, NULL, 3, 2, 'o_custkey', 'int64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 2, NULL, 3, 3, 'o_orderstatus', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (4, 2, NULL, 3, 4, 'o_totalprice', 'float64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (5, 2, NULL, 3, 5, 'o_orderdate', 'date', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (6, 2, NULL, 3, 6, 'o_orderpriority', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (7, 2, NULL, 3, 7, 'o_clerk', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (8, 2, NULL, 3, 8, 'o_shippriority', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (9, 2, NULL, 3, 9, 'o_comment', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 2, NULL, 4, 1, 'r_regionkey', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 2, NULL, 4, 2, 'r_name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 2, NULL, 4, 3, 'r_comment', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 2, NULL, 5, 1, 'n_nationkey', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 2, NULL, 5, 2, 'n_name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 2, NULL, 5, 3, 'n_regionkey', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (4, 2, NULL, 5, 4, 'n_comment', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 4, NULL, 7, 1, 'id', 'int64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 4, NULL, 7, 2, 'bool_col', 'boolean', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 4, NULL, 7, 3, 'tinyint_col', 'int8', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (4, 4, NULL, 7, 4, 'smallint_col', 'int16', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (5, 4, NULL, 7, 5, 'int_col', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (6, 4, NULL, 7, 6, 'bigint_col', 'int64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (7, 4, NULL, 7, 7, 'float_col', 'float32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (8, 4, NULL, 7, 8, 'double_col', 'float64', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (9, 4, NULL, 7, 9, 'decimal_col', 'decimal(18,3)', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (10, 4, NULL, 7, 10, 'varchar_col', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (11, 4, NULL, 7, 11, 'date_col', 'date', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (12, 4, NULL, 7, 12, 'time_col', 'time', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (13, 4, NULL, 7, 13, 'timestamp_col', 'timestamp', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (14, 4, NULL, 7, 14, 'timestamp_s_col', 'timestamp_s', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (15, 4, NULL, 7, 15, 'timestamp_ms_col', 'timestamp_ms', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (16, 4, NULL, 7, 16, 'timestamp_ns_col', 'timestamp_ns', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (17, 4, NULL, 7, 17, 'timestamptz_col', 'timestamptz', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (18, 4, NULL, 7, 18, 'uuid_col', 'uuid', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (19, 4, NULL, 7, 19, 'json_col', 'json', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (20, 4, NULL, 7, 20, 'blob_col', 'blob', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 7, NULL, 8, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 7, NULL, 8, 2, 'list_col', 'list', NULL, 'NULL', true, NULL, '', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 7, NULL, 8, 3, 'element', 'int32', NULL, 'NULL', true, 2, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (4, 7, NULL, 8, 4, 'struct_col', 'struct', NULL, 'NULL', true, NULL, '', 'duckdb');
INSERT INTO public.ducklake_column VALUES (5, 7, NULL, 8, 5, 'a', 'int32', NULL, 'NULL', true, 4, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (6, 7, NULL, 8, 6, 'b', 'varchar', NULL, 'NULL', true, 4, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (7, 7, NULL, 8, 7, 'map_col', 'map', NULL, 'NULL', true, NULL, '', 'duckdb');
INSERT INTO public.ducklake_column VALUES (8, 7, NULL, 8, 8, 'key', 'varchar', NULL, 'NULL', true, 7, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (9, 7, NULL, 8, 9, 'value', 'int32', NULL, 'NULL', true, 7, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (10, 7, NULL, 8, 10, 'struct_with_list', 'struct', NULL, 'NULL', true, NULL, '', 'duckdb');
INSERT INTO public.ducklake_column VALUES (11, 7, NULL, 8, 11, 'name', 'varchar', NULL, 'NULL', true, 10, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (12, 7, NULL, 8, 12, 'tags', 'list', NULL, 'NULL', true, 10, '', 'duckdb');
INSERT INTO public.ducklake_column VALUES (13, 7, NULL, 8, 13, 'element', 'varchar', NULL, 'NULL', true, 12, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 9, NULL, 9, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 9, NULL, 9, 2, 'interval_col', 'interval', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 12, NULL, 11, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 12, NULL, 11, 2, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 15, NULL, 13, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 15, NULL, 13, 2, 'ts', 'timestamp', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 15, NULL, 13, 3, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 19, NULL, 16, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 19, NULL, 16, 2, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 22, NULL, 17, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 22, NULL, 17, 2, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 27, NULL, 19, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (3, 29, NULL, 19, 3, 'score', 'int32', '42', '42', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 27, 30, 19, 2, 'name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 30, NULL, 19, 2, 'full_name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 33, NULL, 21, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 33, NULL, 21, 2, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (1, 39, NULL, 23, 1, 'id', 'int32', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
INSERT INTO public.ducklake_column VALUES (2, 39, NULL, 23, 2, 'val', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');


--
-- Data for Name: ducklake_column_mapping; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_column_tag; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_data_file; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_data_file VALUES (0, 5, 2, NULL, NULL, 'ducklake-01a0880c-56ae-7f76-bf90-05ccec597dab.parquet', true, 'parquet', 25, 2338, 733, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (1, 4, 2, NULL, NULL, 'ducklake-01a0880c-56af-789e-a83a-bac75ca44385.parquet', true, 'parquet', 5, 1083, 624, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (2, 2, 2, NULL, NULL, 'ducklake-01a0880c-56b0-70cc-ae42-41a2c77f0138.parquet', true, 'parquet', 1500, 126839, 1395, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (3, 3, 2, NULL, NULL, 'ducklake-01a0880c-56b2-7db0-b6a6-604ddc15d60c.parquet', true, 'parquet', 15000, 542225, 1272, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (4, 7, 5, NULL, NULL, 'ducklake-01a0880c-56ee-7353-8b23-552d794fe91c.parquet', true, 'parquet', 1, 2941, 2254, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (5, 7, 6, NULL, NULL, 'ducklake-01a0880c-56f2-71e5-af67-d7b2385ac9ab.parquet', true, 'parquet', 1, 1964, 1444, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (6, 8, 8, NULL, NULL, 'ducklake-01a0880c-5709-7ebf-a44b-b222ebce9100.parquet', true, 'parquet', 3, 1311, 924, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (7, 9, 10, NULL, NULL, 'ducklake-01a0880c-571e-7a72-8cc5-01197ea99a76.parquet', true, 'parquet', 1, 296, 218, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (8, 11, 14, NULL, NULL, 'id=0/ducklake-01a0880c-574b-7b12-8797-c1636e2c4648.parquet', true, 'parquet', 10, 429, 245, 0, 12, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (9, 11, 14, NULL, NULL, 'id=1/ducklake-01a0880c-574b-773b-9e0f-de0d10198376.parquet', true, 'parquet', 10, 428, 245, 10, 12, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (10, 11, 14, NULL, NULL, 'id=2/ducklake-01a0880c-574b-7499-bd82-cd9446db5f77.parquet', true, 'parquet', 10, 431, 247, 20, 12, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (11, 11, 14, NULL, NULL, 'id=3/ducklake-01a0880c-574b-7ec7-8383-79a96d6cbb43.parquet', true, 'parquet', 10, 434, 247, 30, 12, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (12, 11, 14, NULL, NULL, 'id=4/ducklake-01a0880c-574c-7a5a-9bcd-4133a04dfcd3.parquet', true, 'parquet', 10, 431, 247, 40, 12, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (13, 13, 17, NULL, NULL, 'month=1/year=2024/ducklake-01a0880c-5771-7a00-a6e4-cab24f04d4f4.parquet', true, 'parquet', 31, 917, 338, 0, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (14, 13, 17, NULL, NULL, 'month=2/year=2024/ducklake-01a0880c-5771-7bd3-a16b-e28da1e20bb0.parquet', true, 'parquet', 29, 890, 342, 31, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (15, 13, 17, NULL, NULL, 'month=3/year=2024/ducklake-01a0880c-5771-7b69-953e-bc8dca342810.parquet', true, 'parquet', 31, 921, 342, 60, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (16, 13, 17, NULL, NULL, 'month=4/year=2024/ducklake-01a0880c-5771-73ea-8624-fb847be026d5.parquet', true, 'parquet', 30, 911, 344, 91, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (17, 13, 17, NULL, NULL, 'month=5/year=2024/ducklake-01a0880c-5772-75c1-81dd-c2948131e89e.parquet', true, 'parquet', 31, 927, 346, 121, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (18, 13, 17, NULL, NULL, 'month=6/year=2024/ducklake-01a0880c-5772-7cd0-8e9e-4a42e57a78f3.parquet', true, 'parquet', 30, 911, 346, 152, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (19, 13, 17, NULL, NULL, 'month=7/year=2024/ducklake-01a0880c-5772-79ce-974b-a2b89ecd1b6d.parquet', true, 'parquet', 31, 937, 346, 182, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (20, 13, 17, NULL, NULL, 'month=8/year=2024/ducklake-01a0880c-5772-7af8-98ec-15e8827856b7.parquet', true, 'parquet', 31, 928, 346, 213, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (21, 13, 17, NULL, NULL, 'month=9/year=2024/ducklake-01a0880c-5773-7db7-8cdc-77633f8642ed.parquet', true, 'parquet', 30, 911, 346, 244, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (22, 13, 17, NULL, NULL, 'month=10/year=2024/ducklake-01a0880c-5773-7822-83c0-0ec55b53b3fd.parquet', true, 'parquet', 31, 931, 346, 274, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (23, 13, 17, NULL, NULL, 'month=11/year=2024/ducklake-01a0880c-5773-7aeb-b338-817a8125df4a.parquet', true, 'parquet', 30, 911, 346, 305, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (24, 13, 17, NULL, NULL, 'month=12/year=2024/ducklake-01a0880c-5773-7467-b8ff-cbfd69cc0c67.parquet', true, 'parquet', 31, 929, 346, 335, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (25, 13, 17, NULL, NULL, 'month=1/year=2025/ducklake-01a0880c-5774-7931-93ff-038dcd6a0066.parquet', true, 'parquet', 31, 927, 346, 366, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (26, 13, 17, NULL, NULL, 'month=2/year=2025/ducklake-01a0880c-5774-7d41-9cc8-11dd3e82d225.parquet', true, 'parquet', 3, 488, 339, 397, 14, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (27, 16, 20, NULL, NULL, 'ducklake-01a0880c-5798-7356-bb0c-1dcc90afd1fc.parquet', true, 'parquet', 1000, 8361, 249, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (29, 17, 23, NULL, NULL, 'ducklake-01a0880c-57b3-7b10-8642-3344ecead8a6.parquet', true, 'parquet', 1000, 8361, 249, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (32, 19, 28, NULL, NULL, 'ducklake-01a0880c-57de-77c2-9e8d-5329bc84fe02.parquet', true, 'parquet', 2, 318, 232, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (33, 19, 31, NULL, NULL, 'ducklake-01a0880c-5801-79d3-a09a-014309d23ad6.parquet', true, 'parquet', 2, 442, 322, 2, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (37, 21, 37, NULL, NULL, 'ducklake-01a0880c-5835-7d3b-90dc-0277ad0b9a1f.parquet', true, 'parquet', 20, 483, 243, 6, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_data_file VALUES (43, 23, 40, NULL, NULL, 'ducklake-01a0880c-5866-7bc2-9ebb-1849ce987256.parquet', true, 'parquet', 5, 526, 366, 0, NULL, NULL, NULL, 44);


--
-- Data for Name: ducklake_delete_file; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_delete_file VALUES (28, 16, 21, NULL, 27, 'ducklake-01a0880c-57a0-732d-9804-f23baaab9e74-delete.parquet', true, 'parquet', 100, 1546, 863, NULL, NULL);
INSERT INTO public.ducklake_delete_file VALUES (31, 17, 24, NULL, 29, 'ducklake-01a0880c-57c2-753e-9189-00f2b39077c9-delete.parquet', true, 'parquet', 200, 2285, 1051, NULL, 25);


--
-- Data for Name: ducklake_file_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_file_column_stats VALUES (0, 5, 1, 125, 25, 0, '0', '24', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (0, 5, 2, 273, 25, 0, 'ALGERIA', 'VIETNAM', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (0, 5, 3, 80, 25, 0, '0', '4', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (0, 5, 4, 1068, 25, 0, ' beans after the carefully regular accounts r', 'usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily ', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (1, 4, 1, 44, 5, 0, '0', '4', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (1, 4, 2, 75, 5, 0, 'AFRICA', 'MIDDLE EAST', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (1, 4, 3, 328, 5, 0, ' foxes boost furiously along the carefully dogged tithes. slyly regular orbits according to the special epit', 's are. furiously even pinto bea', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 1, 6415, 1500, 0, '1', '1500', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 2, 7583, 1500, 0, 'Customer#000000001', 'Customer#000001500', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 3, 42084, 1500, 0, '  dcVkxZ,s,9xW ab60aC3slv3STVRwA1', 'zwrDoaY2gxCkdTXFaxNc', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 4, 1115, 1500, 0, '0', '24', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 5, 18413, 1500, 0, '10-109-430-5638', '34-992-529-2023', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 6, 8464, 1500, 0, '-994.79', '9987.71', false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 7, 696, 1500, 0, 'AUTOMOBILE', 'MACHINERY', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (2, 2, 8, 40568, 1500, 0, ' about the carefully final pinto beans. quickly regular sheaves are slyly final, f', 'ys among the accounts affix blithely asymptotes. carefully even deposits are fluff', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 1, 60043, 15000, 0, '1', '60000', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 2, 22997, 15000, 0, '1', '1499', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 3, 3886, 15000, 0, 'F', 'P', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 4, 88374, 15000, 0, '874.89', '466001.28', false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 5, 32380, 15000, 0, '1992-01-01', '1998-08-02', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 6, 5841, 15000, 0, '1-URGENT', '5-LOW', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 7, 23934, 15000, 0, 'Clerk#000000001', 'Clerk#000001000', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 8, 53, 15000, 0, '0', '0', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (3, 3, 9, 295052, 15000, 0, ' Tiresias. carefully spec', 'zzle: slyly even ideas wake furiously across the ironic p', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 1, 33, 1, 0, '1', '1', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 2, 26, 1, 0, '1', '1', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 3, 29, 1, 0, '12', '12', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 4, 29, 1, 0, '1234', '1234', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 5, 29, 1, 0, '123456', '123456', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 6, 33, 1, 0, '1234567890123', '1234567890123', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 7, 29, 1, 0, '3.14', '3.14', false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 8, 33, 1, 0, '2.718281828', '2.718281828', false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 9, 33, 1, 0, '12345.678', '12345.678', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 10, 40, 1, 0, 'hello world', 'hello world', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 11, 29, 1, 0, '2024-06-15', '2024-06-15', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 12, 33, 1, 0, '13:45:30', '13:45:30', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 13, 33, 1, 0, '2024-06-15 13:45:30.123456', '2024-06-15 13:45:30.123456', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 14, 33, 1, 0, '2024-06-15 13:45:30', '2024-06-15 13:45:30', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 15, 33, 1, 0, '2024-06-15 13:45:30.123', '2024-06-15 13:45:30.123', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 16, 33, 1, 0, '2024-06-15 13:45:30.123456789', '2024-06-15 13:45:30.123456789', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 17, 33, 1, 0, '2024-06-15 13:45:30.123456+00', '2024-06-15 13:45:30.123456+00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 18, 41, 1, 0, '550e8400-e29b-41d4-a716-446655440000', '550e8400-e29b-41d4-a716-446655440000', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 19, 53, 1, 0, '{"key": "value", "n": 1}', '{"key": "value", "n": 1}', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (4, 7, 20, 40, 1, 0, '48656C6C6F20576F726C64', '48656C6C6F20576F726C64', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 1, 33, 1, 0, '2', '2', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 2, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 3, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 4, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 5, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 6, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 7, 25, 0, 1, NULL, NULL, false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 8, 25, 0, 1, NULL, NULL, false, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 9, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 10, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 11, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 12, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 13, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 14, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 15, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 16, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 17, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 18, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 19, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (5, 7, 20, 25, 0, 1, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 1, 37, 3, 0, '1', '3', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 3, 55, 3, 2, '1', '3', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 5, 35, 1, 2, '10', '10', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 6, 38, 1, 2, 'ten', 'ten', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 8, 53, 2, 2, 'x', 'y', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 9, 51, 2, 2, '1', '2', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 11, 50, 2, 1, 'first', 'second', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (6, 8, 13, 56, 2, 2, 'a', 'b', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (7, 9, 1, 29, 1, 0, '1', '1', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (7, 9, 2, 0, 0, 0, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (8, 11, 1, 47, 10, 0, '0', '0', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (8, 11, 2, 78, 10, 0, 'row_0', 'row_5', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (9, 11, 1, 47, 10, 0, '1', '1', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (9, 11, 2, 77, 10, 0, 'row_1', 'row_6', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (10, 11, 1, 47, 10, 0, '2', '2', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (10, 11, 2, 78, 10, 0, 'row_12', 'row_7', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (11, 11, 1, 47, 10, 0, '3', '3', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (11, 11, 2, 81, 10, 0, 'row_13', 'row_8', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (12, 11, 1, 47, 10, 0, '4', '4', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (12, 11, 2, 78, 10, 0, 'row_14', 'row_9', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (13, 13, 1, 150, 31, 0, '0', '30', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (13, 13, 2, 253, 31, 0, '2024-01-01 00:00:00', '2024-01-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (13, 13, 3, 164, 31, 0, 'row_0', 'row_9', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (14, 13, 1, 144, 29, 0, '31', '59', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (14, 13, 2, 236, 29, 0, '2024-02-01 00:00:00', '2024-02-29 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (14, 13, 3, 156, 29, 0, 'row_31', 'row_59', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (15, 13, 1, 152, 31, 0, '60', '90', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (15, 13, 2, 250, 31, 0, '2024-03-01 00:00:00', '2024-03-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (15, 13, 3, 165, 31, 0, 'row_60', 'row_90', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (16, 13, 1, 148, 30, 0, '91', '120', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (16, 13, 2, 244, 30, 0, '2024-04-01 00:00:00', '2024-04-30 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (16, 13, 3, 163, 30, 0, 'row_100', 'row_99', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (17, 13, 1, 153, 31, 0, '121', '151', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (17, 13, 2, 250, 31, 0, '2024-05-01 00:00:00', '2024-05-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (17, 13, 3, 166, 31, 0, 'row_121', 'row_151', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (18, 13, 1, 148, 30, 0, '152', '181', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (18, 13, 2, 243, 30, 0, '2024-06-01 00:00:00', '2024-06-30 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (18, 13, 3, 162, 30, 0, 'row_152', 'row_181', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (19, 13, 1, 153, 31, 0, '182', '212', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (19, 13, 2, 251, 31, 0, '2024-07-01 00:00:00', '2024-07-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (19, 13, 3, 175, 31, 0, 'row_182', 'row_212', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (20, 13, 1, 153, 31, 0, '213', '243', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (20, 13, 2, 251, 31, 0, '2024-08-01 00:00:00', '2024-08-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (20, 13, 3, 166, 31, 0, 'row_213', 'row_243', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (21, 13, 1, 148, 30, 0, '244', '273', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (21, 13, 2, 243, 30, 0, '2024-09-01 00:00:00', '2024-09-30 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (21, 13, 3, 162, 30, 0, 'row_244', 'row_273', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (22, 13, 1, 153, 31, 0, '274', '304', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (22, 13, 2, 250, 31, 0, '2024-10-01 00:00:00', '2024-10-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (22, 13, 3, 170, 31, 0, 'row_274', 'row_304', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (23, 13, 1, 148, 30, 0, '305', '334', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (23, 13, 2, 243, 30, 0, '2024-11-01 00:00:00', '2024-11-30 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (23, 13, 3, 162, 30, 0, 'row_305', 'row_334', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (24, 13, 1, 153, 31, 0, '335', '365', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (24, 13, 2, 251, 31, 0, '2024-12-01 00:00:00', '2024-12-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (24, 13, 3, 167, 31, 0, 'row_335', 'row_365', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (25, 13, 1, 153, 31, 0, '366', '396', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (25, 13, 2, 250, 31, 0, '2025-01-01 00:00:00', '2025-01-31 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (25, 13, 3, 166, 31, 0, 'row_366', 'row_396', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (26, 13, 1, 37, 3, 0, '397', '399', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (26, 13, 2, 49, 3, 0, '2025-02-01 00:00:00', '2025-02-03 00:00:00', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (26, 13, 3, 51, 3, 0, 'row_397', 'row_399', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (27, 16, 1, 4030, 1000, 0, '0', '999', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (27, 16, 2, 4070, 1000, 0, 'row_0', 'row_999', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (29, 17, 1, 4030, 1000, 0, '0', '999', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (29, 17, 2, 4070, 1000, 0, 'row_0', 'row_999', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (32, 19, 1, 33, 2, 0, '1', '2', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (32, 19, 2, 41, 2, 0, 'alice', 'bob', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (33, 19, 1, 33, 2, 0, '3', '4', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (33, 19, 2, 42, 2, 0, 'carol', 'dave', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (33, 19, 3, 33, 2, 0, '100', '200', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (37, 21, 1, 105, 20, 0, '0', '19', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (37, 21, 2, 123, 20, 0, 'bulk_0', 'bulk_9', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (43, 23, 1, 44, 5, 0, '1', '5', NULL, NULL);
INSERT INTO public.ducklake_file_column_stats VALUES (43, 23, 2, 49, 5, 0, 'a', 'e', NULL, NULL);


--
-- Data for Name: ducklake_file_partition_value; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_file_partition_value VALUES (8, 11, 0, '0');
INSERT INTO public.ducklake_file_partition_value VALUES (9, 11, 0, '1');
INSERT INTO public.ducklake_file_partition_value VALUES (10, 11, 0, '2');
INSERT INTO public.ducklake_file_partition_value VALUES (11, 11, 0, '3');
INSERT INTO public.ducklake_file_partition_value VALUES (12, 11, 0, '4');
INSERT INTO public.ducklake_file_partition_value VALUES (13, 13, 0, '1');
INSERT INTO public.ducklake_file_partition_value VALUES (13, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (14, 13, 0, '2');
INSERT INTO public.ducklake_file_partition_value VALUES (14, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (15, 13, 0, '3');
INSERT INTO public.ducklake_file_partition_value VALUES (15, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (16, 13, 0, '4');
INSERT INTO public.ducklake_file_partition_value VALUES (16, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (17, 13, 0, '5');
INSERT INTO public.ducklake_file_partition_value VALUES (17, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (18, 13, 0, '6');
INSERT INTO public.ducklake_file_partition_value VALUES (18, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (19, 13, 0, '7');
INSERT INTO public.ducklake_file_partition_value VALUES (19, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (20, 13, 0, '8');
INSERT INTO public.ducklake_file_partition_value VALUES (20, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (21, 13, 0, '9');
INSERT INTO public.ducklake_file_partition_value VALUES (21, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (22, 13, 0, '10');
INSERT INTO public.ducklake_file_partition_value VALUES (22, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (23, 13, 0, '11');
INSERT INTO public.ducklake_file_partition_value VALUES (23, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (24, 13, 0, '12');
INSERT INTO public.ducklake_file_partition_value VALUES (24, 13, 1, '2024');
INSERT INTO public.ducklake_file_partition_value VALUES (25, 13, 0, '1');
INSERT INTO public.ducklake_file_partition_value VALUES (25, 13, 1, '2025');
INSERT INTO public.ducklake_file_partition_value VALUES (26, 13, 0, '2');
INSERT INTO public.ducklake_file_partition_value VALUES (26, 13, 1, '2025');


--
-- Data for Name: ducklake_file_variant_stats; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_files_scheduled_for_deletion; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (30, 'del/two_snapshots/ducklake-01a0880c-57bb-7518-87f2-72b21571d929-delete.parquet', true, '2026-09-09 14:21:45.404715-07');
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (38, 'merge/table/ducklake-01a0880c-5854-7101-8637-6417cb78d89d.parquet', true, '2026-09-09 14:21:45.570021-07');
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (39, 'merge/table/ducklake-01a0880c-5857-70c2-a467-9af31d855fdd.parquet', true, '2026-09-09 14:21:45.570021-07');
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (40, 'merge/table/ducklake-01a0880c-585a-7253-8102-637a34a2d7be.parquet', true, '2026-09-09 14:21:45.570021-07');
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (41, 'merge/table/ducklake-01a0880c-585d-7ed7-862c-d62bbaeafeb8.parquet', true, '2026-09-09 14:21:45.570021-07');
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (42, 'merge/table/ducklake-01a0880c-585f-72c6-b5b3-d3f6c4f11e4b.parquet', true, '2026-09-09 14:21:45.570021-07');


--
-- Data for Name: ducklake_inlined_data_21_21; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_inlined_data_21_21 VALUES (0, 34, NULL, 1, '\x61');
INSERT INTO public.ducklake_inlined_data_21_21 VALUES (1, 35, NULL, 2, '\x62');
INSERT INTO public.ducklake_inlined_data_21_21 VALUES (2, 35, NULL, 3, '\x63');
INSERT INTO public.ducklake_inlined_data_21_21 VALUES (3, 36, NULL, 4, '\x64');
INSERT INTO public.ducklake_inlined_data_21_21 VALUES (4, 36, NULL, 5, '\x65');
INSERT INTO public.ducklake_inlined_data_21_21 VALUES (5, 36, NULL, 6, '\x66');


--
-- Data for Name: ducklake_inlined_data_tables; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_inlined_data_tables VALUES (21, 'ducklake_inlined_data_21_21', 21);


--
-- Data for Name: ducklake_macro; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_macro_impl; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_macro_parameters; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_metadata; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_metadata VALUES ('version', '1.0', NULL, NULL);
INSERT INTO public.ducklake_metadata VALUES ('created_by', 'DuckDB d8cdaa33fd', NULL, NULL);
INSERT INTO public.ducklake_metadata VALUES ('data_path', '/Users/jianjian.xie/git/presto-jj/presto-ducklake/src/test/resources/ducklake/data/', NULL, NULL);
INSERT INTO public.ducklake_metadata VALUES ('encrypted', 'false', NULL, NULL);
INSERT INTO public.ducklake_metadata VALUES ('data_inlining_row_limit', '10', 'table', 21);


--
-- Data for Name: ducklake_name_mapping; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_partition_column; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_partition_column VALUES (12, 11, 0, 1, 'identity');
INSERT INTO public.ducklake_partition_column VALUES (14, 13, 0, 2, 'month');
INSERT INTO public.ducklake_partition_column VALUES (14, 13, 1, 2, 'year');


--
-- Data for Name: ducklake_partition_info; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_partition_info VALUES (12, 11, 13, NULL);
INSERT INTO public.ducklake_partition_info VALUES (14, 13, 16, NULL);


--
-- Data for Name: ducklake_schema; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_schema VALUES (0, 'fd12cb3c-31f9-445a-b161-5c0a668ba0b1', 0, NULL, 'main', 'main/', true);
INSERT INTO public.ducklake_schema VALUES (1, '01a0880c-56a5-7fd4-9273-311c9ba85207', 1, NULL, 'tpch', 'tpch/', true);
INSERT INTO public.ducklake_schema VALUES (6, '01a0880c-56d0-7540-82fb-2a9533762416', 3, NULL, 'types', 'types/', true);
INSERT INTO public.ducklake_schema VALUES (10, '01a0880c-5721-757c-9422-933f5ce73c68', 11, NULL, 'part', 'part/', true);
INSERT INTO public.ducklake_schema VALUES (15, '01a0880c-577d-792e-9bcd-4b3a5b97e968', 18, NULL, 'del', 'del/', true);
INSERT INTO public.ducklake_schema VALUES (18, '01a0880c-57c4-76c3-b409-899e7f154a5e', 26, NULL, 'evo', 'evo/', true);
INSERT INTO public.ducklake_schema VALUES (20, '01a0880c-5803-766c-85ad-932a4227633a', 32, NULL, 'inl', 'inl/', true);
INSERT INTO public.ducklake_schema VALUES (22, '01a0880c-5838-7c46-ad0a-63f2b501c37e', 38, NULL, 'merge', 'merge/', true);


--
-- Data for Name: ducklake_schema_versions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_schema_versions VALUES (2, 2, 2);
INSERT INTO public.ducklake_schema_versions VALUES (2, 2, 3);
INSERT INTO public.ducklake_schema_versions VALUES (2, 2, 4);
INSERT INTO public.ducklake_schema_versions VALUES (2, 2, 5);
INSERT INTO public.ducklake_schema_versions VALUES (4, 4, 7);
INSERT INTO public.ducklake_schema_versions VALUES (7, 5, 8);
INSERT INTO public.ducklake_schema_versions VALUES (9, 6, 9);
INSERT INTO public.ducklake_schema_versions VALUES (12, 8, 11);
INSERT INTO public.ducklake_schema_versions VALUES (13, 9, 11);
INSERT INTO public.ducklake_schema_versions VALUES (15, 10, 13);
INSERT INTO public.ducklake_schema_versions VALUES (16, 11, 13);
INSERT INTO public.ducklake_schema_versions VALUES (19, 13, 16);
INSERT INTO public.ducklake_schema_versions VALUES (22, 14, 17);
INSERT INTO public.ducklake_schema_versions VALUES (27, 16, 19);
INSERT INTO public.ducklake_schema_versions VALUES (29, 17, 19);
INSERT INTO public.ducklake_schema_versions VALUES (30, 18, 19);
INSERT INTO public.ducklake_schema_versions VALUES (33, 20, 21);
INSERT INTO public.ducklake_schema_versions VALUES (39, 23, 23);


--
-- Data for Name: ducklake_snapshot; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_snapshot VALUES (0, '2026-09-09 14:21:44.980256-07', 0, 1, 0);
INSERT INTO public.ducklake_snapshot VALUES (1, '2026-09-09 14:21:45.114983-07', 1, 2, 0);
INSERT INTO public.ducklake_snapshot VALUES (2, '2026-09-09 14:21:45.127192-07', 2, 6, 4);
INSERT INTO public.ducklake_snapshot VALUES (3, '2026-09-09 14:21:45.168266-07', 3, 7, 4);
INSERT INTO public.ducklake_snapshot VALUES (4, '2026-09-09 14:21:45.169805-07', 4, 8, 4);
INSERT INTO public.ducklake_snapshot VALUES (5, '2026-09-09 14:21:45.18855-07', 4, 8, 5);
INSERT INTO public.ducklake_snapshot VALUES (6, '2026-09-09 14:21:45.202128-07', 4, 8, 6);
INSERT INTO public.ducklake_snapshot VALUES (7, '2026-09-09 14:21:45.206782-07', 5, 9, 6);
INSERT INTO public.ducklake_snapshot VALUES (8, '2026-09-09 14:21:45.216737-07', 5, 9, 7);
INSERT INTO public.ducklake_snapshot VALUES (9, '2026-09-09 14:21:45.227916-07', 6, 10, 7);
INSERT INTO public.ducklake_snapshot VALUES (10, '2026-09-09 14:21:45.237808-07', 6, 10, 8);
INSERT INTO public.ducklake_snapshot VALUES (11, '2026-09-09 14:21:45.24894-07', 7, 11, 8);
INSERT INTO public.ducklake_snapshot VALUES (12, '2026-09-09 14:21:45.250127-07', 8, 12, 8);
INSERT INTO public.ducklake_snapshot VALUES (13, '2026-09-09 14:21:45.266837-07', 9, 13, 8);
INSERT INTO public.ducklake_snapshot VALUES (14, '2026-09-09 14:21:45.282673-07', 9, 13, 13);
INSERT INTO public.ducklake_snapshot VALUES (15, '2026-09-09 14:21:45.296102-07', 10, 14, 13);
INSERT INTO public.ducklake_snapshot VALUES (16, '2026-09-09 14:21:45.304536-07', 11, 15, 13);
INSERT INTO public.ducklake_snapshot VALUES (17, '2026-09-09 14:21:45.320661-07', 11, 15, 27);
INSERT INTO public.ducklake_snapshot VALUES (18, '2026-09-09 14:21:45.341334-07', 12, 16, 27);
INSERT INTO public.ducklake_snapshot VALUES (19, '2026-09-09 14:21:45.342212-07', 13, 17, 27);
INSERT INTO public.ducklake_snapshot VALUES (20, '2026-09-09 14:21:45.358252-07', 13, 17, 28);
INSERT INTO public.ducklake_snapshot VALUES (21, '2026-09-09 14:21:45.370952-07', 13, 17, 29);
INSERT INTO public.ducklake_snapshot VALUES (22, '2026-09-09 14:21:45.378558-07', 14, 18, 29);
INSERT INTO public.ducklake_snapshot VALUES (23, '2026-09-09 14:21:45.386757-07', 14, 18, 30);
INSERT INTO public.ducklake_snapshot VALUES (24, '2026-09-09 14:21:45.39755-07', 14, 18, 31);
INSERT INTO public.ducklake_snapshot VALUES (25, '2026-09-09 14:21:45.404715-07', 14, 18, 32);
INSERT INTO public.ducklake_snapshot VALUES (26, '2026-09-09 14:21:45.41196-07', 15, 19, 32);
INSERT INTO public.ducklake_snapshot VALUES (27, '2026-09-09 14:21:45.413198-07', 16, 20, 32);
INSERT INTO public.ducklake_snapshot VALUES (28, '2026-09-09 14:21:45.429273-07', 16, 20, 33);
INSERT INTO public.ducklake_snapshot VALUES (29, '2026-09-09 14:21:45.440905-07', 17, 20, 33);
INSERT INTO public.ducklake_snapshot VALUES (30, '2026-09-09 14:21:45.449669-07', 18, 20, 33);
INSERT INTO public.ducklake_snapshot VALUES (31, '2026-09-09 14:21:45.465248-07', 18, 20, 34);
INSERT INTO public.ducklake_snapshot VALUES (32, '2026-09-09 14:21:45.475252-07', 19, 21, 34);
INSERT INTO public.ducklake_snapshot VALUES (33, '2026-09-09 14:21:45.476368-07', 20, 22, 34);
INSERT INTO public.ducklake_snapshot VALUES (34, '2026-09-09 14:21:45.506506-07', 21, 22, 35);
INSERT INTO public.ducklake_snapshot VALUES (35, '2026-09-09 14:21:45.510524-07', 21, 22, 36);
INSERT INTO public.ducklake_snapshot VALUES (36, '2026-09-09 14:21:45.521861-07', 21, 22, 37);
INSERT INTO public.ducklake_snapshot VALUES (37, '2026-09-09 14:21:45.524819-07', 21, 22, 38);
INSERT INTO public.ducklake_snapshot VALUES (38, '2026-09-09 14:21:45.528014-07', 22, 23, 38);
INSERT INTO public.ducklake_snapshot VALUES (39, '2026-09-09 14:21:45.529331-07', 23, 24, 38);
INSERT INTO public.ducklake_snapshot VALUES (40, '2026-09-09 14:21:45.547806-07', 23, 24, 39);
INSERT INTO public.ducklake_snapshot VALUES (41, '2026-09-09 14:21:45.558797-07', 23, 24, 40);
INSERT INTO public.ducklake_snapshot VALUES (42, '2026-09-09 14:21:45.561606-07', 23, 24, 41);
INSERT INTO public.ducklake_snapshot VALUES (43, '2026-09-09 14:21:45.564431-07', 23, 24, 42);
INSERT INTO public.ducklake_snapshot VALUES (44, '2026-09-09 14:21:45.567358-07', 23, 24, 43);
INSERT INTO public.ducklake_snapshot VALUES (45, '2026-09-09 14:21:45.570021-07', 23, 24, 44);


--
-- Data for Name: ducklake_snapshot_changes; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (1, 'created_schema:"tpch"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (2, 'created_table:"tpch"."customer",created_table:"tpch"."orders",created_table:"tpch"."region",created_table:"tpch"."nation",inserted_into_table:2,inserted_into_table:3,inserted_into_table:4,inserted_into_table:5', 'fixture-generator', 'Load TPC-H tiny fixture (nation, region, customer, orders)', NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (3, 'created_schema:"types"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (4, 'created_table:"types"."primitives"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (5, 'inserted_into_table:7', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (6, 'inserted_into_table:7', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (7, 'created_table:"types"."nested"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (8, 'inserted_into_table:8', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (9, 'created_table:"types"."unsupported"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (10, 'inserted_into_table:9', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (11, 'created_schema:"part"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (12, 'created_table:"part"."by_identity"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (13, 'altered_table:11', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (14, 'inserted_into_table:11', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (15, 'created_table:"part"."by_month"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (16, 'altered_table:13', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (17, 'inserted_into_table:13', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (18, 'created_schema:"del"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (19, 'created_table:"del"."simple"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (20, 'inserted_into_table:16', 'fixture-generator', 'Insert 1000 rows for del.simple fixture', NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (21, 'deleted_from_table:16', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (22, 'created_table:"del"."two_snapshots"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (23, 'inserted_into_table:17', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (24, 'deleted_from_table:17', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (25, 'deleted_from_table:17', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (26, 'created_schema:"evo"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (27, 'created_table:"evo"."table"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (28, 'inserted_into_table:19', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (29, 'altered_table:19', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (30, 'altered_table:19', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (31, 'inserted_into_table:19', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (32, 'created_schema:"inl"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (33, 'created_table:"inl"."small"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (34, 'inlined_insert:21', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (35, 'inlined_insert:21', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (36, 'inlined_insert:21', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (37, 'inserted_into_table:21', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (38, 'created_schema:"merge"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (39, 'created_table:"merge"."table"', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (40, 'inserted_into_table:23', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (41, 'inserted_into_table:23', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (42, 'inserted_into_table:23', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (43, 'inserted_into_table:23', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (44, 'inserted_into_table:23', NULL, NULL, NULL);
INSERT INTO public.ducklake_snapshot_changes VALUES (45, 'merge_adjacent:23', NULL, NULL, NULL);


--
-- Data for Name: ducklake_sort_expression; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_sort_info; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_table; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_table VALUES (2, '01a0880c-56b0-732b-8543-de72d6dd0c83', 2, NULL, 1, 'customer', 'customer/', true);
INSERT INTO public.ducklake_table VALUES (3, '01a0880c-56b1-7fcf-877e-a8e7a460d88a', 2, NULL, 1, 'orders', 'orders/', true);
INSERT INTO public.ducklake_table VALUES (4, '01a0880c-56af-7346-b7d0-ed255e218ea9', 2, NULL, 1, 'region', 'region/', true);
INSERT INTO public.ducklake_table VALUES (5, '01a0880c-56ae-7c26-9b47-bada331b2c72', 2, NULL, 1, 'nation', 'nation/', true);
INSERT INTO public.ducklake_table VALUES (7, '01a0880c-56da-7210-af30-54499a7344d4', 4, NULL, 6, 'primitives', 'primitives/', true);
INSERT INTO public.ducklake_table VALUES (8, '01a0880c-56f7-73c7-a2e7-83a7170bbfc3', 7, NULL, 6, 'nested', 'nested/', true);
INSERT INTO public.ducklake_table VALUES (9, '01a0880c-570c-7f2a-aa45-8669186bd3c2', 9, NULL, 6, 'unsupported', 'unsupported/', true);
INSERT INTO public.ducklake_table VALUES (11, '01a0880c-572a-724c-b1e6-5a093b7f4ceb', 12, NULL, 10, 'by_identity', 'by_identity/', true);
INSERT INTO public.ducklake_table VALUES (13, '01a0880c-5750-7d20-8917-34849cffa557', 15, NULL, 10, 'by_month', 'by_month/', true);
INSERT INTO public.ducklake_table VALUES (16, '01a0880c-5785-7b77-a635-ff65ea208fd4', 19, NULL, 15, 'simple', 'simple/', true);
INSERT INTO public.ducklake_table VALUES (17, '01a0880c-57a3-7115-8faf-3382bc5bf5a0', 22, NULL, 15, 'two_snapshots', 'two_snapshots/', true);
INSERT INTO public.ducklake_table VALUES (19, '01a0880c-57cc-764c-a85c-c32a24608f1a', 27, NULL, 18, 'table', 'table/', true);
INSERT INTO public.ducklake_table VALUES (21, '01a0880c-580d-7e36-ba6c-2b7db78274e5', 33, NULL, 20, 'small', 'small/', true);
INSERT INTO public.ducklake_table VALUES (23, '01a0880c-5842-756d-a9f6-69e12eaa9473', 39, NULL, 22, 'table', 'table/', true);


--
-- Data for Name: ducklake_table_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_table_column_stats VALUES (5, 1, false, NULL, '0', '24', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (5, 2, false, NULL, 'ALGERIA', 'VIETNAM', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (5, 3, false, NULL, '0', '4', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (5, 4, false, NULL, ' beans after the carefully regular accounts r', 'usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily ', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (4, 1, false, NULL, '0', '4', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (4, 2, false, NULL, 'AFRICA', 'MIDDLE EAST', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (4, 3, false, NULL, ' foxes boost furiously along the carefully dogged tithes. slyly regular orbits according to the special epit', 's are. furiously even pinto bea', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 1, false, NULL, '1', '1500', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 2, false, NULL, 'Customer#000000001', 'Customer#000001500', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 3, false, NULL, '  dcVkxZ,s,9xW ab60aC3slv3STVRwA1', 'zwrDoaY2gxCkdTXFaxNc', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 4, false, NULL, '0', '24', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 5, false, NULL, '10-109-430-5638', '34-992-529-2023', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 6, false, false, '-994.79', '9987.71', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 7, false, NULL, 'AUTOMOBILE', 'MACHINERY', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (2, 8, false, NULL, ' about the carefully final pinto beans. quickly regular sheaves are slyly final, f', 'ys among the accounts affix blithely asymptotes. carefully even deposits are fluff', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 1, false, NULL, '1', '60000', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 2, false, NULL, '1', '1499', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 3, false, NULL, 'F', 'P', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 4, false, false, '874.89', '466001.28', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 5, false, NULL, '1992-01-01', '1998-08-02', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 6, false, NULL, '1-URGENT', '5-LOW', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 7, false, NULL, 'Clerk#000000001', 'Clerk#000001000', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 8, false, NULL, '0', '0', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (3, 9, false, NULL, ' Tiresias. carefully spec', 'zzle: slyly even ideas wake furiously across the ironic p', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 1, false, NULL, '1', '2', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 2, true, NULL, '1', '1', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 3, true, NULL, '12', '12', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 4, true, NULL, '1234', '1234', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 5, true, NULL, '123456', '123456', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 6, true, NULL, '1234567890123', '1234567890123', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 7, true, false, '3.14', '3.14', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 8, true, false, '2.718281828', '2.718281828', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 9, true, NULL, '12345.678', '12345.678', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 10, true, NULL, 'hello world', 'hello world', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 11, true, NULL, '2024-06-15', '2024-06-15', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 12, true, NULL, '13:45:30', '13:45:30', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 13, true, NULL, '2024-06-15 13:45:30.123456', '2024-06-15 13:45:30.123456', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 14, true, NULL, '2024-06-15 13:45:30', '2024-06-15 13:45:30', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 15, true, NULL, '2024-06-15 13:45:30.123', '2024-06-15 13:45:30.123', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 16, true, NULL, '2024-06-15 13:45:30.123456789', '2024-06-15 13:45:30.123456789', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 17, true, NULL, '2024-06-15 13:45:30.123456+00', '2024-06-15 13:45:30.123456+00', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 18, true, NULL, '550e8400-e29b-41d4-a716-446655440000', '550e8400-e29b-41d4-a716-446655440000', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 19, true, NULL, '{"key": "value", "n": 1}', '{"key": "value", "n": 1}', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (7, 20, true, NULL, '48656C6C6F20576F726C64', '48656C6C6F20576F726C64', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 1, false, NULL, '1', '3', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 3, true, NULL, '1', '3', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 5, true, NULL, '10', '10', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 6, true, NULL, 'ten', 'ten', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 8, true, NULL, 'x', 'y', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 9, true, NULL, '1', '2', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 11, true, NULL, 'first', 'second', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (8, 13, true, NULL, 'a', 'b', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (9, 1, false, NULL, '1', '1', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (9, 2, false, NULL, NULL, NULL, NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (11, 1, false, NULL, '0', '4', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (11, 2, false, NULL, 'row_0', 'row_9', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (13, 1, false, NULL, '0', '399', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (13, 2, false, NULL, '2024-01-01 00:00:00', '2025-02-03 00:00:00', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (13, 3, false, NULL, 'row_0', 'row_99', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (16, 1, false, NULL, '0', '999', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (16, 2, false, NULL, 'row_0', 'row_999', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (17, 1, false, NULL, '0', '999', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (17, 2, false, NULL, 'row_0', 'row_999', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (19, 1, false, NULL, '1', '4', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (19, 2, false, NULL, 'alice', 'dave', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (21, 1, false, NULL, '0', '19', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (21, 2, false, NULL, 'a', 'f', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (23, 1, false, NULL, '1', '5', NULL);
INSERT INTO public.ducklake_table_column_stats VALUES (23, 2, false, NULL, 'a', 'e', NULL);


--
-- Data for Name: ducklake_table_stats; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.ducklake_table_stats VALUES (5, 25, 25, 2338);
INSERT INTO public.ducklake_table_stats VALUES (4, 5, 5, 1083);
INSERT INTO public.ducklake_table_stats VALUES (2, 1500, 1500, 126839);
INSERT INTO public.ducklake_table_stats VALUES (3, 15000, 15000, 542225);
INSERT INTO public.ducklake_table_stats VALUES (7, 2, 2, 4905);
INSERT INTO public.ducklake_table_stats VALUES (8, 3, 3, 1311);
INSERT INTO public.ducklake_table_stats VALUES (9, 1, 1, 296);
INSERT INTO public.ducklake_table_stats VALUES (11, 50, 50, 2153);
INSERT INTO public.ducklake_table_stats VALUES (13, 400, 400, 12439);
INSERT INTO public.ducklake_table_stats VALUES (16, 1000, 1000, 8361);
INSERT INTO public.ducklake_table_stats VALUES (17, 1000, 1000, 8361);
INSERT INTO public.ducklake_table_stats VALUES (19, 4, 4, 760);
INSERT INTO public.ducklake_table_stats VALUES (21, 26, 26, 483);
INSERT INTO public.ducklake_table_stats VALUES (23, 5, 5, 1435);


--
-- Data for Name: ducklake_tag; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: ducklake_view; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: ducklake_data_file ducklake_data_file_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_data_file
    ADD CONSTRAINT ducklake_data_file_pkey PRIMARY KEY (data_file_id);


--
-- Name: ducklake_delete_file ducklake_delete_file_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_delete_file
    ADD CONSTRAINT ducklake_delete_file_pkey PRIMARY KEY (delete_file_id);


--
-- Name: ducklake_schema ducklake_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_schema
    ADD CONSTRAINT ducklake_schema_pkey PRIMARY KEY (schema_id);


--
-- Name: ducklake_snapshot_changes ducklake_snapshot_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_snapshot_changes
    ADD CONSTRAINT ducklake_snapshot_changes_pkey PRIMARY KEY (snapshot_id);


--
-- Name: ducklake_snapshot ducklake_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_snapshot
    ADD CONSTRAINT ducklake_snapshot_pkey PRIMARY KEY (snapshot_id);


--
-- PostgreSQL database dump complete
--


