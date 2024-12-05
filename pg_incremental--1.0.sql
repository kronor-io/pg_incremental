-- Create a safe, extension-owned schema (or fail if it exists)
CREATE SCHEMA incremental;
GRANT USAGE ON SCHEMA incremental TO public;

CREATE TABLE incremental.pipelines (
    pipeline_name text not null,
	pipeline_type "char" not null,
    owner_id oid not null,
    source_relation regclass,
    command text,
    primary key (pipeline_name)
);
GRANT SELECT ON incremental.pipelines TO public;

/* pipelines that track new rows by waiting for lockers and finding the safe range of sequence values */
CREATE TABLE incremental.sequence_pipelines (
    pipeline_name text not null references incremental.pipelines (pipeline_name) on delete cascade on update cascade,
    sequence_name regclass not null,
    last_processed_sequence_number bigint,
    primary key (pipeline_name)
);
GRANT SELECT ON incremental.sequence_pipelines TO public;

/* pipelines that track new rows by selecting past time ranges */
CREATE TABLE incremental.time_interval_pipelines (
    pipeline_name text not null references incremental.pipelines (pipeline_name) on delete cascade on update cascade,
    time_interval interval not null,
    batched bool not null default false,
    min_delay interval not null,
    last_processed_time timestamptz,
    primary key (pipeline_name)
);
GRANT SELECT ON incremental.time_interval_pipelines TO public;

/* pipelines that process new files */
CREATE TABLE incremental.file_list_pipelines (
    pipeline_name text not null references incremental.pipelines (pipeline_name) on delete cascade on update cascade,
    prefix text not null,
    batched bool not null default false,
    primary key (pipeline_name)
);
GRANT SELECT ON incremental.file_list_pipelines TO public;

/* files that have been processed */
CREATE TABLE incremental.processed_files (
    pipeline_name text not null references incremental.pipelines (pipeline_name) on delete cascade on update cascade,
    path text not null,
    primary key (pipeline_name, path)
);
GRANT SELECT ON incremental.processed_files TO public;

CREATE FUNCTION incremental.create_sequence_pipeline(
                    name text,
                    sequence_name regclass,
                    command text,
                    schedule text default '* * * * *',
                    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_sequence_pipeline$function$;
COMMENT ON FUNCTION incremental.create_sequence_pipeline(text,regclass,text,text,bool)
 IS 'create a pipeline of new sequence ranges';

CREATE FUNCTION incremental.create_time_interval_pipeline(
                    name text,
                    time_interval interval,
                    command text,
                    batched bool default true,
                    start_time timestamptz default NULL,
                    source_table_name regclass default NULL,
                    schedule text default '* * * * *',
                    min_delay interval default '30 seconds',
                    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_time_interval_pipeline$function$;
COMMENT ON FUNCTION incremental.create_time_interval_pipeline(text,interval,text,bool,timestamptz,regclass,text,interval,bool)
 IS 'create a pipeline of new time intervals';

CREATE FUNCTION incremental.create_file_list_pipeline(
                    name text,
                    prefix text,
                    command text,
                    batched bool default false,
                    schedule text default '* * * * *',
                    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.create_file_list_pipeline(text,text,text,bool,text,bool)
 IS 'create a pipeline of new files';

CREATE PROCEDURE incremental.execute_pipeline(name text)
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_execute_pipeline$function$;
COMMENT ON PROCEDURE incremental.execute_pipeline(text)
 IS 'execute the pipeline command';

CREATE FUNCTION incremental.reset_pipeline(name text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$incremental_reset_pipeline$function$;
COMMENT ON FUNCTION incremental.reset_pipeline(text)
 IS 'reset the last processed sequence value of a pipeline';

CREATE FUNCTION incremental.drop_pipeline(name text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$incremental_drop_pipeline$function$;
COMMENT ON FUNCTION incremental.drop_pipeline(text)
 IS 'drop a pipeline by name';

CREATE FUNCTION incremental._drop_trigger()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SET search_path = pg_catalog
 SECURITY DEFINER
 AS $function$
DECLARE
  v_obj record;
BEGIN
  FOR v_obj IN
    SELECT * FROM pg_event_trigger_dropped_objects()
    WHERE object_type IN ('table', 'foreign table', 'sequence')
  LOOP
    DELETE FROM incremental.pipelines p
    WHERE source_relation = v_obj.objid;
  END LOOP;
END;
$function$;
COMMENT ON FUNCTION incremental._drop_trigger()
 IS 'cleans up pipelines belonging when the source table is dropped';

CREATE EVENT TRIGGER pipeline_drop_trigger
 ON SQL_DROP
 EXECUTE PROCEDURE incremental._drop_trigger();
