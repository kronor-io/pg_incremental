ALTER TABLE incremental.file_list_pipelines ADD COLUMN max_batch_size int;

DROP FUNCTION incremental.create_file_list_pipeline(text,text,text,bool,text,text,bool);
CREATE FUNCTION incremental.create_file_list_pipeline(
    pipeline_name text,
    file_pattern text,
    command text,
    list_function text default NULL,
    batched bool default false,
    max_batch_size int default 100,
    schedule text default '*/15 * * * *',
    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.create_file_list_pipeline(text,text,text,text,bool,int,text,bool)
 IS 'create a pipeline of new files';
