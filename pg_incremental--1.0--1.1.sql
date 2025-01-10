ALTER TABLE incremental.pipelines ADD COLUMN search_path text;

DROP FUNCTION incremental.create_file_list_pipeline(text,text,text,bool,text,text,bool);
CREATE FUNCTION incremental.create_file_list_pipeline(
    pipeline_name text,
    file_pattern text,
    command text,
    batched bool default false,
    list_function text default NULL,
    schedule text default '* * * * *',
    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.create_file_list_pipeline(text,text,text,bool,text,text,bool)
 IS 'create a pipeline of new files';
