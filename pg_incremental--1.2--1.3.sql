CREATE FUNCTION incremental.skip_file(
    pipeline_name text,
    path text)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_skip_file$function$;
COMMENT ON FUNCTION incremental.skip_file(text,text)
 IS 'skip a file in a file list pipeline';
