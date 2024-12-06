#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/file_list.h"
#include "crunchy/incremental/pipeline.h"
#include "executor/spi.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"


/*
 * FileList represents a set of files that can be safely processed.
 */
typedef struct FileList
{
	List	   *files;
	bool		batched;
}			FileList;


static void ExecuteFileListPipelineForFile(char *pipelineName, char *command, char *path);
static FileList * GetSafeFileList(char *pipelineName);
static void InsertProcessedFile(char *pipelineName, char *path);



/*
 * InitializeFileListPipelineState adds the initial file list pipeline state.
 */
void
InitializeFileListPipelineState(char *pipelineName, char *pattern, bool batched)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"insert into incremental.file_list_pipelines "
		"(pipeline_name, file_pattern, batched) "
		"values ($1, $2, $3)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 3;
	Oid			argTypes[] = {TEXTOID, TEXTOID, BOOLOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CStringGetTextDatum(pattern),
		BoolGetDatum(batched)
	};
	char		argNulls[] = "   ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);
	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * ExecuteFileListPipeline executes a file list pipeline.
 */
void
ExecuteFileListPipeline(char *pipelineName, char *command)
{
	/* get the full fileList of data to process */
	FileList   *fileList = GetSafeFileList(pipelineName);

	if (fileList->files == NIL)
	{
		ereport(NOTICE, (errmsg("pipeline %s: no files to process",
								pipelineName)));
		return;
	}

	if (fileList->batched)
	{
		/* pass the files as an array */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("batched file pipelines are not yet supported")));
	}
	else
	{
		ListCell   *fileCell = NULL;

		foreach(fileCell, fileList->files)
		{
			char	   *path = lfirst(fileCell);

			ExecuteFileListPipelineForFile(pipelineName, command, path);
			InsertProcessedFile(pipelineName, path);
		}
	}
}


/*
 * ExecuteFileListPipelineForList executes a file list pipeline for
 * the given file list.
 */
static void
ExecuteFileListPipelineForFile(char *pipelineName, char *command, char *path)
{
	ereport(NOTICE, (errmsg("pipeline %s: processing file list pipeline for %s",
							pipelineName, path)));

	PushActiveSnapshot(GetTransactionSnapshot());

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(path)
	};
	char	   *argNulls = " ";

	SPI_connect();
	SPI_execute_with_args(command,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);
	SPI_finish();

	PopActiveSnapshot();
}


/*
 * GetSafeFileList reads the current state of the given sequence pipeline
 * and returns whether there are rows to process.
 */
static FileList *
GetSafeFileList(char *pipelineName)
{
	MemoryContext outerContext = CurrentMemoryContext;
	FileList   *fileList = (FileList *) palloc0(sizeof(FileList));

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the unprocessed files.
	 */
	char	   *query =
		"select path "
		"from incremental.file_list_pipelines, "
		"crunchy_lake.list_files(file_pattern) "
		"where pipeline_name operator(pg_catalog.=) $1 "
		"and path not in ("
		"select path from incremental.processed_files where pipeline_name operator(pg_catalog.=) $1"
		")";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName)
	};
	char	   *argNulls = " ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;

	/* TODO: maybe return as array */
	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		HeapTuple	row = SPI_tuptable->vals[0];

		bool		isNull = false;
		Datum		pathDatum = SPI_getbinval(row, rowDesc, 1, &isNull);

		MemoryContext oldContext = MemoryContextSwitchTo(outerContext);

		fileList->files = lappend(fileList->files, TextDatumGetCString(pathDatum));

		MemoryContextSwitchTo(oldContext);
	}

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/* return whether there are rows to process */
	return fileList;
}


/*
 * InsertProcessedFile adds a new processed file to the processed_files
 * table.
 */
static void
InsertProcessedFile(char *pipelineName, char *path)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipleines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-drawn sequence number, which may be part of a write that
	 * has not committed yet. Also block other pipeline rollups.
	 */
	char	   *query =
		"insert into incremental.processed_files (pipeline_name, path) "
		"values ($1, $2)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CStringGetTextDatum(path)
	};
	char	   *argNulls = "  ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	if (SPI_processed <= 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("pipeline \"%s\" cannot be found",
							   pipelineName)));

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * RemoveProcessedFileList removes all the processed files for the given pipeline.
 */
void
RemoveProcessedFileList(char *pipelineName)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipleines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-drawn sequence number, which may be part of a write that
	 * has not committed yet. Also block other pipeline rollups.
	 */
	char	   *query =
		"delete from incremental.file_list_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName)
	};
	char	   *argNulls = " ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	if (SPI_processed <= 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("pipeline \"%s\" cannot be found",
							   pipelineName)));

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}
