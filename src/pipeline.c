#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/cron.h"
#include "crunchy/incremental/pipeline.h"
#include "crunchy/incremental/sequence.h"
#include "executor/spi.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

static void CreatePipeline(char *pipelineName, PipelineType pipelineType, Oid sequenceId,
						   char *command, char *schedule, bool executeImmediately);
static void InsertPipeline(char *pipelineName, PipelineType pipelineType, Oid sourceRelationId,
						   char *command);
static void EnsurePipelineOwner(char *pipelineName, Oid ownerId);
static void ExecutePipeline(char *pipelineName, PipelineType pipelineType, char *command);
static void ResetPipeline(char *pipelineName, PipelineType pipelineType);
static void DeletePipeline(char *pipelineName);
static char *GetCronJobNameForPipeline(char *pipelineName);
static char *GetCronCommandForPipeline(char *pipelineName);

static Query *ParseQuery(char *command, List *paramTypes);
static char *DeparseQuery(Query *query);
static void ExecuteCommand(char *commandString);


PG_FUNCTION_INFO_V1(incremental_create_sequence_pipeline);
PG_FUNCTION_INFO_V1(incremental_execute_pipeline);
PG_FUNCTION_INFO_V1(incremental_reset_pipeline);
PG_FUNCTION_INFO_V1(incremental_drop_pipeline);


/*
 * incremental_create_sequence_pipeline creates a new pipeline that tracks
 * a sequence.
 */
Datum
incremental_create_sequence_pipeline(PG_FUNCTION_ARGS)
{
	/*
	 * create_sequence_pipeline is not strict because the last argument can be
	 * NULL, so check the arguments that cannot be NULL.
	 */
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("pipeline_name cannot be NULL")));
	if (PG_ARGISNULL(1))
		ereport(ERROR, (errmsg("sequence_name cannot be NULL")));
	if (PG_ARGISNULL(2))
		ereport(ERROR, (errmsg("command cannot be NULL")));

	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			sequenceId = PG_GETARG_OID(1);
	char	   *command = text_to_cstring(PG_GETARG_TEXT_P(2));
	char	   *schedule = PG_ARGISNULL(3) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(3));

	switch (get_rel_relkind(sequenceId))
	{
		case RELKIND_SEQUENCE:
			{
				Oid			relationId = InvalidOid;
				int32		columnNumber = 0;

				if (!sequenceIsOwned(sequenceId, DEPENDENCY_AUTO, &relationId, &columnNumber))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("only sequences that are owned by a table are supported")));
				}

				break;
			}

		case RELKIND_RELATION:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_PARTITIONED_TABLE:
			{
				/* user entered a table name, see if it has a single sequence */
				sequenceId = FindSequenceForRelation(sequenceId);
				break;
			}

		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("%s is not a table or sequence", get_rel_name(sequenceId))));
			}
	}

	CreatePipeline(pipelineName, SEQUENCE_PIPELINE, sequenceId, command, schedule, true);

	PG_RETURN_VOID();
}


/*
 * incremental_execute_pipeline executes a pipeline to its initial state.
 */
Datum
incremental_execute_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	ExecutePipeline(pipelineName, pipelineDesc->pipelineType, pipelineDesc->command);

	PG_RETURN_VOID();
}


/*
 * incremental_reset_pipeline reset a pipeline to its initial state.
 */
Datum
incremental_reset_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	ResetPipeline(pipelineName, pipelineDesc->pipelineType);

	PG_RETURN_VOID();
}


/*
 * incremental_drop_pipeline drops a pipeline by name.
 */
Datum
incremental_drop_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	DeletePipeline(pipelineName);

	UnscheduleCronJob(GetCronJobNameForPipeline(pipelineName));

	PG_RETURN_VOID();
}


/*
 * CreatePipeline sets up a pipeline for a table with a sequence.
 */
static void
CreatePipeline(char *pipelineName, PipelineType pipelineType, Oid sourceRelationId,
			   char *command, char *schedule, bool executeImmediately)
{
	/* determine the parameter list for the query */
	List	   *paramTypes = NIL;

	switch (pipelineType)
	{
		case SEQUENCE_PIPELINE:
			paramTypes = list_make2_oid(INT8OID, INT8OID);
			break;

		case FILE_PIPELINE:
			paramTypes = list_make1_oid(TEXTARRAYOID);
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}

	/* sanitize the query */
	Query	   *parsedQuery = ParseQuery(command, paramTypes);
	char	   *sanitizedCommand = DeparseQuery(parsedQuery);

	InsertPipeline(pipelineName, SEQUENCE_PIPELINE, sourceRelationId, sanitizedCommand);

	switch (pipelineType)
	{
		case SEQUENCE_PIPELINE:
			InitializeSequencePipelineState(pipelineName, sourceRelationId);
			break;

		case FILE_PIPELINE:
			/* initial state is empty */
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}

	if (executeImmediately)
	{
		ExecutePipeline(pipelineName, pipelineType, sanitizedCommand);
	}

	if (schedule != NULL)
	{
		char	   *jobName = GetCronJobNameForPipeline(pipelineName);
		char	   *cronCommand = GetCronCommandForPipeline(pipelineName);

		int64		jobId = ScheduleCronJob(jobName, schedule, cronCommand);

		ereport(NOTICE, (errmsg("pipeline %s: scheduled cron job  with ID " INT64_FORMAT
								" and schedule %s",
								pipelineName, jobId, schedule)));
	}
}


/*
 * InsertPipeline adds a new pipeline.
 */
static void
InsertPipeline(char *pipelineName, PipelineType pipelineType, Oid sourceRelationId,
			   char *command)
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
		"insert into incremental.pipelines "
		"(pipeline_name, pipeline_type, owner_id, source_relation, command) "
		"values ($1, $2, $3, $4, $5)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 5;
	Oid			argTypes[] = {TEXTOID, CHAROID, OIDOID, OIDOID, TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CharGetDatum(pipelineType),
		ObjectIdGetDatum(savedUserId),
		ObjectIdGetDatum(sourceRelationId),
		CStringGetTextDatum(command)
	};
	char	   *argNulls = "     ";

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
 * ReadPipelineCommand returns a full description of a pipeline.
 */
PipelineDesc *
ReadPipelineDesc(char *pipelineName)
{
	PipelineDesc *pipelineDesc = (PipelineDesc *) palloc0(sizeof(PipelineDesc));

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have read
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext callerContext = CurrentMemoryContext;

	char	   *query =
		"select pipeline_type, owner_id, source_relation, command "
		"from incremental.pipelines where pipeline_name = $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName)
	};
	char	   *argNulls = " ";

	/* we do not switch user, because we want to preserve permissions */

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
						errmsg("no such pipeline named \"%s\"", pipelineName)));

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;
	HeapTuple	row = SPI_tuptable->vals[0];

	bool		isNull = false;
	Datum		pipelineTypeDatum = SPI_getbinval(row, rowDesc, 1, &isNull);
	Datum		ownerIdDatum = SPI_getbinval(row, rowDesc, 2, &isNull);
	Datum		sourceRelationDatum = SPI_getbinval(row, rowDesc, 3, &isNull);
	Datum		commandDatum = SPI_getbinval(row, rowDesc, 4, &isNull);

	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	pipelineDesc->pipelineName = pstrdup(pipelineName);
	pipelineDesc->pipelineType = DatumGetChar(pipelineTypeDatum);
	pipelineDesc->ownerId = DatumGetObjectId(ownerIdDatum);
	pipelineDesc->sourceRelationId = DatumGetObjectId(sourceRelationDatum);
	pipelineDesc->command = TextDatumGetCString(commandDatum);

	MemoryContextSwitchTo(spiContext);

	SPI_finish();

	return pipelineDesc;
}



/*
 * EnsurePipelineOwner throws an error if the current user is not
 * superuser and not the pipeline owner.
 */
static void
EnsurePipelineOwner(char *pipelineName, Oid ownerId)
{
	if (superuser())
		return;

	if (ownerId != GetUserId())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied for pipeline %s", pipelineName)));
}


/*
 * ExecutePipeline executes a pipeline.
 */
static void
ExecutePipeline(char *pipelineName, PipelineType pipelineType, char *command)
{
	char	   *fullCommand = NULL;

	switch (pipelineType)
	{
		case SEQUENCE_PIPELINE:
			fullCommand = GetSequencePipelineCommand(pipelineName, command);
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}

	ExecuteCommand(fullCommand);
}


/*
 * ResetPipeline reset a pipeline to its initial state.
 */
static void
ResetPipeline(char *pipelineName, PipelineType pipelineType)
{
	switch (pipelineType)
	{
		case SEQUENCE_PIPELINE:
			UpdateLastProcessedSequenceNumber(pipelineName, 0);
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}
}


/*
 * DeletePipeline removes a pipeline from the pipeline.pipelines table.
 */
static void
DeletePipeline(char *pipelineName)
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
		"delete from incremental.pipelines "
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

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * GetCronJobNameForPipeline returns the name of the cron job to use for a given pipeline.
 */
static char *
GetCronJobNameForPipeline(char *pipelineName)
{
	return psprintf("pipeline:%s", pipelineName);
}


/*
 * GetCronCommandForPipeline returns the command of the cron job to use for a given pipeline.
 */
static char *
GetCronCommandForPipeline(char *pipelineName)
{
	return psprintf("select incremental.execute_pipeline(%s)",
					quote_literal_cstr(pipelineName));
}


/*
 * ParseQuery parses a query string.
 *
 * The function parses the query and returns the query tree.
 */
static Query *
ParseQuery(char *command, List *paramTypes)
{
	List	   *parseTreeList = pg_parse_query(command);

	if (list_length(parseTreeList) != 1)
		ereport(ERRCODE_FEATURE_NOT_SUPPORTED,
				(errmsg("pg_pipeline can only execute a single query in a pipeline")));

	Oid		   *params = (Oid *) palloc(list_length(paramTypes) * sizeof(Oid));
	ListCell   *paramTypeCell = NULL;
	int			paramCount = 0;

	foreach(paramTypeCell, paramTypes)
	{
		params[paramCount++] = lfirst_oid(paramTypeCell);
	}

	RawStmt    *rawStmt = (RawStmt *) linitial(parseTreeList);
	List	   *queryTreeList =
		pg_analyze_and_rewrite_fixedparams(rawStmt, command, params, paramCount, NULL);

	/* we already checked parseTreeList lenght above */
	Assert(list_length(queryTreeList) == 1);

	return (Query *) linitial(queryTreeList);
}


/*
 * DeparsQuery deparses a Query AST.
 */
static char *
DeparseQuery(Query *query)
{
	int			save_nestlevel = NewGUCNestLevel();

	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	bool		pretty = false;
	char	   *newQuery = pg_get_querydef(query, pretty);

	AtEOXact_GUC(true, save_nestlevel);

	return newQuery;
}


/*
 * ExecuteCommand executes a SQL command.
 */
static void
ExecuteCommand(char *commandString)
{
	List	   *commandList = pg_parse_query(commandString);
	ListCell   *commandCell = NULL;

	foreach(commandCell, commandList)
	{
		RawStmt    *rawParseTree = lfirst(commandCell);

		PlannedStmt *plannedStmt = makeNode(PlannedStmt);

		plannedStmt->commandType = CMD_UTILITY;
		plannedStmt->utilityStmt = rawParseTree->stmt;

		ProcessUtility(plannedStmt, commandString, false, PROCESS_UTILITY_QUERY,
					   NULL, NULL, None_Receiver, NULL);
	}
}
