#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/pipeline.h"
#include "crunchy/incremental/sequence.h"
#include "executor/spi.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"


/*
 * SequenceNumberRange represents a range of sequence numbers that can
 * be safely processed.
 */
typedef struct SequenceNumberRange
{
	uint64		rangeStart;
	uint64		rangeEnd;
}			SequenceNumberRange;


static SequenceNumberRange PopSequenceNumberRange(char *pipelineName, Oid sequenceId);
static bool GetSequenceNumberRange(char *pipelineName, Oid sequenceId,
								   SequenceNumberRange * range);


PG_FUNCTION_INFO_V1(incremental_sequence_range);


/*
 * GetSequencePipelineCommand returns the full command to run for sequence
 * pipelines.
 */
char *
GetSequencePipelineCommand(char *pipelineName, char *command)
{
	return psprintf(
					"DO LANGUAGE plpgsql $crunchy_pipeline_do$\n"
					"DECLARE\n"
					" $1 bigint;\n"
					" $2 bigint;\n"
					"BEGIN\n"
					" -- get a safe range of sequence values\n"
					" select range_start, range_end into $1, $2\n"
					" from incremental._sequence_range(%s);\n"
					"\n"
					" if $1 <= $2 then\n"
					"%s;\n"
					" end if;\n"
					"END;\n"
					"$crunchy_pipeline_do$;",
					quote_literal_cstr(pipelineName),
					command);
}


/*
 * InitializeSequencePipelineStats adds the initial sequence pipeline state.
 */
void
InitializeSequencePipelineState(char *pipelineName, Oid sequenceId)
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
		"insert into incremental.sequence_pipelines "
		"(pipeline_name, sequence_name) "
		"values ($1, $2)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, OIDOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		ObjectIdGetDatum(sequenceId)
	};
	char	   *argNulls = "   ";

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
 * incremental_sequence_range determines a safe range that can be processed
 * and stores progress as part of the transaction.
 */
Datum
incremental_sequence_range(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));

	TupleDesc	tupleDesc;

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));
	}

	SequenceNumberRange *range;

	if (fcinfo->flinfo->fn_extra != NULL)
		range = (SequenceNumberRange *) fcinfo->flinfo->fn_extra;
	else
	{
		PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

		range = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt,
									   sizeof(SequenceNumberRange));

		*range = PopSequenceNumberRange(pipelineName, pipelineDesc->sourceRelationId);

		fcinfo->flinfo->fn_extra = range;
	}

	if (range->rangeStart <= range->rangeEnd)
		ereport(NOTICE, (errmsg("pipeline %s: processing sequence values from "
								INT64_FORMAT " to " INT64_FORMAT,
								pipelineName, range->rangeStart, range->rangeEnd)));
	else
		ereport(NOTICE, (errmsg("pipeline %s: no rows to process",
								pipelineName)));


	Datum		values[] = {
		Int64GetDatum(range->rangeStart),
		Int64GetDatum(range->rangeEnd)
	};
	bool		nulls[] = {
		false,
		false
	};

	HeapTuple	tuple = heap_form_tuple(tupleDesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}


/*
 * PopSequenceNumber range returns a range of sequence numbers that can
 * be safely processed by taking the last returned sequence number as the
 * end of the range, and waiting for all concurrent writers to finish.
 *
 * The start of the range is the end of the previous range + 1.
 *
 * Note: An assumptions is that writers will only insert sequence numbers
 * that were obtained after locking the table.
 */
static SequenceNumberRange
PopSequenceNumberRange(char *pipelineName, Oid sequenceId)
{
	SequenceNumberRange range = {
		.rangeStart = 0,
		.rangeEnd = 0
	};

	Oid			relationId = InvalidOid;
	int			columnNumber = 0;

	if (!sequenceIsOwned(sequenceId, DEPENDENCY_AUTO, &relationId, &columnNumber) &&
		!sequenceIsOwned(sequenceId, DEPENDENCY_INTERNAL, &relationId, &columnNumber))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("only sequences that are owned by a table are supported")));
	}

	if (pg_class_aclcheck(sequenceId, GetUserId(), ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						get_rel_name(sequenceId))));
	}

	bool		hasRows = GetSequenceNumberRange(pipelineName, sequenceId, &range);

	if (hasRows)
	{
		LOCKTAG		tableLockTag;

		SET_LOCKTAG_RELATION(tableLockTag, MyDatabaseId, relationId);

		/*
		 * Wait for concurrent writers that may have seen sequence numbers <=
		 * the last-drawn sequence number.
		 */
		WaitForLockers(tableLockTag, ShareLock, true);

		/*
		 * We update the last-processed sequence number, which will commit or
		 * abort with the current (sub)transaction.
		 */
		UpdateLastProcessedSequenceNumber(pipelineName, range.rangeEnd);
	}

	return range;
}


/*
 * GetSequenceNumberRange reads the current state of the given sequence pipeline
 * and returns whether there are rows to process.
 */
static bool
GetSequenceNumberRange(char *pipelineName, Oid sequenceId, SequenceNumberRange * range)
{
	range->rangeStart = 0;
	range->rangeEnd = 0;

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-drawn sequence number, which may be part of a write that
	 * has not committed yet. Also block other pipeline rollups.
	 */
	char	   *query =
		"select"
		" last_processed_sequence_number + 1,"
		" pg_catalog.pg_sequence_last_value($2) seq "
		"from incremental.sequence_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1 "
		"for update";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, OIDOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		ObjectIdGetDatum(sequenceId)
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

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;
	HeapTuple	row = SPI_tuptable->vals[0];

	/* read last_processed_sequence_number + 1 result */
	bool		rangeStartIsNull = false;
	Datum		rangeStartDatum = SPI_getbinval(row, rowDesc, 1, &rangeStartIsNull);

	if (!rangeStartIsNull)
		range->rangeStart = DatumGetInt64(rangeStartDatum);

	/* read pg_sequence_last_value result */
	bool		rangeEndIsNull = false;
	Datum		rangeEndDatum = SPI_getbinval(row, rowDesc, 2, &rangeEndIsNull);

	if (!rangeEndIsNull)
		range->rangeEnd = DatumGetInt64(rangeEndDatum);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/* return whether there are rows to process */
	return !rangeEndIsNull && range->rangeStart <= range->rangeEnd;
}

/*
 * UpdateLastProcessedSequenceNumber updates the last_processed_sequence_number
 * in pipeline.pipelines to the given values.
 */
void
UpdateLastProcessedSequenceNumber(char *pipelineName, int64 lastSequenceNumber)
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
		"update incremental.sequence_pipelines "
		"set last_processed_sequence_number = $2 "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, INT8OID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		Int64GetDatum(lastSequenceNumber)
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
 * FindSequenceForRelation returns the Oid of a sequence belonging to the
 * given relation.
 *
 * We currently don't detect sequences that were manually added through
 * DEFAULT nextval(...).
 */
Oid
FindSequenceForRelation(Oid relationId)
{
	List	   *sequences = getOwnedSequences(relationId);

	if (list_length(sequences) == 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("relation \"%s\" does not have any sequences associated "
							   "with it",
							   get_rel_name(relationId)),
						errhint("Specify the name of the sequence to use for the "
								"pipeline as the argument")));

	if (list_length(sequences) > 1)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("relation \"%s\" has multiple sequences associated "
							   "with it",
							   get_rel_name(relationId)),
						errhint("Specify the name of the sequence to use for the "
								"pipeline as the argument")));

	return linitial_oid(sequences);
}
