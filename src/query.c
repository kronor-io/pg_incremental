#include "postgres.h"

#include "crunchy/incremental/query.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/ruleutils.h"


/*
 * ParseQuery parses a query string.
 *
 * The function parses the query and returns the query tree.
 */
Query *
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
		pg_analyze_and_rewrite_params(rawStmt, command, NULL, NULL, NULL);

	/* we already checked parseTreeList lenght above */
	Assert(list_length(queryTreeList) == 1);

	return (Query *) linitial(queryTreeList);
}


/*
 * DeparsQuery deparses a Query AST.
 */
char *
DeparseQuery(Query *query)
{
	int			save_nestlevel = NewGUCNestLevel();

	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	bool		pretty = false;
	char	   *newQuery = nodeToString(query, pretty);

	AtEOXact_GUC(true, save_nestlevel);

	return newQuery;
}


/*
 * ExecuteCommand executes a SQL command.
 */
void
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
