#pragma once

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

Query *ParseQuery(char *command, List *paramTypes);
char *DeparseQuery(Query *query);
void ExecuteCommand(char *commandString);
