#pragma once

#define DEFAULT_FILE_LIST_FUNCTION "crunchy_lake.list_files"

extern char *DefaultFileListFunction;

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched, char *listFunction, int maxBatchSize);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command);
char	   *SanitizeListFunction(char *listFunction);
void		InsertProcessedFile(char *pipelineName, char *path);
