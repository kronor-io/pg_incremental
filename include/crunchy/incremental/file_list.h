#pragma once

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched, char *listFunction);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command);
char	   *SanitizeListFunction(char *listFunction);
