#pragma once

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command);
