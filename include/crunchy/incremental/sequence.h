#pragma once

void		InitializeSequencePipelineState(char *pipelineName, Oid sequenceId);
void		UpdateLastProcessedSequenceNumber(char *pipelineName, int64 lastSequenceNumber);
char	   *GetSequencePipelineCommand(char *pipelineName, char *command);
Oid			FindSequenceForRelation(Oid relationId);
