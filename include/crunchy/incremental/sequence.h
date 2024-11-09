#pragma once

void		InitializeSequencePipelineState(char *pipelineName, Oid sequenceId);
void		UpdateLastProcessedSequenceNumber(char *pipelineName, int64 lastSequenceNumber);
void		ExecuteSequenceRangePipeline(char *pipelineName, char *command);
Oid			FindSequenceForRelation(Oid relationId);
