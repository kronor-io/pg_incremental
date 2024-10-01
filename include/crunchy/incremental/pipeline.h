#pragma once

#define SEQUENCE_PIPELINE 's'
#define FILE_PIPELINE 'f'

typedef char PipelineType;

/*
 * PipelineDesc describes a pipeline.
 */
typedef struct PipelineDesc
{
	/* name of the pipeline */
	char	   *pipelineName;

	/* type of the pipeline */
	PipelineType pipelineType;

	/* user ID of the pipeline owner */
	Oid			ownerId;

	/* OID of the source relation or sequence */
	Oid			sourceRelationId;

	/* command to run for the pipeline */
	char	   *command;
}			PipelineDesc;

PipelineDesc *ReadPipelineDesc(char *pipelineName);
