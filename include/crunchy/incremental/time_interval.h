#pragma once

void		InitializeTimeRangePipelineState(char *pipelineName, Interval *timeInterval, Interval *minDelay);
void		UpdateLastProcessedTimeInterval(char *pipelineName, TimestampTz lastProcessedTime);
void		ExecuteTimeIntervalPipeline(char *pipelineName, char *command);
