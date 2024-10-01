#pragma once

int64		ScheduleCronJob(char *jobName, char *schedule, char *command);
void		UnscheduleCronJob(char *jobName);
