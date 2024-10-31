# pg\_incremental: Fast, Reliable, Incremental Insert Processing in PostgreSQL

When storing a stream of event data in PostgreSQL (e.g. IoT, time series), a common challenge is to process only the new inserts. For instance, you might want to create one or more rollup tables containing pre-aggregated data, and insert and update the aggregates as new data arrives. However, you cannot really know the data that is still being inserted by concurrent transactions, and immediately aggregating data when inserting (e.g. via triggers) is certain to create a concurrency bottleneck. When periodically repeating an aggregation, you also want to make sure that events are processed successfully exactly once, even when queries fail.

`pg_incremental` is a simple extension that helps you do fast, reliable, incremental batch processing of inserts in PostgreSQL.

## Creating incremental processing pipelines

To use `pg_incremental`, you need to have a bigserial or bigint identity column in your table that serves as a unique row identifier, with an index (e.g. btree / primary key, BRIN). 

For example, consider this source table and rollup table:
```sql
-- create a source table
create table events (
  event_id bigint generated always as identity,
  event_time timestamptz default now(),
  client_id bigint,
  path text,
  response_time double precision,
  primary key (event_id)
);
insert into events (client_id, path, response_time)
select s, '/', random() from generate_series(1,1000000) s;

-- crete a rollup table to pre-aggregate the number of events per day
create table events_agg (
  day date,
  event_count bigint,
  primary key (day)
);

```

You can then define a "pipeline" with the `incremental.create_sequence_pipeline` function by giving it the name of a source table name with a sequence, or an explicit sequence name. The query you pass will be executed in a context where $1 and $2 are set to the lowest and highest value of a range of sequence values that can be safely aggregated, because the pipeline execution makes sure that all ongoing transactions are only genearting higher values by waiting for table locks. 

```sql
-- create a pipeline from a postgres table using a sequence
select incremental.create_sequence_pipeline('event-aggregation', 'events', $$
  insert into events_agg
  select date_trunc('day', event_time), count(*)
  from events
  where event_id between $1 and $2
  group by 1
  on conflict (day) do update set event_count = events_agg.event_count + excluded.event_count;
$$);
```

Creating a pipeline automatically sets up a [pg_cron](https://github.com/citusdata/pg_cron) job that runs every 5 minutes by default (configurable) and runs the insert..select with an unprocessed sequence range. You can define multiple pipelines for the same source table to create different aggregations. 

## Resetting an incremental processing pipelines

If you need to rebuild an aggregation you can reset a pipeline to the beginning.
```sql
-- Reset a rollup
begin;
truncate events_agg
select incremental.reset_pipeline('event-aggregation');
commit;
```
The next time the pipeline runs, the start of the sequence value range will be set to 0 to reprocess all rows.

## Dropping an incremental processing pipelines

When you are done with a pipeline, you can drop it using `incremental.drop_pipline(..)`:
```sql
-- Drop the pipeline
select incremental.drop_pipeline('event-aggregation');
```
