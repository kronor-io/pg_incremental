create extension pg_incremental cascade;
create schema time_range;
set search_path to time_range;
set client_min_messages to warning;

-- create a source table
create table events (
  event_id bigint generated always as identity,
  event_time timestamptz default now(),
  client_id bigint,
  path text,
  response_time double precision
);

-- BRIN indexes are highly effective in selecting new ranges
create index on events using brin (event_id);

-- generate some random inserts
insert into events (client_id, path, response_time, event_time)
select s, '/page-' || (s % 3), random(), now() - interval '3 days' from generate_series(1,100) s;

-- create a summary table to pre-aggregate the number of events per day
create table events_agg (
  day timestamptz,
  event_count bigint,
  primary key (day)
);

select incremental.create_time_interval_pipeline('event-aggregation', '1 day',
  schedule := NULL,
  command := $$
  insert into events_agg
  select date_trunc('day', event_time), count(*)
  from events
  where event_time >= $1 and event_time < $2
  group by 1
  $$);

select count(*) from events;
select sum(event_count) from events_agg;

insert into events (client_id, path, response_time)
select s, '/page-' || (s % 3), random() from generate_series(1,100) s;

call incremental.execute_pipeline('event-aggregation');

select count(*) from events;

-- counts have not changed, because past is already processed
select sum(event_count) from events_agg;

drop schema time_range cascade;
drop extension pg_incremental;
