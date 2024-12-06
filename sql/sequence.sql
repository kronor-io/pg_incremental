create extension pg_incremental cascade;
create schema sequence;
set search_path to sequence;

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
insert into events (client_id, path, response_time)
select s, '/page-' || (s % 3), random() from generate_series(1,100) s;

-- create a summary table to pre-aggregate the number of events per day
create table events_agg (
  day timestamptz,
  event_count bigint,
  primary key (day)
);

select incremental.create_sequence_pipeline('event-aggregation',
  sequence_name := 'events',
  schedule := NULL,
  command := $$
  insert into events_agg
  select date_trunc('day', event_time), count(*)
  from events
  where event_id between $1 and $2
  group by 1
  on conflict (day) do update set event_count = events_agg.event_count + excluded.event_count;
  $$);

select count(*) from events;
select sum(event_count) from events_agg;

call incremental.execute_pipeline('event-aggregation');

insert into events (client_id, path, response_time)
select s, '/page-' || (s % 3), random() from generate_series(1,100) s;

call incremental.execute_pipeline('event-aggregation');

select count(*) from events;
select sum(event_count) from events_agg;

drop schema sequence cascade;
drop extension pg_incremental;
