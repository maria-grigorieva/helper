-- Query to get generated events and requested events
-- for tasks from DEFT
-- TODO:
-- t_production_task.total_req_events doesn't allow
-- to get the number of requested events,
-- because if requested all events, then this field is set to 0
-- So, This metadata must be taken from JEDI
select
    lower(t.campaign) as campaign,
    lower(t.subcampaign) as subcampaing,
    t.phys_group as phys_group,
    t.project as project,
    s_t.step_name as step,
    t.pr_id as request,
    t.status as status,
    t.taskid as task_id,
    t.taskname as taskname,
    listagg(hashtag.hashtag,
    ',') within
group (order by
    ht_t.taskid) as hashtag_list,
    t.total_events as events_total,
    t.total_req_events as t_events_requested,
    t.timestamp as t_stamp
FROM
    t_production_task t,
    atlas_deft.t_ht_to_task ht_t,
    atlas_deft.t_hashtag hashtag,
    t_production_step s,
    t_step_template s_t
WHERE
    t.taskid = ht_t.taskid
    and ht_t.ht_id = hashtag.ht_id
    and lower(t.campaign) = 'mc16'
    and lower(t.subcampaign) = 'mc16a'
    and t.step_id = s.step_id
    and s.step_t_id = s_t.step_t_id
GROUP BY
    lower(t.campaign),
    lower(t.subcampaign),
    t.phys_group,
    t.project,
    s_t.step_name,
    t.pr_id,
    t.status,
    t.taskid,
    t.taskname,
    t.total_events,
    t.total_req_events,
    t.timestamp;

-- Query to get numbers of requested events for tasks from JEDI,
-- categorized by a lists of hashtags from DEFT
-- restricted for campaign mc16/mc16a
select
   lower(t.campaign) as campaign,
   lower(t.subcampaign) as subcampaing,
   t.phys_group as phys_group,
   t.project as project,
   s_t.step_name as step,
   t.pr_id as request,
   t.status as status,
   t.taskid as task_id,
   t.taskname as taskname,
   LISTAGG(hashtag.hashtag,',') within group (order by ht_t.taskid) as hashtag_list,
   t.total_events as events_total,
   sum(decode(jdc.startevent,null,jdc.nevents,jdc.endevent-jdc.startevent+1)) as t_events_requested,
   t.timestamp as t_stamp
FROM
   ATLAS_DEFT.t_production_task t,
   ATLAS_DEFT.t_ht_to_task ht_t,
   ATLAS_DEFT.t_hashtag hashtag,
   ATLAS_DEFT.t_production_step s,
   ATLAS_DEFT.t_step_template s_t,
   ATLAS_PANDA.jedi_datasets jd,
   ATLAS_PANDA.jedi_dataset_contents jdc
WHERE
   t.taskid = ht_t.taskid
   and ht_t.ht_id = hashtag.ht_id
   and lower(t.campaign) = 'mc16'
   and lower(t.subcampaign) = 'mc16a'
   and t.step_id = s.step_id
   and s.step_t_id = s_t.step_t_id
   and t.taskid = jd.jeditaskid
   and jdc.datasetid = jd.datasetid
   and jd.masterid is null
   and jd.jeditaskid = jdc.jeditaskid
   and jdc.type in ('input', 'pseudo_input')
GROUP BY
   lower(t.campaign),
   lower(t.subcampaign),
   t.phys_group,
   t.project,
   s_t.step_name,
   t.pr_id,
   t.status,
   t.taskid,
   t.taskname,
   t.total_events,
   t.timestamp;

-- Restricted DEFT Query
-- for Diboson: 'diboson','ZZ', 'WW', 'WZ', 'WWbb', 'WWll', 'zz', 'ww', 'wz', 'wwbb','wwll'
with
    all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
    hashtag_mc16a as (
     select
       taskid,
       hashtag_list
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
     )
       select
         lower(t.campaign) as campaign,
         lower(t.subcampaign) as subcampaing,
         t.phys_group as phys_group,
         t.project as project,
         t.pr_id as request,
         s_t.step_name as step,
         t.status as task_status,
         t.taskid as task_id,
         t.taskname as taskname,
         t.total_events as events_total,
         t.timestamp as task_timestamp,
         h.hashtag_list as tag_list
      FROM
         hashtag_mc16a h
         LEFT JOIN
         ATLAS_DEFT.t_production_task t
        ON h.taskid = t.taskid
         JOIN
         ATLAS_DEFT.t_production_step s
           ON t.step_id = s.step_id
         JOIN
          ATLAS_DEFT.t_step_template s_t
           ON s.step_t_id = s_t.step_t_id
      GROUP BY
         lower(t.campaign),
         lower(t.subcampaign),
         t.phys_group,
         t.project,
         s_t.step_name,
         t.pr_id,
         t.status,
         t.taskid,
         t.taskname,
         t.total_events,
         t.timestamp,
         h.hashtag_list;

-- Restricted DEFT/JEDI Query
with
    all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
    hashtag_mc16a as (
     select
       taskid,
       hashtag_list
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
     ),
    total_events as (
       select
         lower(t.campaign) as campaign,
         lower(t.subcampaign) as subcampaing,
         t.phys_group as phys_group,
         t.project as project,
         t.pr_id as request,
         s_t.step_name as step,
         t.status as task_status,
         t.taskid as task_id,
         t.taskname as taskname,
         t.total_events as events_total,
         t.timestamp as task_timestamp,
         h.hashtag_list as tag_list
      FROM
         hashtag_mc16a h
         LEFT JOIN
         ATLAS_DEFT.t_production_task t
        ON h.taskid = t.taskid
         JOIN
         ATLAS_DEFT.t_production_step s
           ON t.step_id = s.step_id
         JOIN
          ATLAS_DEFT.t_step_template s_t
           ON s.step_t_id = s_t.step_t_id
      WHERE
        s_t.step_name LIKE 'evgen'
      GROUP BY
         lower(t.campaign),
         lower(t.subcampaign),
         t.phys_group,
         t.project,
         s_t.step_name,
         t.pr_id,
         t.status,
         t.taskid,
         t.taskname,
         t.total_events,
         t.timestamp,
         h.hashtag_list
    )
    SELECT
      evt.campaign,
      evt.subcampaing,
      evt.phys_group,
      evt.project,
      evt.request,
      evt.step,
      evt.task_status,
      evt.task_id,
      evt.taskname,
      evt.events_total,
      sum(decode(jdc.startevent,null,jdc.nevents,jdc.endevent-jdc.startevent+1)) as events_requested,
      evt.task_timestamp,
      evt.tag_list
    FROM
      total_events evt,
      ATLAS_PANDA.jedi_datasets jd,
      ATLAS_PANDA.jedi_dataset_contents jdc
    WHERE
     evt.task_id = jd.jeditaskid
     and jdc.datasetid = jd.datasetid
     and jd.masterid is null
     and jd.jeditaskid = jdc.jeditaskid
     and jdc.type in ('input', 'pseudo_input')
   GROUP BY
     evt.campaign,
      evt.subcampaing,
      evt.phys_group,
      evt.project,
      evt.request,
      evt.step,
      evt.task_status,
      evt.task_id,
      evt.taskname,
      evt.events_total,
      evt.task_timestamp,
      evt.tag_list;


-- get requested events from JEDI
-- for task_id
select
  jdc.jeditaskid,
  sum(decode(jdc.startevent,NULL,jdc.nevents,jdc.endevent-jdc.startevent+1)) AS NEVENTS
from
  ATLAS_PANDA.jedi_datasets jd,
  ATLAS_PANDA.jedi_dataset_contents jdc
WHERE
  jd.jeditaskid in (11503256, 11448574, 11502417, 11325711)
  and jdc.datasetid = jd.datasetid
  and jd.masterid IS NULL
  and jdc.type in ('input','pseudo-input')
group by
  jdc.jeditaskid;


-- select tasks
with
 all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   )
     select
       taskid
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc';

-- the same, but restricted to 10 tasks
with
 all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
  selected_tasks as (
     select
       taskid
       from
         hashtag_diboson
       where
          rownum<=10 and
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
  )
select
  jdc.jeditaskid,
  sum(decode(jdc.startevent,NULL,jdc.nevents,jdc.endevent-jdc.startevent+1)) AS req_events
from
  selected_tasks t,
  ATLAS_PANDA.jedi_datasets jd,
  ATLAS_PANDA.jedi_dataset_contents jdc
WHERE
  t.taskid = jd.jeditaskid
  and jdc.datasetid = jd.datasetid
  and jd.masterid IS NULL
  and jdc.type in ('input','pseudo-input')
group by
  jdc.jeditaskid;

-- RESULT:
-- JEDITASKID REQ_EVENTS
-- ---------- ----------
--   10837720    1325000
--   10730379    2400000
--   10837722     330000
--   10730382    1800000
--   10837713    5000000
--   10730388    1800000
--   10837716    3335000
--   10837718     330000
--   10730372    2400000
--   10730367    2400000
--
-- 10 rows selected.
--
-- Elapsed: 00:02:30.01


-- the same, restricted to 10 tasks
-- without using jedi_dataset_contants table
with
 all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
  selected_tasks as (
     select
       taskid
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
  )
select
  jdc.jeditaskid,
  sum(decode(jdc.startevent,NULL,jdc.nevents,jdc.endevent-jdc.startevent+1)) AS input_events
from
  selected_tasks t,
  ATLAS_PANDA.jedi_datasets jd,
  ATLAS_PANDA.jedi_dataset_contents jdc
WHERE
  t.taskid = jd.jeditaskid
  and jdc.datasetid = jd.datasetid
  and jd.masterid IS NULL
  and jdc.type in ('input','pseudo-input')
GROUP BY
  jdc.jeditaskid;


-- 100 rows selected.
-- Elapsed: 00:00:49.53

-- 269 rows selected.
-- Elapsed: 00:04:01.04

-- DEFT/JEDI request
-- req_events are taken from jed_datasets.nevents
with
    all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
    hashtag_mc16a as (
     select
       taskid,
       hashtag_list
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
     ),
    total_events as (
       select
         lower(t.campaign) as campaign,
         lower(t.subcampaign) as subcampaing,
         t.phys_group as phys_group,
         t.project as project,
         t.pr_id as request,
         s_t.step_name as step,
         t.status as task_status,
         t.taskid as task_id,
         t.taskname as taskname,
         t.total_events as events_total,
         t.timestamp as task_timestamp,
         h.hashtag_list as tag_list
      FROM
         hashtag_mc16a h
         LEFT JOIN
         ATLAS_DEFT.t_production_task t
        ON h.taskid = t.taskid
         JOIN
         ATLAS_DEFT.t_production_step s
           ON t.step_id = s.step_id
         JOIN
          ATLAS_DEFT.t_step_template s_t
           ON s.step_t_id = s_t.step_t_id
      GROUP BY
         lower(t.campaign),
         lower(t.subcampaign),
         t.phys_group,
         t.project,
         s_t.step_name,
         t.pr_id,
         t.status,
         t.taskid,
         t.taskname,
         t.total_events,
         t.timestamp,
         h.hashtag_list
    )
    SELECT
      evt.campaign,
      evt.subcampaing,
      evt.phys_group,
      evt.project,
      evt.request,
      evt.step,
      evt.task_status,
      evt.task_id,
      evt.taskname,
      evt.events_total,
      jd.nevents as events_requested,
      evt.task_timestamp,
      evt.tag_list
    FROM
      total_events evt,
      ATLAS_PANDA.jedi_datasets jd
    WHERE
     evt.task_id = jd.jeditaskid
     and jd.masterid is null
   GROUP BY
     evt.campaign,
      evt.subcampaing,
      evt.phys_group,
      evt.project,
      evt.request,
      evt.step,
      evt.task_status,
      evt.task_id,
      evt.taskname,
      evt.events_total,
      jd.nevents,
      evt.task_timestamp,
      evt.tag_list;


-- the latest

with
    all_hashtags as (
      select
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag
      where
        hashtag.ht_id = ht_t.ht_id
      GROUP BY
        taskid
    ),
    hashtag_diboson as (
       SELECT
         taskid,
         hashtag_list
       FROM
         all_hashtags
       WHERE
         lower(hashtag_list) LIKE '%diboson%'
         OR lower(hashtag_list) LIKE '%ZZ%'
         OR lower(hashtag_list) LIKE '%WW%'
         OR lower(hashtag_list) LIKE '%WZ%'
         OR lower(hashtag_list) LIKE '%WWbb%'
         OR lower(hashtag_list) LIKE '%WWll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
   ),
    hashtag_mc16a as (
     select
       taskid,
       hashtag_list
       from
         hashtag_diboson
       where
          lower(hashtag_list) LIKE '%mc16a%'
     )
       select
         t.taskid as task_id,
         t.total_events as events_total
      FROM
         hashtag_mc16a h
         LEFT JOIN
         ATLAS_DEFT.t_production_task t
        ON h.taskid = t.taskid
         JOIN
         ATLAS_DEFT.t_production_step s
           ON t.step_id = s.step_id
         JOIN
          ATLAS_DEFT.t_step_template s_t
           ON s.step_t_id = s_t.step_t_id
      WHERE lower(s_t.step_name) = 'simul';