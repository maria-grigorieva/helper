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
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%wz%'
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
         OR lower(hashtag_list) LIKE '%ww%'
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
         t.campaign as campaign,
         t.subcampaign as subcampaing,
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
         t.campaign,
         t.subcampaign,
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
         OR lower(hashtag_list) LIKE '%wwbb%'
         OR lower(hashtag_list) LIKE '%wwll%'
         OR lower(hashtag_list) LIKE '%zz%'
         OR lower(hashtag_list) LIKE '%ww%'
         OR lower(hashtag_list) LIKE '%wz%'
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



----------------
-- REAL working query
-- after meeting with Siarhei
-- 1. get all tasks for mc16a subcampaign
-- 2. get lists of hashtags for all mc16a tasks
-- 3. define phys_category for each tasks,  according to regular expressions

with mc16a_tasks as (
    select
      task.taskid,
      task.step_id
    from
      ATLAS_DEFT.t_production_task task
    where
      lower(task.campaign) = 'mc16'
      and lower(task.subcampaign) = 'mc16a'
),
  task_hashtags as (
      SELECT
        tasks.taskid,
        s_t.step_name,
        LISTAGG(hashtag.hashtag, ', ')
        WITHIN GROUP (
          ORDER BY tasks.taskid) AS hashtag_list
      FROM
        mc16a_tasks tasks,
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag,
        ATLAS_DEFT.t_production_step s,
        ATLAS_DEFT.t_step_template s_t
      WHERE
        tasks.taskid = ht_t.taskid
        and hashtag.ht_id = ht_t.ht_id
        and tasks.step_id = s.step_id
        and s.step_t_id = s_t.step_t_id
      GROUP BY
        tasks.taskid,
        s_t.step_name
  ),
  phys_categories as (
    select
      taskid,
      step_name,
      hashtag_list,
      CASE
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(charmonium|jpsi|bs|bd|bminus|bplus|charm|bottom|bottomonium|b0)+(*)')
        THEN 'BPhysics'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)btagging(*)')
        THEN 'BTag'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(diboson|zz|ww"|wz|wwbb|wwll)(*)')
        THEN 'Diboson'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(drellyan|dy)(*)')
        THEN 'DrellYan'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(exotic|monojet|blackhole|technicolor|randallsundrum|wprime|zprime|magneticmonopole|extradimensions|warpeded|contactinteraction|seesaw)(*)')
        THEN 'Exotic'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(photon|diphoton)(*)')
        THEN 'GammaJets'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(vbf|higgs|mh125|zhiggs|whiggs|bsmhiggs|chargedhiggs|bsmhiggs|smhiggs)(*)')
        THEN 'Higgs'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(minBias|minbias)(*)')
        THEN 'Minbias'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(dijet|multijet|qcd)(*)')
        THEN 'Multijet'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(performance)(*)')
        THEN 'Performance'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(singleparticle)(*)')
        THEN 'SingleParticle'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(singletop)(*)')
        THEN 'SingleTop'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(bino|susy|pmssm|leptosusy|rpv|mssm)(*)')
        THEN 'SUSY'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(triboson|triplegaugecoupling|zzw|www)(*)')
        THEN 'Triboson'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(ttbar)(*)')
        THEN 'TTbar'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(ttw|ttz|ttv|ttvv|4top)(*)')
        THEN 'TTbarX'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(upgrad)(*)')
        THEN 'Upgrade'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(w)(*)')
        THEN 'Wjets'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(z)(*)')
        THEN 'Zjets'
      ELSE 'Uncategorized'
      END as phys_category
    from task_hashtags
    where
      REGEXP_LIKE(lower(hashtag_list), '(*)(mc16a|mc16a_cp|mc16a_trig|mc16a_hpc|mc16a_pc)(*)')
  ),
  result as (
      SELECT
        task.taskid,
        task.step_name,
        task.phys_category,
        jd.nevents     AS requested_events,
        jd.neventsused AS processed_events
      FROM
        phys_categories task,
        ATLAS_PANDA.jedi_datasets jd
      WHERE
        task.taskid = jd.jeditaskid
        AND jd.masterid IS NULL
        AND jd.type IN ('input')
  )
    select
      phys_category,
      step_name,
      sum(processed_events) as processed,
      sum(requested_events) as requested
    from
      result
    group by
      phys_category,
      step_name
    order by
      phys_category,
      step_name;

-- analyze events in task chain
select
  t.taskid,
  t.total_events,
  t.total_req_events,
  jd.nevents,
  jd.neventsused,
  jd.datasetid
from
  ATLAS_DEFT.t_production_task t,
  ATLAS_PANDA.jedi_datasets jd
where
  t.taskid = jd.jeditaskid
  and jd.type in ('input')
    AND jd.masterid IS NULL
  and t.taskid in (10730520, 11190000, 11190002, 11326868, 11326870)
order by
  t.taskid;