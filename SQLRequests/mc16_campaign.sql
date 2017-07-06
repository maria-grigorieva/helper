with mc16a_tasks as (
    select
      task.taskid,
      task.step_id,
      task.taskname,
      task.subcampaign,
      task.project,
      task.phys_group,
      task.status,
      task.pr_id
    from
      ATLAS_DEFT.t_production_task task
    where
      lower(task.campaign) = 'mc16'
      and task.status in ('done','finished')
),
  task_hashtags as (
      SELECT
        tasks.subcampaign,
        tasks.phys_group,
        tasks.project,
        tasks.pr_id,
        s_t.step_name,
        tasks.status,
        tasks.taskid,
        tasks.taskname,
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
        tasks.subcampaign,
        tasks.phys_group,
        tasks.project,
        tasks.pr_id,
        s_t.step_name,
        tasks.status,
        tasks.taskid,
        tasks.taskname
  ),
  phys_categories as (
    select
      subcampaign,
      phys_group,
      project,
      pr_id,
      step_name,
      status,
      taskid,
      taskname,
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
      REGEXP_LIKE(lower(hashtag_list), '(*)((mc16[a-z]?)|(mc16[a-z]?_cp)|(mc16[a-z]?_trig)|(mc16[a-z]?_hpc)|(mc16[a-z]?_pc)|(mc16[a-z]?campaign)|(mc16[a-z]?))(*)')
  ),
  result as (
      SELECT
        task.subcampaign,
        task.phys_group,
        task.project,
        task.pr_id,
        task.step_name,
        task.status,
        task.taskid,
        task.taskname,
        task.hashtag_list,
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
  subcampaign,
  phys_category,
  step_name,
  sum(requested_events) as requested,
  sum(processed_events) as processed
from
  result
group by
  subcampaign,
  phys_category,
  step_name
order by
  subcampaign,
  phys_category,
  step_name;