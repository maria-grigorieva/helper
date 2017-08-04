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
      step_name