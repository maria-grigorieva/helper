with mc16a_tasks as (
    select
      t.campaign,
      t.taskid,
      t.step_id,
      t.taskname,
      t.subcampaign,
      t.project,
      t.phys_group,
      t.status,
      t.pr_id,
      r.description,
      r.reference_link,
      r.energy_gev
    from
      ATLAS_DEFT.t_production_task t,
      ATLAS_DEFT.t_prodmanager_request r
    where
      lower(t.campaign) = 'mc16'
      and t.status in ('done','finished')
      and t.pr_id = r.pr_id
),
  task_hashtags as (
      SELECT
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        s_t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.description,
        t.reference_link,
        t.energy_gev,
        LISTAGG(hashtag.hashtag, ', ')
        WITHIN GROUP (
          ORDER BY t.taskid) AS hashtag_list
      FROM
        mc16a_tasks t,
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag,
        ATLAS_DEFT.t_production_step s,
        ATLAS_DEFT.t_step_template s_t
      WHERE
        t.taskid = ht_t.taskid
        and hashtag.ht_id = ht_t.ht_id
        and t.step_id = s.step_id
        and s.step_t_id = s_t.step_t_id
      GROUP BY
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        s_t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.description,
        t.reference_link,
        t.energy_gev
  ),
  phys_categories as (
    select
      campaign,
      subcampaign,
      phys_group,
      project,
      pr_id,
      step_name,
      status,
      taskid,
      taskname,
      description,
      reference_link,
      energy_gev,
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
      REGEXP_LIKE(lower(hashtag_list), '(*)((mc16[a-z]?)|(mc16[a-z]?_cp)|(mc16[a-z]?_trig)|(mc16[a-z]?_hpc)|(mc16[a-z]?_pc)|(mc16[a-z]?campaign))(*)')
  )
      SELECT
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.hashtag_list,
        t.phys_category,
        description,
        reference_link,
        energy_gev,
        jd.status as dataset_status,
        jd.nevents AS requested_events,
        jd.neventsused AS processed_events
      FROM
        phys_categories t,
        ATLAS_PANDA.jedi_datasets jd
      WHERE
        t.taskid = jd.jeditaskid
        AND jd.masterid IS NULL
        AND jd.type IN ('input')
        and jd.status in ('ready', 'done', 'finished');