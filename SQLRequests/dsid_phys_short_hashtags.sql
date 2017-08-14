with
    first_one as (
      select
        distinct
        taskid,
        LISTAGG(hashtag.hashtag, ', ')
          within group (order by ht_t.taskid) as hashtag_list
      from
        atlas_deft.t_ht_to_task ht_t,
        atlas_deft.t_hashtag hashtag
      where
        hashtag.ht_id=ht_t.ht_id
      GROUP BY
        taskid
    ),
    first_two as (
      select
        DISTINCT
        ltrim(regexp_substr(t.taskname, '(\.){1}[0-9]+',1,1),'.') as dataset_id,
        ltrim(regexp_substr(t.taskname, '(\.)[0-9]+(\.)[a-z0-9A-Z_]+',1,1), regexp_substr(t.taskname, '(\.){1}[0-9]+',1,1)) as phys_short,
        f.hashtag_list
      from
        first_one f,
        t_production_task t
      where
        t.taskid = f.taskid
        and regexp_like(t.taskname, '^(mc)|(data).+')
    ),
    second_one as (
      select
        distinct ltrim(regexp_substr(taskname, '(\.){1}[0-9]+',1,1),'.') as dataset_id,
        ltrim(regexp_substr(taskname, '(\.)[0-9]+(\.)[a-z0-9A-Z_]+',1,1), regexp_substr(taskname, '(\.){1}[0-9]+',1,1)) as phys_short
      from
        t_production_task
      where
        regexp_like(taskname, '^(mc)|(data).+')
   )
    select
      s.dataset_id,
      s.phys_short,
      f.hashtag_list
    from
      first_two f
      RIGHT JOIN
      second_one s
      ON
      f.dataset_id = s.dataset_id
    order by
      f.hashtag_list;