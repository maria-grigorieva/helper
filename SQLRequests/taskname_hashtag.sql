with
    first_part as (
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
    )
      select
        DISTINCT
        ltrim(regexp_substr(t.taskname, '^(mc)|(data)[a-z0-9A-Z_]+(\.)[0-9]+(\.)[a-z0-9A-Z_]+',1,1),'.') as phys_short,
        f.hashtag_list
      from
        first_part f
          RIGHT JOIN
        t_production_task t
          ON f.taskid = t.taskid
        order by f.hashtag_list;