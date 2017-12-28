# -*- coding: utf-8 -*-
import DButils
import csv
import time
import ConfigParser
from DocumentProcessing import FileConnector
import cx_Oracle
import json

Config = ConfigParser.ConfigParser()
Config.read("settings.cfg")
dsn = Config.get("oracle", "dsn")
print dsn
conn, cursor = DButils.connectDEFT_DSN(dsn)


__deft_projects = "select * from t_projects where project = 'mc16_13TeV'"
__step_template = "select * from t_step_template where step_name = 'Simul'"
__jedi_dataset = "select * from atlas_panda.jedi_datasets where rownum<10"
__jedi_dataset_contents = "select * from atlas_panda.jedi_dataset_contents where rownum<=10"

__sql_get_request = "select * from t_prodmanager_request where pr_id = %"
__sql_get_production_step_request_id = "select * from t_production_step where pr_id = %"
__sql_containers = "select parent_tid, name from t_production_container where rownum <=10"
__sql_tags = "select * from atlas_deft.t_production_tag where rownum<=10"

__sql_get_campaign_subcampaign = "select * from t_prodmanager_request where lower(campaign) = 'mc16' and lower(sub_campaign) = 'mc16a'"
__sql_get_finished_requests = "select pr_id, description, reference_link, phys_group, energy_gev, project " \
                              "from t_prodmanager_request where lower(campaign) = 'mc16' and " \
                              "lower(sub_campaign) = 'mc16a' and status = 'finished' and request_type='MC'"

__sql_get_step_template_for_request_id = "select t.step_name, t.swrelease, t.trf_name, t.lparams, t.vparams from t_production_step s, t_step_template t where s.pr_id=11869 and s.step_id = t.step_name"
__sql_get_step_template = "select * from t_step_template where rownum <= 10"
__sql_get_tasks_for_request = "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
                              "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spaceing, t.pileup as pileup " \
                              "from t_production_task t, t_prodmanager_request r " \
                              "where t.pr_id = r.pr_id and " \
                              "lower(r.campaign) = 'mc16' and lower(r.sub_campaign) = 'mc16a'" \
                              "and t.status in ('done','finished')"
_sql_get_request_task_container = "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
                  "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spacing," \
                  "cont.name as container_name " \
                  "from t_production_task t, t_prodmanager_request r, t_production_container cont " \
                  "where t.pr_id = r.pr_id and cont.parent_tid = t.taskid and " \
                  "lower(r.campaign) = 'mc16' and lower(r.sub_campaign) = 'mc16a'" \
                  "and t.status in ('done','finished')"

__sql_request_task_container_tag = "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
                  "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spacing," \
                  "cont.name as container_name, tag.name as tag_name, tag.trf_name as trf_name, tag.trf_release as trf_release " \
                  "from t_production_task t, t_prodmanager_request r, t_production_container cont, atlas_deft.t_production_tag tag " \
                  "where t.pr_id = r.pr_id and cont.parent_tid = t.taskid and tag.taskid = t.taskid " \
                  "and lower(r.campaign) = 'mc16' and lower(r.sub_campaign) = 'mc16a'" \
                  "and t.status in ('done','finished')"

__sql_request_task_container__tag_without_log = "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
                  "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spacing," \
                  "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) as cont_type," \
                  "cont.name as container_name, tag.name as tag_name, tag.trf_name as trf_name, tag.trf_release as trf_release " \
                  "from t_production_task t, t_prodmanager_request r, t_production_container cont, atlas_deft.t_production_tag tag " \
                  "where t.pr_id = r.pr_id and cont.parent_tid = t.taskid and tag.taskid = t.taskid " \
                  "and lower(r.campaign) = 'mc16' and lower(r.sub_campaign) = 'mc16a'" \
                  "and t.status in ('done','finished') and " \
                  "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) != 'log'" \
                  "ORDER BY r_group, r_project, r_id, r_descr, taskname, t_status, cont_type, container_name"

__sql_request_task_container_tag_hash = "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
                  "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spacing," \
                  "LISTAGG(hashtag.hashtag, ',') within group (order by ht_t.taskid) as hashtag_list, "\
                  "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) as cont_type," \
                  "cont.name as container_name, tag.name as tag_name, tag.trf_name as trf_name, tag.trf_release as trf_release " \
                  "from t_production_task t, t_prodmanager_request r, t_production_container cont, atlas_deft.t_production_tag tag, " \
                  "atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag " \
                  "where t.pr_id = r.pr_id and cont.parent_tid = t.taskid and tag.taskid = t.taskid " \
                  "and t.taskid = ht_t.taskid and ht_t.ht_id = hashtag.ht_id " \
                  "and lower(r.campaign) = 'mc16' and lower(r.sub_campaign) = 'mc16a'" \
                  "and t.status in ('done','finished') and " \
                  "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) != 'log'" \
                  "ORDER BY r_group, r_project, r_id, r_descr, taskname, t_status, cont_type, container_name"


__sql_hashtags_list = "select taskid, LISTAGG(h.hashtag, ',') within group (order by th.taskid) as ht_id_list "\
                      "from atlas_deft.t_ht_to_task th, atlas_deft.t_hashtag h where h.ht_id=th.ht_id " \
                      "GROUP BY taskid"

__sql = "with hashtags as ( "\
        "select taskid, LISTAGG(h.hashtag, ', ') within group (order by th.taskid) as ht_id_list "\
        "from atlas_deft.t_ht_to_task th, atlas_deft.t_hashtag h where h.ht_id=th.ht_id " \
        "GROUP BY taskid), "\
        "aggregate as (" \
            "select r.phys_group as r_group, r.pr_id as r_id, r.project as r_project, r.description as r_descr, " \
              "t.taskid as taskid, t.taskname as taskname, t.status as t_status, t.bunchspacing as bunch_spacing," \
              "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) as cont_type," \
              "cont.name as container_name, tag.name as tag_name, tag.trf_name as trf_name, tag.trf_release as trf_release, " \
              "r.campaign as campaign, r.sub_campaign as subcampaign "\
              "from t_production_task t, t_prodmanager_request r, t_production_container cont, atlas_deft.t_production_tag tag " \
              "where t.pr_id = r.pr_id and cont.parent_tid = t.taskid and tag.taskid = t.taskid " \
              "and lower(r.campaign) = 'mc16'" \
              "and t.status in ('done','finished') and " \
              "substr(cont.name, instrc (cont.name,'.',1,4)+1,instrc(cont.name,'.',1,5)-instrc(cont.name,'.',1,4)-1) != 'log'" \
              "ORDER BY r_group, r_project, r_id, r_descr, taskname, t_status, cont_type, container_name "\
        ") "\
        "select a.r_group phys_group, a.r_id as request, a.r_project as project, a.r_descr as description, a.taskid as taskID, h.ht_id_list as keywords, a.taskname as taskname, a.t_status as task_status," \
        "a.cont_type as container_type, a.container_name as container_name, a.tag_name as tag_name, a.trf_name, a.trf_release, a.campaign as campaign, a.subcampaign as subcampaign from aggregate a LEFT OUTER JOIN hashtags h "\
        " ON h.taskid = a.taskid " \
        "ORDER BY campaign, subcampaign, phys_group, project, description, request, keywords, taskid, task_status, container_type, container_name"

__sql_TASKS_PROD_AGGREGATE = "select * from ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE where rownum<=100";
__sql_TASKS_PROD_AGGREGATE_COUNTER = "select count(*) from ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE";

"""
    ElasticSearch version 1.0
    Wrong production step!
"""
__sql_for_es = '''
    select
        lower(t.campaign) as campaign,
        lower(t.subcampaign) as subcampaing,
        t.phys_group as phys_group,
        t.project as project,
        substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step,
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
        atlas_deft.t_hashtag hashtag
    WHERE
        t.taskid = ht_t.taskid
        and ht_t.ht_id = hashtag.ht_id
        and lower(t.campaign) = 'mc16'
        and lower(t.subcampaign) = 'mc16a'
    GROUP BY
        lower(t.campaign),
        lower(t.subcampaign),
        t.phys_group,
        t.project,
        substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1),
        t.pr_id,
        t.status,
        t.taskid,
        t.taskname,
        t.total_events,
        t.total_req_events,
        t.timestamp
'''

"""
    TO ElasticSearch
    version 2.0
    Right Production Step
"""
__sql_for_es_with_step = '''
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
        t.timestamp
'''

__sql_for_es_with_step_restricted = '''
    with result as (
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
            t.timestamp
            )
            select
              campaign,
              subcampaing,
              phys_group,
              project,
              step,
              request,
              status,
              task_id,
              taskname,
              hashtag_list,
              events_total,
              t_events_requested,
              t_stamp
            from
              result
            where
              lower(hashtag_list) LIKE '%mc16a%'
              OR lower(hashtag_list) LIKE '%mc16a_cp'
              OR lower(hashtag_list) LIKE '%mc16a_trig'
              OR lower(hashtag_list) LIKE '%mc16a_hpc'
              OR lower(hashtag_list) LIKE '%mc16a_pc'
'''


__sql_for_es_only_tasks = '''
    with result as (
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
            t.timestamp
            )
            select
              listagg(task_id, ', ') within group (order by task_id) selected_tasks
            from
              result
            where
              rownum<=100 and
              (lower(hashtag_list) LIKE '%mc16a%'
              OR lower(hashtag_list) LIKE '%mc16a_cp'
              OR lower(hashtag_list) LIKE '%mc16a_trig'
              OR lower(hashtag_list) LIKE '%mc16a_hpc'
              OR lower(hashtag_list) LIKE '%mc16a_pc')
'''

#listagg(task_id, ', ') within group (order by task_id) selected_tasks

__sql_for_es_containers = '''
    select
        lower(t.campaign) as campaign,
        lower(t.subcampaign) as subcampaing,
        t.phys_group as phys_group,
        t.project as project,
        substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step,
        t.pr_id as request,
        t.status as status,
        t.taskid as task_id,
        t.taskname as taskname,
        listagg(hashtag.hashtag,
        ',') within
    group (order by
        ht_t.taskid) as hashtag_list,
        tag.name as tag_name, 
        tag.trf_name as trf_name, 
        tag.trf_release as trf_release
    FROM
        t_production_task t,
        atlas_deft.t_ht_to_task ht_t,
        atlas_deft.t_hashtag hashtag,
        atlas_deft.t_production_tag tag
    WHERE
        t.taskid = ht_t.taskid
        and ht_t.ht_id = hashtag.ht_id
        and lower(t.campaign) = 'mc16'
        and lower(t.subcampaign) = 'mc16a'
        and tag.taskid = t.taskid
    GROUP BY
        lower(t.campaign),
        lower(t.subcampaign),
        t.phys_group,
        t.project,
        substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1),
        t.pr_id,
        t.status,
        t.taskid,
        t.taskname,
        tag.name,
        tag.trf_name,
        tag.trf_release
'''

__sql_for_es_containers_with_step = '''
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
        tag.name as tag_name,
        tag.trf_name as trf_name,
        tag.trf_release as trf_release
    FROM
        t_production_task t,
        atlas_deft.t_ht_to_task ht_t,
        atlas_deft.t_hashtag hashtag,
        atlas_deft.t_production_tag tag,
        t_production_step s,
        t_step_template s_t
    WHERE
        t.taskid = ht_t.taskid
        and ht_t.ht_id = hashtag.ht_id
        and lower(t.campaign) = 'mc16'
        and lower(t.subcampaign) = 'mc16a'
        and tag.taskid = t.taskid
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
        tag.name,
        tag.trf_name,
        tag.trf_release
'''


__hashtag_request = '''
    with 
        hashtags as (
          select 
            taskid, 
            LISTAGG(hashtag.hashtag, ', ') 
              within group (order by ht_t.taskid) as hashtag_list
          from atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag where hashtag.ht_id=ht_t.ht_id
          GROUP BY taskid
        ), 
        requests as (
            select 
              r.pr_id, 
              r.description, 
              r.reference_link, 
              r.phys_group, 
              r.energy_gev, 
              r.project,
              t.taskid
            from 
              t_prodmanager_request r, 
              t_production_task t
            where 
              r.pr_id = t.pr_id 
              and lower(t.campaign) = 'mc16'
              and lower(t.subcampaign) = 'mc16a'
        )
        select 
          r.pr_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.taskid,
          h.hashtag_list
        from 
          requests r
          LEFT JOIN
          hashtags h
          ON h.taskid = r.taskid
        group by 
          r.pr_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.taskid,
          h.hashtag_list
'''


__hashtag_request_task = '''
    with 
        hashtags as (
          select 
            taskid, 
            LISTAGG(hashtag.hashtag, ', ') 
              within group (order by ht_t.taskid) as hashtag_list
          from atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag where hashtag.ht_id=ht_t.ht_id
          GROUP BY taskid
        ), 
        requests as (
            select 
              r.pr_id, 
              r.description, 
              r.reference_link, 
              r.phys_group, 
              r.energy_gev, 
              r.project,
              substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step,
              t.taskid,
              t.taskname,
              t.status,
              t.timestamp
            from 
              t_prodmanager_request r, 
              t_production_task t
            where 
              r.pr_id = t.pr_id 
              and lower(t.campaign) = 'mc16'
              and lower(t.subcampaign) = 'mc16a'
        )
        select 
          r.pr_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.step,
          r.taskid,
          r.status,
          r.timestamp,
          h.hashtag_list
        from 
          requests r
          LEFT JOIN
          hashtags h
          ON h.taskid = r.taskid
        group by 
          r.pr_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.step,
          r.taskid,
          r.status,
          r.timestamp,
          h.hashtag_list
'''

__hashtag_request_task_data_container = '''
    with 
        hashtags as (
          select 
            taskid, 
            LISTAGG(hashtag.hashtag, ', ') 
              within group (order by ht_t.taskid) as hashtag_list
          from atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag where hashtag.ht_id=ht_t.ht_id
          GROUP BY taskid
        ), 
        requests as (
            select 
              r.pr_id as request_id, 
              r.description, 
              r.reference_link, 
              r.phys_group, 
              r.energy_gev, 
              r.project,
              substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step,
              t.taskid,
              t.taskname,
              t.status,
              t.timestamp,
              container.name as container_name,
              substr(container.name, instrc (container.name,'.',1,4)+1,instrc(container.name,'.',1,5)-instrc(container.name,'.',1,4)-1) as container_type
            from 
              t_prodmanager_request r, 
              t_production_task t,
              t_production_container container
            where 
              r.pr_id = t.pr_id 
              and t.taskid = container.parent_tid
              and lower(t.campaign) = 'mc16'
              and lower(t.subcampaign) = 'mc16a'
        )
        select 
          r.request_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.step,
          r.taskid,
          r.status,
          r.timestamp,
          r.container_name, 
          r.container_type,
          h.hashtag_list
        from 
          requests r
          LEFT JOIN
          hashtags h
          ON h.taskid = r.taskid
        group by 
          r.request_id,
          r.description, 
          r.reference_link, 
          r.phys_group, 
          r.energy_gev, 
          r.project,
          r.step,
          r.taskid,
          r.status,
          r.timestamp,
          r.container_name, 
          r.container_type,
          h.hashtag_list
'''

__hashtag_request_task_tag_container = '''
    with
        hashtags as (
          select
            taskid,
            LISTAGG(hashtag.hashtag, ', ')
              within group (order by ht_t.taskid) as hashtag_list
          from atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag where hashtag.ht_id=ht_t.ht_id
          GROUP BY taskid
        ),
        production_tags as (
          select
            task.taskid,
            tag.name as tag_name,
            tag.trf_release,
            tag.tag_parameters as tag_params
          from
            atlas_deft.t_production_tag tag,
            t_production_task task
          where
            tag.name = trim(regexp_substr(trim(regexp_substr(task.taskname, '[^.]+',1,5)), '[^_]*$'))
        ),
        requests as (
            select
              r.pr_id as request_id,
              r.description,
              r.reference_link,
              r.phys_group,
              r.energy_gev,
              r.project,
              substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step,
              t.taskid,
              t.taskname,
              t.status,
              t.timestamp,
              container.name as container_name,
              substr(container.name, instrc (container.name,'.',1,4)+1,instrc(container.name,'.',1,5)-instrc(container.name,'.',1,4)-1) as container_type
            from
              t_prodmanager_request r,
              t_production_task t,
              t_production_container container
            where
              r.pr_id = t.pr_id
              and t.taskid = container.parent_tid
              and lower(t.campaign) = 'mc16'
              and lower(t.subcampaign) = 'mc16a'
            group by
              r.pr_id,
              r.description,
              r.reference_link,
              r.phys_group,
              r.energy_gev,
              r.project,
              substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1),
              t.taskid,
              t.taskname,
              t.status,
              t.timestamp,
              container.name,
              substr(container.name, instrc (container.name,'.',1,4)+1,instrc(container.name,'.',1,5)-instrc(container.name,'.',1,4)-1)
        ),
        requests_to_tags as (
          select
            r.request_id,
            r.description,
            r.reference_link,
            r.phys_group,
            r.energy_gev,
            r.project,
            r.step,
            r.taskid,
            r.taskname,
            r.status,
            r.timestamp,
            t.tag_name,
            t.trf_release,
            t.tag_params,
            r.container_name,
            r.container_type
          from
            requests r,
            production_tags t
          where r.taskid = t.taskid
        )
        select
          r.request_id,
          r.description,
          r.reference_link,
          r.phys_group,
          r.energy_gev,
          r.project,
          r.step,
          r.taskid,
          r.status,
          r.timestamp,
          r.tag_name,
          r.trf_release,
          r.tag_params,
          r.container_name,
          r.container_type,
          h.hashtag_list
        from
          requests_to_tags r
          LEFT JOIN
          hashtags h
          ON h.taskid = r.taskid
'''


__sql_task_hashtags = '''
        with
            hashtags as (
              select
                r.pr_id,
                ht_t.taskid,
                LISTAGG(hashtag.hashtag, ', ')
                  within group (order by ht_t.taskid) as hashtag_list
              from
                atlas_deft.t_ht_to_task ht_t,
                atlas_deft.t_hashtag hashtag,
                t_production_task t,
                t_prodmanager_request r
              where
                hashtag.ht_id=ht_t.ht_id
                and ht_t.taskid = t.taskid
                and t.pr_id = r.pr_id
              GROUP BY
                r.pr_id,
                ht_t.taskid
        )
        select
          pr_id,
          taskid,
          hashtag_list
        from hashtags
        where
          lower(hashtag_list) LIKE '%mc16a%'
          OR lower(hashtag_list) LIKE '%mc16a_cp'
          OR lower(hashtag_list) LIKE '%mc16a_trig'
          OR lower(hashtag_list) LIKE '%mc16a_hpc'
          OR lower(hashtag_list) LIKE '%mc16a_pc'
'''
# group by
#   r.request_id,
#   r.description,
#   r.reference_link,
#   r.phys_group,
#   r.energy_gev,
#   r.project,
#   r.step,
#   r.taskid,
#   r.status,
#   r.timestamp,
#   r.tag_name,
#   r.trf_release,
#   r.container_name,
#   r.container_type,
#   h.hashtag_list

__sql_task_tags = '''
          select
            task.taskid,
            task.taskname,
            trim(regexp_substr(trim(regexp_substr(task.taskname, '[^.]+',1,5)), '[^_]*$')) as tag_name,
            tag.trf_release,
            tag.tag_parameters
          from
            atlas_deft.t_production_tag tag,
            t_production_task task
          where
            tag.name = trim(regexp_substr(trim(regexp_substr(task.taskname, '[^.]+',1,5)), '[^_]*$'))
            and task.taskid = 10728909
          order by task.taskid
'''

__sql_tag = "select * from atlas_deft.t_production_tag tag where tag.name = 's3126'"
__sql_task = '''
select
taskname,
trim(regexp_substr(trim(regexp_substr(taskname, '[^.]+',1,5)), '[^_]*$')) as tag_name
from t_production_task
where taskid = 10728909
'''

__sql_input_events_counter = '''
    select
      count(i_ds.input_events)
    from
      t_production_task task,
      t_input_dataset i_ds
    where
      task.inputdataset = i_ds.name
'''

__sql_input_data = '''
    with summary as (
        select
          r.pr_id,
          t.taskid,
          sum(d.input_events) as req_events
        from
          t_prodmanager_request r,
          t_input_dataset d,
          t_production_task t
        where
          r.pr_id = d.pr_id
          and r.pr_id = t.pr_id
          and lower(r.campaign) = 'mc16'
          and lower(r.sub_campaign) = 'mc16a'
        group by
          r.pr_id,
          t.taskid
        order by
          r.pr_id,
          t.taskid
          ),
          hashtags as (
            select
              t.taskid,
              LISTAGG(h.hashtag, ', ')
              within group (order by ht_t.taskid) as hashtag_list
            from
              t_production_task t,
              atlas_deft.t_ht_to_task ht_t,
              atlas_deft.t_hashtag h
            where
              t.taskid = ht_t.taskid
              and h.ht_id = ht_t.ht_id
              and lower(t.campaign) = 'mc16'
              and lower(t.subcampaign) = 'mc16a'
            group by
              t.taskid
          )
            select
              h.hashtag_list,
              s.req_events
            from
              summary s
              LEFT JOIN
              hashtags h ON
              s.taskid = h.taskid

'''

handler = open('SQLRequests/mc16a_event_summary.sql')

sql_test_request = "SELECT taskid from t_production_task where subcampaign = 'MC16a' and phys_group = 'MCGN' and project = 'mc16_13TeV'"

datasets_query = '''
SELECT
  count(jd.datasetname)
FROM
  t_production_task t
  JOIN
  ATLAS_PANDA.jedi_datasets jd
  ON jd.jeditaskid = t.taskid
  LEFT JOIN t_task tt
  ON t.taskid = tt.taskid
WHERE
  jd.type IN ('output') AND
      t.timestamp > to_date('01-01-2016', 'dd-mm-yyyy hh24:mi:ss') AND
      t.timestamp <= to_date('30-11-2017', 'dd-mm-yyyy hh24:mi:ss') AND
      t.pr_id > 300
'''

ML_task_hashtags = '''
select t.taskname,
LISTAGG(hashtag.hashtag, ', ')
        WITHIN GROUP (
          ORDER BY t.taskid) AS hashtag_list
from t_production_task t
  LEFT JOIN ATLAS_DEFT.t_ht_to_task ht_t
  ON t.taskid = ht_t.taskid
  LEFT JOIN ATLAS_DEFT.t_hashtag hashtag
  ON hashtag.ht_id = ht_t.ht_id
group by t.taskname
'''

xx = '''
select * from t_production_dataset where name LIKE '%.EVNT_%.%'
'''

result = DButils.ResultIter(conn, xx, 100, True)
for item in result:
    print item

# result = DButils.ResultIter(conn, handler.read()[:-1], 100, True)
#DButils.QueryToCSV(conn, handler.read()[:-1], 'datasets_tags.csv', 100)
# DButils.QueryToCSV(conn, ML_task_hashtags, 'ML_task_hashtags.csv', 10000)
#
# start = time.time()
# result = DButils.ResultIter(conn, handler.read()[:-1], 100, True)
# for r in result:
#     print json.dumps(r)
# end = time.time()
# diff = end - start
# print(diff)
# counter = 0
# for i in range(0, 10):
#     start = time.time()
#     result = DButils.ResultIter(conn, handler.read()[:-1], 100, True)
#     for r in result:
#         print json.dumps(r)
#     end = time.time()
#     diff = end - start
#     print(diff)
#     counter += diff
# print(counter / 10)

    # for k,v in r['tag_parameters'].iteritems():
    #     print k, v

# DButils.QueryToCSV(conn, handler.read()[:-1], 'mc16_camp.csv')
#DButils.CSV2JSON("mc16_camp.csv", "mc16_camp.json")
# result = DButils.QueryAll(conn, handler.read())
#DButils.QueryToCSV(conn, handler.read()[:-1], 'mc16_events.csv')
# result = DButils.ResultIter(conn, __hashtag_request_task_tag_container, 1)
# end = time.time()

#
# for i in result:
#     print i
#
# print len(result)
#
# print [fix_lob(r) for r in result]
#
# DButils.CSV2JSON("hashtag_request_task_data_container.csv", "hashtag_request_task_data_container.json")
# end = time.time()
# print "Query Execution time:"
# print(end - start)