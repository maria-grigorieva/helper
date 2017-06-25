import DButils
import csv
import time
import ConfigParser
from DocumentProcessing import FileConnector

Config = ConfigParser.ConfigParser()
Config.read("settings.cfg")
dsn = Config.get("oracle", "dsn")
print dsn
conn, cursor = DButils.connectDEFT_DSN(dsn)

__sql_get_campaign_subcampaign = "select * from t_prodmanager_request where lower(campaign) = 'mc16' and lower(sub_campaign) = 'mc16a'"
__sql_get_finished_requests = "select pr_id, description, reference_link, phys_group, energy_gev, project " \
                              "from t_prodmanager_request where lower(campaign) = 'mc16' and " \
                              "lower(sub_campaign) = 'mc16a' and status = 'finished' and request_type='MC'"
__sql_get_production_step_request_id = "select * from t_production_step where pr_id = 11869"
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

__sql_container = "select parent_tid, name from t_production_container where rownum <=10"
__sql_tag = "select * from atlas_deft.t_production_tag where rownum<=10"
__sql_TASKS_PROD_AGGREGATE = "select * from ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE where rownum<=100";
__sql_TASKS_PROD_AGGREGATE_COUNTER = "select count(*) from ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE";


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

# start = time.time()
# result = DButils.QueryAll(conn, __hashtag_request_task_data_container)
# end = time.time()
#
# for item in result:
#     print item
# print "Query Execution time:"
# print(end - start)
start = time.time()
DButils.QueryToCSV(conn, __hashtag_request_task_data_container, "hashtag_request_task_data_container.csv")
DButils.CSV2JSON("hashtag_request_task_data_container.csv", "hashtag_request_task_data_container.json")
end = time.time()
print "Query Execution time:"
print(end - start)