import DButils
import csv
import time

dsn = "___ORACLE_DEFT_DSN___"
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

#and lower(r.sub_campaign) = 'mc16c'" \
__sql_container = "select parent_tid, name from t_production_container where rownum <=10"
__sql_tag = "select * from atlas_deft.t_production_tag where rownum<=10"

# __sql_get_tasks_for_request = "select * from t_production_task where pr_id = 12304"

# cursor.execute(__sql_tag)
# print cursor.description
#
start = time.time()
result = DButils.QueryAll(conn, __sql_container)
end = time.time()
print "Query Execution time:"
print(end - start)

for item in result:
    print item

# start = time.time()
# DButils.QueryToCSV(conn, __sql, "MC16_request_task_container_tag_hash_list.csv")
# end = time.time()
# print "Query Execution time:"
# print(end - start)