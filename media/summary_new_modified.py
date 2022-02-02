from tkinter import *
from tkinter import messagebox
from tkinter import ttk
import tkinter as tk
from tkinter import Button, Tk, HORIZONTAL
from tkinter.ttk import Progressbar
import time
from functools import partial
import threading
import requests
import cx_Oracle
import pandas as pd
import shutil
import webbrowser
import os, zipfile, math, datetime, re
import sys

# ----------------------------- estimation factors Start------------------------------------------
# estimation factors start
# object count factors
obj_tables_est_factor = 15
obj_check_const_esti_factor = 2
obj_synon_est_factor = 1
obj_views_mviews_est_factor = 10
obj_types_est_factor = 15
obj_packages_est_factor = 850
obj_procedure_est_factor = 360
obj_function_est_factor = 360
obj_trigger_est_factor = 360
# detail count factors
detail_proc_est_factor = 30
detail_func_est_factor = 35
detail_package_est_factor = 9
detail_tables_est_factor = 0
detail_column_est_factor = 1
detail_parti_esti_factor = 2
detail_check_const_esti_factor = 2
detail_synon_est_factor = 1
detail_synon_dblink_est_factor = 15
detail_views_mviews_simple_est_factor = 1
detail_views_mviews_medium_est_factor = 5
detail_views_mviewscomplex_est_factor = 10
detail_simple_est_factor = 1
detail_medium_est_factor = 5
detail_complex_est_factor = 10
# common count factors
pk_esti_factor = 2
fk_esti_factor = 2
index_esti_factor = 2
sequence_esti_factor = 3
dblink_estimate_factor = 60
dblink_est_factor = 60
pragma_est_factor = 60
conn_by_est_factor = 0.1
bulkcollect_est_factor = 4
varchar_est_fac = 0.1
number_est_fac = 0.1
date_est_fac = 0.1
percentiletype_est_fac = 1
percentiler_rowtype_est_fac = 1
const_est_factor = 0.1
records_est_fac = 0.1
job_est_factor = 90
schedule_est_factor = 90
program_est_factor = 90
row_num_est_factor = 0.1
pivot_est_factor = 0.1
unpivot_est_factor = 0.1
job_schedule_program_esti_factor = [job_est_factor, schedule_est_factor, program_est_factor]
primary_forign_esti_factor = [pk_esti_factor, fk_esti_factor]


# ----------------------------- estimation factors End------------------------------------------


## oracle connection for the object validation tool
def get_count(connection, query):
    # dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    # connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    # print("connected to the Oracle Database")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        data = None
    finally:
        cursor.close()
    return data


def commonlogicextract(kimberly, x):
    '''
    :param kimberly: This is the main string to(dhqvjqjnbdjqbk(0dbvhdq)dqbdhq)vskvdkjb nmdhvgkhgdkjw to()
    :param x: to
    :return:  [to(dhqvjqjnbdjqbk(0dbvhdq)dqbdhq),to()]                            --Sk 8/26 Assigned by Vamsi on --8/26
    '''
    startIndex = [m.start() for m in re.finditer(rf'{x}\(', kimberly)]
    listdata = []
    for index in startIndex:
        current = []
        bracket_level = 0
        for s in kimberly[index + len(x) + 1:]:
            if s != '(' and s != ')' and bracket_level >= 0:
                current.append(s)
            elif s == '(':
                current.append(s)
                bracket_level += 1
            elif s == ')':
                bracket_level -= 1
                if bracket_level < 0:
                    current.append(s)
                    break
                else:
                    current.append(s)
        listdata.append(x + '(' + ''.join(current))
    return listdata


def rm_singlequ(single_line_text):
    dir_all = re.findall(r'\/\*[\s\S]*?\*/', single_line_text)
    for j in dir_all:
        single_line_text = single_line_text.replace(j, '\n', 1)
    single_line_text = re.sub(r'extract\s+\(', 'extract(', single_line_text)
    single_line_text = re.sub(r'extractvalue\s+\(', 'extractvalue(', single_line_text)
    single_line_text = re.sub(r'\-\-+', '--', single_line_text)
    extractpartsdictformat = {}
    listformat = commonlogicextract(single_line_text, 'extract')
    listformat1 = commonlogicextract(single_line_text, 'extractvalue')
    arya = 0
    for arc in listformat:
        single_line_text = single_line_text.replace(arc, 'arya' + str(arya), 1)
        extractpartsdictformat['arya' + str(arya)] = arc
        arya = arya + 1
    for arc1 in listformat1:
        single_line_text = single_line_text.replace(arc1, 'aryan' + str(arya), 1)
        extractpartsdictformat['aryan' + str(arya)] = arc1
        arya = arya + 1
    singlequoye = re.findall(r"\'.*?\'", single_line_text)
    for i in singlequoye:
        cond = bool(re.search(r'\-\-+', i))
        if cond:
            if '--' in i:
                # main_split = i.split('--')
                # part1 = main_split[0]
                # part2 = main_split[1]
                # '-- Table ' | | v_table_name | | ' not found';
                # combine_str = part1 + 'singquote' + part2
                combine_str = i.replace('--', 'singquote')
                single_line_text = single_line_text.replace(i, combine_str)
    dirall = re.findall(r"\-\-.*?\n", single_line_text)
    for dashcomments in dirall:
        single_line_text = single_line_text.replace(dashcomments, "\n", 1)
    single_line_text = re.sub("singquote", "--", single_line_text)
    for barc in extractpartsdictformat:
        single_line_text = single_line_text.replace(barc, extractpartsdictformat[barc])
    return single_line_text


def regexarrowdecrypt(data):
    data = data
    data = data.replace('dasheswiththearrow', '------>')
    return data


def regexarrowencrypt(data):
    data = data
    regexp = re.findall(r"\-+\>", data)
    for i in regexp:
        data = data.replace(i, 'dasheswiththearrow', 1)
    return data


def function_pre_lower(data):
    # function to Convert the whole string to lower case except the data which is in single quotes --SK 7/12
    # Example:: """ the Data had the ' The data Main forum ' values Main to Copy """ ==> """ the data had the ' The data Main forum ' values main to copy """

    data = re.sub(r"\b(?<!')(\w+)(?!')\b", lambda match: match.group().lower(), data)

    return data


# def data_remove_comments(read_text):
#     dir_all = re.findall(r'\/\*([\s\S]*?)\*/', read_text)
#     for j in dir_all:
#         read_text = read_text.replace('/*' + j + '*/', '')
#
#     read_text_split = read_text.split('\n')
#     dash_comments_list = []
#     if len(read_text_split):
#         for whole_lines in read_text_split:
#             if not whole_lines.strip().lstrip().startswith('dbms.') or whole_lines.strip().lstrip().startswith('dbms_'):
#                 dash_comments = re.findall(r'--.*', whole_lines)
#                 dash_comments_list.append(dash_comments)
#
#         remov_dash_empty_list = [ele for ele in dash_comments_list if ele != []]
#         for i in remov_dash_empty_list:
#             read_text = read_text.replace(i[0], '')
#
#     return read_text


def checking_count(listofdetails, simple_count, complex_count):
    """
    Conditon1:: ['pragma', 'bulk collect' and dblink ('@'),listagg,row_number,pivot,unpivot,connect by,with,type] keywords we are making as complex
    Conditon2:: Multiple select statement are there within a single select statement treating as complex
    Condition3:: If Count of semicolon is greater than 500 making as complex
    Condition4:: If query includes more than 3 or 4 tables in a select statement treated as complex i)
                i) Take the data from select and from and ;, take data from 'from' and ';'; if 'join' key words more than or equal to 2 treat as complex
                ii) Take the data from select and from and where; from 'from' and 'where' if number of commas are greater than or equal to 2 treat as complex
    :param listofdetails:  Defines the specific list of all package or procedure or function or triggers or views or sequenced etc...
    :param simple_count:  Defines the count of simple packages or simple proecedure etc.. in the list
    :param complex_count: Defines the count of complex packages or complex procedures etc.. in the list
    :return: the simple objects count and complex object count                                                 --SK 8/5
    """
    counting_details = ['bulk collect', 'pragma', '@', 'listagg', 'row_number', 'pivot', 'unpivot', 'connect by',
                        'with', 'type']
    for iterate_list in listofdetails:
        simple_flag = False
        complex_flag = False
        # comments_remove = data_remove_comments(iterate_list)
        single_line_text = function_pre_lower(iterate_list)
        single_line_text = single_line_text + "\n"
        single_line_text = regexarrowencrypt(single_line_text)
        single_line_text = rm_singlequ(single_line_text)
        single_line_text = regexarrowdecrypt(single_line_text)
        # comments_remove = iterate_list
        # Condition1
        for items in counting_details:
            if items in single_line_text:
                complex_flag = True
                break
        # Conditon2
        select_statement = re.findall(r'select(.*?);', single_line_text)
        for i in select_statement:
            if 'select' in i:
                complex_flag = True
                break
        # Condition3
        count_semicolon = single_line_text.count(';')
        if count_semicolon > 500:
            complex_flag = True
            break
        # Condition4
        # i)
        select_from_semiclon = re.findall(r'select(.*?)\sfrom\s(.*?);', single_line_text)
        for j in select_from_semiclon:
            after_from = j[1]
            if after_from.count('join') >= 2:
                complex_flag = True
                break
        # ii)
        select_from_where = re.findall(r'select(.*?)\sfrom\s(.*?)where', single_line_text)
        for k in select_from_where:
            from_where = k[1]
            if from_where.count(',') >= 2:
                complex_flag = True
                break

        if complex_flag == True:
            complex_count = complex_count + 1
        else:
            simple_count = simple_count + 1
    return simple_count, complex_count


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


html = resource_path("files")
html_file = html + '/' + 'qmigrator.html'
with open(html_file, 'r', encoding='utf-8') as f:
    html_string = f.read()


# def html_report(qmigrator_hours, manual_mig_hours, total_co_count, total_so_count, percentage_saving, schema,):
def html_report(total_co_so, qmigrator_hours, manual_mig_hours, total_co_count, total_so_count, percentage_saving,
                cschema_type, simple_so_percentage, complex_so_percentage, simple_co_percentage, complex_co_percentage,
                html_string):
    # html_string = '''
    # <html>
    #   <head><title>HTML</title>
    #   <center><h3>Qmigrator<h3/></center>
    #   </head>
    #   <style>
    #   table, th, td {{font-size:10pt; border:1px solid black; border-collapse:collapse; text-align:left;}}
    #   th, td {{padding: 5px;}}
    # </style>
    #   <link rel="stylesheet" type="text/css" href="df_style.css"/>
    #   <body style="background-color:white;">
    #     <center> Total number of storage objects : {so}</center>
    #     <center> Total number of Code objects : {co}</center>
    #     <center> Manual Migration effort for a team familiar with Oracle to PostgreSQL migration : {mig} Hours</center>
    #     <center> QMIgrator estimate : {qmig} Hours</center>
    #     <center> Percentage of Savings:  {saving} %</center>
    #     <center><p>     Additional Qmigrator Benefits ::
    #                     1. Automated detailed Assessment Engine
    #                     2. Seamless Data Migration
    #                     3. Data Quality check report
    #                     4. Load testing report (Optional)
    #                     5. Cloud Migration </p></center>
    #   </body>
    #
    # </html>.
    # '''
    qfilename = 'Qmigrator_' + str(cschema_type) + '.html'
    # simple_so_percentage, complex_so_percentage, simple_co_percentage, complex_co_percentage)
    with open(qfilename, 'w') as f:
        # f.write(html_string.format(tot=total_co_so, so=total_so_count, co=total_co_count,
        #                            mig=manual_mig_hours, qmig=qmigrator_hours, saving=percentage_saving,
        #                            persosim=simple_so_percentage, persocom=complex_so_percentage,
        #                            percosim=simple_co_percentage, percocom=complex_co_percentage))
        html_string = html_string.replace('{tot}', str(total_co_so)).replace('{so}', str(total_so_count)).replace(
            '{co}', str(total_co_count)) \
            .replace('{mig}', str(manual_mig_hours)).replace('{qmig}', str(qmigrator_hours)).replace('{saving}',
                                                                                                     str(percentage_saving)) \
            .replace('{persosim}', str(simple_so_percentage)).replace('{persocom}', str(complex_so_percentage)).replace(
            '{percosim}', str(simple_co_percentage)).replace('{percocom}', str(complex_co_percentage))
        f.write(html_string)
    webbrowser.open_new_tab(qfilename)


# storage objects:

def datatypes(cschema_type, statements_list, single_connection, ceecount):
    data_type_query = """
                       select case when DATA_PRECISION is null and DATA_SCALE is null then
                               'alter table '||lower(OWNER)||'.'||lower(TABLE_NAME)||' alter column '||lower(COLUMN_NAME)||' type numeric('||DATA_LENGTH||');'
                               ELSE
                               'alter table '||lower(OWNER)||'.'||lower(TABLE_NAME)||' alter column '||lower(COLUMN_NAME)||' type numeric('||decode(DATA_PRECISION,null,DATA_LENGTH,DATA_PRECISION)||','||decode(DATA_SCALE,null,DATA_LENGTH,DATA_SCALE)||');'
                               end
                               FROM (
                               select lower(OWNER) as OWNER,lower(TABLE_NAME) as TABLE_NAME,lower(COLUMN_NAME) as COLUMN_NAME,DATA_PRECISION,DATA_SCALE,DATA_LENGTH
                               from dba_tab_columns where OWNER='@schemaname' AND DATA_TYPE='NUMBER' and TABLE_NAME not like '%$%'
                               AND NOT EXISTS (select 1 from dba_views DV WHERE owner='@schemaname' AND DV.VIEW_NAME=dba_tab_columns.TABLE_NAME)
                               AND NOT EXISTS (SELECT 1 FROM DBA_MVIEWS DM WHERE DM.MVIEW_NAME=dba_tab_columns.TABLE_NAME and lower(owner)=lower('@schemaname'))
                               and NOT exists (select 1 from DBA_SYNONYMS DS where owner='@schemaname' and dba_tab_columns.TABLE_NAME=DS.TABLE_NAME)
                               order by TABLE_NAME
                               )
                                    """

    print("data type def")
    data_type_oracle = str(data_type_query).replace('@schemaname', cschema_type)

    # Modified -- Sk 12/27
    datatype_orac = oracle_conn_data_type(data_type_oracle, single_connection)

    datatype_list = [i[0] for i in datatype_orac]
    print(len(datatype_list))
    if datatype_list:
        datatype_orac_str = '\n'.join(datatype_list)
        datatype_orac_str = re.sub(r' +', ' ', datatype_orac_str)
        sql_path_data = 'Source_' + cschema_type + '/' + str(
            cschema_type).upper() + '/' + 'DATATYPES' + '/' + 'datatypes.sql'
        create_and_append_sqlfile(sql_path_data, datatype_orac_str)


def tables(cschema_type, statements_list, single_connection, ceecount):
    table_list_query = """ 
                select DISTINCT OBJECT_NAME from dba_objects a where NOT exists (select 1 from dba_objects b where
                a.object_name=b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW') AND A.OWNER=upper('@schemaname') and OBJECT_NAME not like '%$%' AND A.OBJECT_TYPE='TABLE' and TEMPORARY='N' ORDER BY 1
                """
    table_def_query = """
                   with x as
                   (
                   SELECT owner, object_name, object_type
                   FROM dba_objects
                   WHERE owner = upper('@schemaname')
                   AND object_type IN ('TABLE')
                   AND object_name = '@name'
                   AND object_name not in (select mview_name From dba_mviews where owner=upper('@schemaname')) and TEMPORARY='N'
                   )
                   SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x"""
    count_table_query = """
            select COUNT(DISTINCT dtc.TABLE_NAME) as table_count,COUNT(dtc.COLUMN_NAME) as columns_count from
                dba_tab_columns dtc
                where LOWER(dtc.owner)=LOWER('@schemaname') AND dtc.TABLE_NAME NOT LIKE '%$$'
                AND NOT EXISTS (SELECT 1 FROM DBA_MVIEWS mv WHERE LOWER(mv.OWNER)=LOWER('@schemaname') and
                mv.MVIEW_NAME=dtc.TABLE_NAME)
                and not exists (select 1 from dba_views dv where OWNER='@schemaname' and dv.VIEW_NAME=dtc.TABLE_NAME)
            """
    print("tables def")
    query_oracle = str(table_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))

    # Commented the whole for loop since we are not using for utility html files --SK 8/5
    # for item in lists:
    #     query_oracle2 = str(table_def_query).replace('@schemaname', cschema_type).replace('@name', item)
    #     try:
    #         def_orac_df = exe_oracle_connection(query_oracle2, username, password, hostname, port, sid, cschema_type)
    #     except Exception as e:
    #         print('error', e)
    #         continue
    #     if len(def_orac_df.values):
    #         query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         # read_text = query_text.read()
    #         # query_text = query_text.read()
    #         if not query_text.strip().endswith(';'):
    #             query_text = query_text + ";"
    #         sql_path_data = 'Source_' + cschema_type + '/' + str(
    #             cschema_type).upper() + '/' + 'TABLES' + '/' + 'tables.sql'
    #         create_and_append_sqlfile(sql_path_data, query_text)
    #     else:
    #         continue
    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        # sub_objects_count.extend(['Tables', len(lists), len(lists) * table_esti_factor,round(float((len(lists) * table_esti_factor) / 60), 2)])
        sub_objects_count.extend(['Tables', len(lists), round(float((len(lists) * detail_tables_est_factor) / 60), 2)])
        statements_list.append(sub_objects_count)
    elif str(ceecount).upper() == 'O':
        # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}
        # oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
        #                                                        parent_tag='OraclecountExtractorvalidation')
        # count_tablequery_oracle = orc_count_query_tag['count_table_query'].replace('@schemaname', cschema_type)
        query_oracle = str(count_table_query).replace('@schemaname', cschema_type)
        count_table_query = query_oracle.replace('@schemaname', cschema_type)
        data = get_count(single_connection, count_table_query)
        if data:
            table_co = data[0][0]
            column_count = data[0][1]
            esimate = round((obj_tables_est_factor * len(lists)) / 60, 2)
            # sub_objects_count.extend(['Tables', len(lists), estimate_factor * len(lists), esimate])
            sub_objects_count.extend(['Tables', len(lists), esimate])
            statements_list.append(sub_objects_count)


def column(cschema_type, statements_list, single_connection, ceecount):
    schema_column_query = """SELECT count(1) FROM dba_TAB_columns WHERE owner = '@schemaname'"""
    count_querys1 = schema_column_query
    obj_list1 = 'column'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())
    data = get_count(single_connection, count_query)
    if data:
        if str(ceecount).upper() == 'D':
            part1 = data[0][0]
            # sub_objects_count.extend([obj_list1, part1, part1 * column_esti_factor, round(float((part1 * column_esti_factor) / 60), 2)])
            sub_objects_count.extend([obj_list1, part1, round(float((part1 * detail_column_est_factor) / 60), 2)])
            statements_list.append(sub_objects_count)


def partitions(cschema_type, statements_list, single_connection, ceecount):
    schema_partition_query = """SELECT count(1) FROM dba_TAB_PARTITIONS WHERE table_owner = '@schemaname'"""
    count_querys1 = schema_partition_query
    obj_list1 = 'Partitions'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())
    data = get_count(single_connection, count_query)
    if data:
        part1 = data[0][0]
        if str(ceecount).upper() == 'D':
            # sub_objects_count.extend([obj_list1, part1, part1 * partition_esti_factor,round(float((part1 * partition_esti_factor) / 60), 2)])
            sub_objects_count.extend([obj_list1, part1, round(float((part1 * detail_parti_esti_factor) / 60), 2)])
            statements_list.append(sub_objects_count)


def check_constarints(cschema_type, statements_list, single_connection, ceecount):
    check_constrai_def_query = """
            SELECT adding_primary_keys AS constraints_generation FROM (
            (select 'ALTER TABLE '||lower(OWNER)||'.'||lower(table_name)||' ADD CONSTRAINT '||constraint_name||' PRIMARY KEY(' ||PKey||'); ' as adding_primary_keys,1 as sno  from (
            select OWNER,table_name,listagg(PKEY,',') within group  (order by PKEY) as PKey,constraint_name  from
            (Select dba_cons_columns.table_name,
            dba_cons_columns.column_name as PKey,
            dba_constraints.constraint_name as constraint_name,
            dba_constraints.owner AS OWNER
            from dba_constraints, dba_cons_columns
            where
            dba_constraints.constraint_type = 'P'
            and dba_constraints.constraint_name = dba_cons_columns.constraint_name
            and dba_constraints.owner = dba_cons_columns.owner
            and dba_cons_columns.owner = upper('@schemaname')
                                             AND dba_cons_columns.table_name NOT LIKE '%$%'
            AND NOT EXISTS (select 1 from dba_objects b where dba_cons_columns.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
           -- group by dba_constraints.owner,dba_cons_columns.table_name,dba_constraints.constraint_name  ,dba_cons_columns.column_name
            order by
            dba_cons_columns.table_name,
            dba_cons_columns.column_name
            )
            group by OWNER,table_name,CONSTRAINT_NAME
            )
            )
            UNION

            (select 'ALTER TABLE'||' '|| child_tab_owner || '.' || TABLE_NAME ||' '|| 'ADD constraint '||r_constraint_name||' '|| Foreign_Key ||'; ' as Foreign_Key_query ,2 as sno
            from (SELECT
            c.table_name,c.child_tab_owner,
            'FOREIGN KEY ('|| cc.fk_column || ') REFERENCES ' || p.parent_tab_owner || '.' || p.table_name || '('||
            pc.ref_column ||')' AS Foreign_Key,
            c.r_constraint_name
            FROM ( SELECT owner child_tab_owner, table_name, constraint_name, r_constraint_name FROM dba_CONSTRAINTS
            WHERE owner = upper('@schemaname') AND constraint_type = 'R' and table_name not like '%$%') c,
            ( SELECT owner parent_tab_owner, table_name, constraint_name FROM dba_CONSTRAINTS WHERE owner =
            upper('@schemaname') AND constraint_type IN('P', 'U') and table_name not like '%$%') p,
            ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
            fk_column
            FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) cc,
            ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
            ref_column
            FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) pc
            WHERE c.r_constraint_name = p.constraint_name
            AND c.table_name = cc.table_name AND c.constraint_name = cc.constraint_name AND c.child_tab_owner = cc.owner
            AND p.table_name = pc.table_name AND p.constraint_name = pc.constraint_name AND p.parent_tab_owner =
            pc.owner
            order by c.table_name)
            )

            union

            (select 'ALTER TABLE '||lower(OWNER)||'.'||lower(table_name)||' ADD CONSTRAINT '||constraint_name||' UNIQUE(' ||PKey||');' as adding_unique_keys,0 as sno  from (
            select OWNER,table_name,listagg(PKEY,',') within group  (order by PKEY) as PKey,constraint_name  from
            (
            Select dba_cons_columns.table_name,
            dba_cons_columns.column_name as PKey,
            dba_constraints.constraint_name as constraint_name,
            dba_constraints.owner AS OWNER
            from dba_constraints, dba_cons_columns
            where
            dba_constraints.constraint_type = 'U'
            and dba_constraints.constraint_name = dba_cons_columns.constraint_name
            and dba_constraints.owner = dba_cons_columns.owner
            and dba_cons_columns.owner = upper('@schemaname')
                                             AND dba_cons_columns.table_name NOT LIKE '%$%'
            AND NOT EXISTS (select 1 from dba_objects b where dba_cons_columns.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            order by
            dba_cons_columns.table_name,
            dba_cons_columns.column_name
            )
            group by OWNER,table_name,CONSTRAINT_NAME
            ))
            union  --- check


            (SELECT res.*,4 as sno FROM (
            (select 'alter table '||lower(SCHEMA_NAME)||'.'||lower(table_name)||' add constraint '||object_name||' check ('||CHECK_CONDITION||');' AS ADDING_CHECK_CONSTRAINTS
            from ( WITH ref AS (
            SELECT extractValue(xs.object_value, '/ROW/OWNER') AS schema_name
            , extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
            , extractValue(xs.object_value, '/ROW/CONSTRAINT_NAME') AS object_name
            , extractValue(xs.object_value, '/ROW/SEARCH_CONDITION') AS condition_column
            , extractValue(xs.object_value, '/ROW/COLUMN_NAME') AS column_name
            FROM (
            SELECT XMLTYPE(
            DBMS_XMLGEN.GETXML('SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
            cols.column_name
            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
            cols.constraint_name
            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND OBJECT_TYPE=''MATERIALIZED VIEW'')
            AND cons.owner = upper(''@schemaname'') and cons.constraint_type = ''C'' and cons.STATUS=''ENABLED'' '
            )
            ) AS xml FROM DUAL
            ) x
            , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
            )
            SELECT table_name AS table_name,
            trim(upper(replace(check_condition, '"', ''))) AS check_condition,object_name,schema_name
           FROM (
            SELECT
            schema_name,
            table_name,
            object_name,
            'CHECK' AS constraint_type,
            condition_column AS check_condition
            FROM ref
            UNION
            SELECT
            owner AS schema_name,
            table_name,
            'SYS_C0000'||column_id AS object_name,
            'CHECK' AS constraint_type,
            '"'||column_name||'" IS NOT NULL' AS check_condition
            FROM DBA_tab_columns tcols where owner = upper('@schemaname') and nullable = 'N' and tcols.table_name not like '%$%'
            AND NOT EXISTS (select 1 from dba_objects b where tcols.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            AND NOT EXISTS ( SELECT 1 FROM ref WHERE ref.table_name = tcols.table_name
            AND ref.schema_name = tcols.owner
            AND ref.column_name = tcols.column_name
            AND ref.condition_column = '"'||tcols.column_name||'" IS NOT NULL')
            /* ALL_TAB_COLUMNS contains Tables and Views. Add below to exclude Views NOT NULL constraints */
            AND NOT EXISTS ( SELECT 1 FROM DBA_VIEWS vw WHERE vw.view_name = tcols.table_name
            AND vw.owner = tcols.owner
            )
            )
            ) where   CHECK_CONDITION  not like  '%IS NOT NULL' and table_name not like '%$%'
                                             )
             UNION

            (select 'alter table '||lower(SCHEMA_NAME)||'.'||lower(table_name)||' alter column  '||REPLACE(CHECK_CONDITION,' IS ',' SET ')||'; ' AS ADDING_NOTNULLS
            from (
            WITH ref AS (
            SELECT extractValue(xs.object_value, '/ROW/OWNER') AS schema_name
            , extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
            , extractValue(xs.object_value, '/ROW/CONSTRAINT_NAME') AS object_name
            , extractValue(xs.object_value, '/ROW/SEARCH_CONDITION') AS condition_column
            , extractValue(xs.object_value, '/ROW/COLUMN_NAME') AS column_name
            FROM (
            SELECT XMLTYPE(
            DBMS_XMLGEN.GETXML('SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
            cols.column_name
            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
            cols.constraint_name
            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND OBJECT_TYPE=''MATERIALIZED VIEW'')
            AND cons.owner = upper(''@schemaname'') and cons.constraint_type = ''C'' and cons.STATUS=''ENABLED'' '
            )
            ) AS xml FROM DUAL
            ) x
            , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
            )
            SELECT table_name AS table_name,
            trim(upper(replace(check_condition, '"', ''))) AS check_condition,object_name,schema_name
            FROM (
            SELECT
            schema_name,
            table_name,
            object_name,
            'CHECK' AS constraint_type,
            condition_column AS check_condition
            FROM ref
            UNION
            SELECT
            owner AS schema_name,
            table_name,
            'SYS_C0000'||column_id AS object_name,
            'CHECK' AS constraint_type,
           '"'||column_name||'" IS NOT NULL' AS check_condition
            FROM DBA_tab_columns tcols where owner = upper('@schemaname') and nullable = 'N' and tcols.table_name not like '%$%'
            AND NOT EXISTS (select 1 from dba_objects b where tcols.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            AND NOT EXISTS ( SELECT 1 FROM ref WHERE ref.table_name = tcols.table_name
            AND ref.schema_name = tcols.owner
            AND ref.column_name = tcols.column_name
            AND ref.condition_column = '"'||tcols.column_name||'" IS NOT NULL')
            /* ALL_TAB_COLUMNS contains Tables and Views. Add below to exclude Views NOT NULL constraints */
            AND NOT EXISTS ( SELECT 1 FROM DBA_VIEWS vw WHERE vw.view_name = tcols.table_name
            AND vw.owner = tcols.owner
            )
            )
            ) where   CHECK_CONDITION   like  '%IS NOT NULL' and table_name not like '%$%'
            )
            ) res)

            UNION  ---index
            select  'CREATE INDEX '||INDEX_NAME||' ON '||TABLE_NAME||'('||INDEX_COLS||');', 5 as sno  FROM

           (
            WITH cols AS (
            SELECT idx.owner AS schema_name, idx.table_name, idx.index_name, cols.column_name, cols.column_position,
            idx.uniqueness, decode(cols.descend, 'ASC', '', ' '||cols.descend) descend
            FROM DBA_INDEXES idx, DBA_IND_COLUMNS cols
            WHERE idx.owner = cols.index_owner AND idx.table_name = cols.table_name AND idx.index_name = cols.index_name
            AND idx.owner = upper('@schemaname') and idx.table_name not like '%$%'
            ),
            expr AS (
            SELECT extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
            , extractValue(xs.object_value, '/ROW/INDEX_NAME') AS index_name
            , extractValue(xs.object_value, '/ROW/COLUMN_EXPRESSION') AS column_expression
            , extractValue(xs.object_value, '/ROW/COLUMN_POSITION') AS column_position
            FROM (
            SELECT XMLTYPE(
            DBMS_XMLGEN.GETXML( 'SELECT table_name, index_name, column_expression, column_position FROM
            DBA_IND_EXPRESSIONS WHERE index_owner = upper(''@schemaname'')  and table_name not like ''%$%'' '
            ||' union all SELECT null, null, null, null FROM dual '
            )
            ) AS xml FROM DUAL
            ) x
            , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
            )
            SELECT
            cols.table_name,
            listagg(CASE WHEN cols.column_name LIKE 'SYS_N%' THEN expr.column_expression || cols.descend ELSE
            cols.column_name || cols.descend END, ', ') within group(order by cols.column_position) as Index_Cols,
            cols.index_name
            FROM cols
            LEFT OUTER JOIN expr ON cols.table_name = expr.table_name
            AND cols.index_name = expr.index_name
            AND cols.column_position = expr.column_position
            GROUP BY cols.schema_name, cols.table_name, cols.index_name, cols.uniqueness
            ORDER BY cols.table_name, cols.index_name, cols.uniqueness
            ) res  where not exists (select 1 from dba_mviews mv where mv.MVIEW_NAME=res.table_name and lower(owner)=lower('@schemaname'))
            )  order by sno asc
                    """

    Meta_list_checkcponst_count = """select count(1) as count from (
                            SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
                            cols.column_name
                            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
                            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
                            cols.constraint_name
                            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
                            AND cons.owner = upper('@schemaname') AND cons.constraint_type = 'C'   AND cons.table_name NOT LIKE '%$%')"""

    print("check constra def")
    checkdef_count_checkconstr = Meta_list_checkcponst_count.replace('@schemaname', cschema_type)
    count_check_constr = oracle_connection(checkdef_count_checkconstr, single_connection)
    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        check_count = count_check_constr['COUNT'][0]
        if check_count > 0:
            query_modified = """
            select count(*) from (
            select * from ( WITH ref AS (
            SELECT extractValue(xs.object_value, '/ROW/OWNER') AS schema_name
            , extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
            , extractValue(xs.object_value, '/ROW/CONSTRAINT_NAME') AS object_name
            , extractValue(xs.object_value, '/ROW/SEARCH_CONDITION') AS condition_column
            , extractValue(xs.object_value, '/ROW/COLUMN_NAME') AS column_name
            FROM (
            SELECT XMLTYPE(
            DBMS_XMLGEN.GETXML('SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
            cols.column_name
            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
            cols.constraint_name
            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND
            OBJECT_TYPE=''MATERIALIZED VIEW'')
            AND cons.owner = upper(''@schemaname'') and cons.constraint_type = ''C'' '
            )
            ) AS xml FROM DUAL
            ) x
            , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
            )
            SELECT table_name AS table_name,
            trim(upper(replace(check_condition, '"', ''))) AS check_condition
            FROM (
            SELECT
            schema_name,
            table_name,
            object_name,
            'CHECK' AS constraint_type,
            condition_column AS check_condition
            FROM ref
            UNION
            SELECT
            owner AS schema_name,
            table_name,
            'SYS_C0000'||column_id AS object_name,
            'CHECK' AS constraint_type,
            '"'||column_name||'" IS NOT NULL' AS check_condition
            FROM DBA_tab_columns tcols where owner = upper('@schemaname') and nullable = 'N'
            AND NOT EXISTS (select 1 from dba_objects b where tcols.table_name =b.object_name AND
            OBJECT_TYPE='MATERIALIZED VIEW')
            AND NOT EXISTS ( SELECT 1 FROM ref WHERE ref.table_name = tcols.table_name
            AND ref.schema_name = tcols.owner
            AND ref.column_name = tcols.column_name
            AND ref.condition_column = '"'||tcols.column_name||'" IS NOT NULL')
            /* ALL_TAB_COLUMNS contains Tables and Views. Add below to exclude Views NOT NULL constraints */
            AND NOT EXISTS ( SELECT 1 FROM DBA_VIEWS vw WHERE vw.view_name = tcols.table_name
            AND vw.owner = tcols.owner
            )
            )
            ) where table_name not like '%$%')
            """
            query_modified = query_modified.replace('@schemaname', str(cschema_type).upper())
            count_check_constr_mod = oracle_connection(query_modified, single_connection)

            check_count = count_check_constr_mod['COUNT(*)'][0]

        # sub_objects_count.extend(['Constarints', check_count, check_count * check_constarint_esti_factor,round(float((check_count * check_constarint_esti_factor) / 60), 2)])
        sub_objects_count.extend(
            ['Constarints', check_count, round(float((check_count * detail_check_const_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)
    elif str(ceecount).upper() == 'O':
        check_count = count_check_constr['COUNT'][0]
        # sub_objects_count.extend(['Constarints', check_count, check_count * check_constarint_esti_factor,round(float((check_count * check_constarint_esti_factor) / 60), 2)])
        sub_objects_count.extend(
            ['Constarints', check_count, round(float((check_count * obj_check_const_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)

    # check_count = count_check_constr['COUNT'][0]

    # Commented by --SK 8/5 SInce we are not using for utility genertaed html report

    # if int(check_count) != 0:
    #     list_query_oracle_check = check_constrai_def_query.replace('@schemaname', cschema_type).replace('@order', '')
    #     try:
    #         def_orac_df = exe_oracle_connection(list_query_oracle_check, username, password, hostname, port,
    #                                             sid,
    #                                             cschema_type)
    #     except Exception as e:
    #         def_orac_df = pd.DataFrame()
    #         print('error', e)
    #     print(len(def_orac_df))
    #     if len(def_orac_df):
    #         # print(def_orac_df.columns)
    #         # query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
    #         query_text = def_orac_df[def_orac_df.columns[0]].to_list()
    #         query_text = '\n'.join(query_text)
    #         query_text = re.sub(r' +', ' ', query_text)
    #         if not query_text.strip().endswith(';'):
    #             query_text = query_text + ";"
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         check_schemaname = """set search_path to @schemaname;""".replace('@schemaname', cschema_type.lower())
    #         query_text = check_schemaname + '\n' + query_text
    #         sql_path_data = 'Source_' + cschema_type + '/' + str(
    #             cschema_type).upper() + '/' + 'CHECKCONSTRAINS' + '/' + 'checkconstrains.sql'
    #         create_and_append_sqlfile(sql_path_data, query_text)


def prim_forei(cschema_type, statements_list, single_connection, ceecount):
    prima_count_query = """
                        select count(1)from (
                        Select dba_cons_columns.table_name, dba_cons_columns.column_name as PKey
                        from dba_constraints, dba_cons_columns
                        where
                        dba_constraints.constraint_type = 'P'
                        and dba_constraints.constraint_name = dba_cons_columns.constraint_name
                        and dba_constraints.owner = dba_cons_columns.owner
                        and dba_cons_columns.owner = upper('@schemaname') and dba_cons_columns.table_name not like '%$%'
                        AND NOT EXISTS (select 1 from dba_objects b where dba_cons_columns.table_name =b.object_name AND
                        OBJECT_TYPE='MATERIALIZED VIEW')
                        order by
                        dba_cons_columns.table_name,
                        dba_cons_columns.column_name
                        )

                        """
    foreign_count_query = """
                        select count(1) foreign_key_count from (
                            SELECT  /*+ RULE */
                        c.table_name,
                        'FOREIGN KEY ('|| cc.fk_column || ') REFERENCES ' || p.parent_tab_owner || '.' || p.table_name || '('||
                        pc.ref_column ||')' AS Foreign_Key
                        FROM ( SELECT owner child_tab_owner, table_name, constraint_name, r_constraint_name FROM dba_CONSTRAINTS
                        WHERE owner = upper('@schemaname') AND constraint_type = 'R' and table_name not like '%$%') c,
                        ( SELECT owner parent_tab_owner, table_name, constraint_name FROM dba_CONSTRAINTS WHERE owner =
                        upper('@schemaname') AND constraint_type IN('P', 'U')  and table_name not like '%$%') p,
                        ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
                        fk_column
                        FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) cc,
                        ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
                        ref_column
                        FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) pc
                        WHERE c.r_constraint_name = p.constraint_name
                        AND c.table_name = cc.table_name AND c.constraint_name = cc.constraint_name AND c.child_tab_owner = cc.owner
                        AND p.table_name = pc.table_name AND p.constraint_name = pc.constraint_name AND p.parent_tab_owner =
                        pc.owner
                        order by c.table_name)
                        """

    other_obj = ['Primary Keys', 'foreign keys']
    count_queries_for_pri_check = [prima_count_query, foreign_count_query]
    for i, j, k in zip(count_queries_for_pri_check, other_obj, primary_forign_esti_factor):
        sub_objects_count = []
        count_query = i.replace('@schemaname', cschema_type)
        data = get_count(single_connection, count_query)
        if data:
            part1 = data[0][0]
            estimate_factor = k
            esimate = round((estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
            sub_objects_count.extend([j, part1, esimate])
            statements_list.append(sub_objects_count)


def indexes(cschema_type, statements_list, single_connection, ceecount):
    index_list_query = "SELECT object_name FROM dba_objects WHERE owner = upper('@schemaname') AND object_type IN ('INDEX') and OBJECT_NAME not like '%$%'"
    index_def_query = """
                                      with x as
                                      (
                                      SELECT owner, object_name, object_type
                                      FROM dba_objects
                                      WHERE owner = upper('@schemaname')
                                      AND object_type IN ('INDEX')
                                      AND object_name = '@name'
                                      )
                                      SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x"""

    print("index def")
    sub_objects_count = []
    query_oracle = str(index_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))

    # Commented by --SK 8/5 since we are not using for html exe generation
    # for item in lists:
    #     query_oracle2 = str(index_def_query).replace('@schemaname', cschema_type).replace('@name', item)
    #     try:
    #         def_orac_df = exe_oracle_connection(query_oracle2, username, password, hostname, port, sid, cschema_type)
    #     except Exception as e:
    #         print('ERROR', e)
    #         continue
    #     if len(def_orac_df.values):
    #         query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         # read_text = query_text.read()
    #         # query_text = query_text.read()
    #         if not query_text.strip().endswith(';'):
    #             query_text = query_text + ";"
    #         sql_path_data = 'Source' + '/' + str(
    #             cschema_type).upper() + '/' + 'INDEXES' + '/' + 'indexes.sql'
    #         create_and_append_sqlfile(sql_path_data, query_text)
    #     else:
    #         continue

    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        # sub_objects_count.extend(['Indexes', len(lists), len(lists * index_esti_factor),round(float(len(lists * index_esti_factor) / 60), 2)])
        sub_objects_count.extend(['Indexes', len(lists), round(float(len(lists * index_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)
    elif str(ceecount).upper() == 'O':
        ind_est_factor_h = round((index_esti_factor * len(lists)) / 60, 2)
        # sub_objects_count.extend(["Indexes", len(lists), (index_factor * len(lists)), ind_est_factor_h])
        sub_objects_count.extend(["Indexes", len(lists), ind_est_factor_h])
        statements_list.append(sub_objects_count)

    # index_factor = 2
    # ind_est_factor_h = round((index_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Indexes", len(lists), len(lists), index_factor, ind_est_factor_h])
    # statements_list.append(sub_objects_count)


def sequences(cschema_type, statements_list, single_connection, ceecount):
    sequence_list_query = """SELECT object_name FROM dba_objects WHERE owner = upper('@schemaname')
                                               AND object_type IN ('SEQUENCE') and lower(object_name) not like lower('ISEQ%')"""

    sequence_def_query = """
                             select lower(sequence_name) from (
                                   select 'create sequence '||SEQUENCE_NAME||' minvalue '||MIN_VALUE||' maxvalue '||decode(MAX_VALUE,9999999999999999999999999999,99999999999999999)||
                                   ' increment by '||INCREMENT_BY||' start with '||LAST_NUMBER||' CACHE '||CACHE_SIZE||' NO CYCLE;' as sequence_name
                                   from dba_sequences where SEQUENCE_OWNER=upper('@schemaname') and upper(SEQUENCE_NAME) = '@name' and lower(SEQUENCE_NAME) not like lower('ISEQ%'))
                       """

    print("sequence def")
    sub_objects_count = []
    query_oracle = str(sequence_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))

    # Commented -- SK 8/5 since we are not using in the html exe generation
    # for item in lists:
    #     query_oracle2 = str(sequence_def_query).replace('@schemaname', cschema_type).replace('@name', item)
    #     try:
    #         def_orac_df = exe_oracle_connection(query_oracle2, username, password, hostname, port, sid, cschema_type)
    #     except Exception as e:
    #         print('ERROR', e)
    #         continue
    #     if len(def_orac_df.values):
    #         query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         # read_text = query_text.read()
    #         # query_text = query_text.read()
    #         if not query_text.strip().endswith(';'):
    #             query_text = query_text + ";"
    #         sql_path_data = 'Source_' + cschema_type + '/' + str(
    #             cschema_type).upper() + '/' + 'SEQUENCES' + '/' + 'sequences.sql'
    #         create_and_append_sqlfile(sql_path_data, query_text)
    #     else:
    #         continue

    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        # sub_objects_count.extend(['Sequences', len(lists), len(lists * sequence_esti_factor),round(float(len(lists * sequence_esti_factor) / 60), 2)])
        sub_objects_count.extend(['Sequences', len(lists), round(float(len(lists * sequence_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)
    elif str(ceecount).upper() == 'O':
        seq_est_factor_h = round((sequence_esti_factor * len(lists)) / 60, 2)
        # sub_objects_count.extend(["Sequences", len(lists), (seq_est_factor * len(lists)), seq_est_factor_h])
        sub_objects_count.extend(["Sequences", len(lists), seq_est_factor_h])
        statements_list.append(sub_objects_count)
    # seq_est_factor = 3
    # seq_est_factor_h = round((seq_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Sequences", len(lists), len(lists), seq_est_factor, seq_est_factor_h])
    # statements_list.append(sub_objects_count)


def synonyms(cschema_type, statements_list, single_connection, ceecount):
    synonym_list_query = """
                        SELECT object_name
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('SYNONYM')
                        """
    synonym_def_query = """
                         with x as
                (
                SELECT owner, object_name, object_type
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('SYNONYM')
                AND object_name like '@name'
                )
                SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                        """

    print("Syn def")
    sub_objects_count = []
    synonym_list = []
    query_oracle = str(synonym_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))

    # -- SK 8/5
    # for item in lists:
    #     query_oracle2 = str(synonym_def_query).replace('@schemaname', cschema_type).replace('@name', item)
    #     try:
    #         def_orac_df = exe_oracle_connection(query_oracle2, username, password, hostname, port, sid, cschema_type)
    #     except Exception as e:
    #         print('error', e)
    #         continue
    #     if len(def_orac_df.values):
    #         query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
    #         # read_text = query_text.read()
    #         query_text = query_text.read()
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         if not query_text.strip().endswith(';'):
    #             query_text = query_text + ";"
    #         query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
    #         synonym_list.append(query_text)
    #         sql_path_data = 'Source_' + cschema_type + '/' + str(
    #             cschema_type).upper() + '/' + 'SYNONYMS' + '/' + 'synonyms.sql'
    #         create_and_append_sqlfile(sql_path_data, query_text)
    #     else:
    #         continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'SYNONYMS' + '/' + 'synonyms.sql') as f:
            seq_str = synonym_list
            # read_text_single = [line.lower().strip() for line in seq_str if line.strip()]
            synonym_count = 0
            synonym_at_count = 0
            synonym_estimation_count = 0
            for i in seq_str:
                synonym_estimation_count = synonym_estimation_count + 1
                i_atfinding = re.findall(r'@(.*?),|@(.*?);', i)
                if len(i_atfinding) != 0:
                    synonym_at_count = synonym_at_count + 1
                else:
                    synonym_count = synonym_count + 1
            total_estimation_synonyms = (synonym_at_count * detail_synon_dblink_est_factor) + (
                    synonym_count * detail_synon_est_factor)
            # sub_object_count.extend(['Synonyms', len(lists), total_estimation_synonyms, round(float((total_estimation_synonyms) / 60), 2)])
            sub_object_count.extend(['Synonyms', len(lists), round(float((total_estimation_synonyms) / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as f:
            print('File Not found for Synonyms')
    elif str(ceecount).upper() == 'O':
        syn_est_factor_h = round((obj_synon_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(["Synonyms", len(lists), (syn_est_factor * len(lists)), syn_est_factor_h])
        sub_object_count.extend(["Synonyms", len(lists), syn_est_factor_h])
        statements_list.append(sub_object_count)

    # syn_est_factor = 1
    # syn_est_factor_h = round((syn_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Synonyms", len(lists), len(lists), syn_est_factor, syn_est_factor_h])
    # statements_list.append(sub_objects_count)


def views(cschema_type, statements_list, single_connection, ceecount, simple_so_count,
          complex_so_count):
    print("VIEWS SIMPLE AND COMPLEX COUNT STARTED ", simple_so_count, complex_so_count)
    view_list_query = "SELECT object_name FROM dba_objects WHERE owner = upper('@schemaname') AND object_type IN ('VIEW') and object_name not like '%$%'"
    view_def_query = """
                                       with x as
                                       (
                                       SELECT owner, object_name, object_type
                                       FROM dba_objects
                                       WHERE owner = upper('@schemaname')
                                       AND object_type IN ('VIEW')
                                       AND object_name = '@name'
                                       )
                                       SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x"""

    count_view_query = """
        select COUNT(DISTINCT dtc.TABLE_NAME) as view_count,COUNT(dtc.COLUMN_NAME) as column_count from dba_tab_columns dtc
                 where dtc.owner='@schemaname' AND dtc.TABLE_NAME NOT LIKE '%$$'
                --AND NOT EXISTS (SELECT 1 FROM DBA_MVIEWS mv  WHERE mv.OWNER='@schemaname' and mv.MVIEW_NAME=dtc.TABLE_NAME)
                 and  exists (select 1 from dba_views dv where LOWER(OWNER)=LOWER('@schemaname') and dv.VIEW_NAME=dtc.TABLE_NAME)
        """

    print("views def")
    viwes_list = []
    query_oracle = str(view_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))
    for item in lists:
        query_oracle2 = str(view_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('error', e)
            continue
        if len(def_orac_df.values):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # read_text = query_text.read()
            # query_text = query_text.read()
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            viwes_list.append(query_text)

            # --SK 8/5
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'VIEWS' + '/' + 'views.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue

    if str(ceecount).upper() == 'D':
        try:
            # Checking SImple Storage and Complex Storage count
            simple_so_count, complex_so_count = checking_count(viwes_list, simple_so_count, complex_so_count)

            read_text_single = viwes_list
            read_text_single_str = "\n".join(read_text_single)
            read_text_single_str = re.sub(r' +', ' ', read_text_single_str)
            create_or_replace_split = read_text_single_str.lower().split('create or replace')
            del create_or_replace_split[0]
            simple_count = 0
            medium_count = 0
            complex_count = 0
            highly_complex_count = 0
            views_count = 0
            for i in create_or_replace_split:
                views_count = views_count + 1
                i = 'create or replace ' + i
                from_btw_where = re.findall(r'from(.*?)where|from(.*?);|from(.*?)order by', i, re.DOTALL)
                if from_btw_where:
                    from_btw_where = from_btw_where[0]
                    from_btw_where = str([i.strip() for i in from_btw_where if i != ''])
                    from_btw_where_comma_split = from_btw_where.split(',')
                    i_select_split = i.split('select')
                    number_select_sts = len(i_select_split) - 1
                    if len(from_btw_where_comma_split) == 1 and number_select_sts == 1:
                        simple_count = simple_count + 1
                    elif len(from_btw_where_comma_split) > 1 and len(
                            from_btw_where_comma_split) < 5 and number_select_sts == 1:
                        medium_count = medium_count + 1
                    elif len(from_btw_where_comma_split) > 5 and len(
                            from_btw_where_comma_split) < 10 and number_select_sts == 1:
                        complex_count = complex_count + 1
                    else:
                        # i_select_split = i.split('select')
                        # number_select_sts = len(i_select_split) - 1
                        result = (3 * number_select_sts) + 5
                        highly_complex_count = highly_complex_count + result
            total_estimation_view = (simple_count * detail_views_mviews_simple_est_factor) + (
                    medium_count * detail_views_mviews_medium_est_factor) + (
                                            complex_count * detail_views_mviewscomplex_est_factor) + highly_complex_count
            sub_object_count = []
            # sub_object_count.extend(['Views', len(lists), total_estimation_view, round(float((total_estimation_view) / 60), 2)])
            sub_object_count.extend(['Views', len(lists), round(float((total_estimation_view) / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as f:
            print('File Not Found for Views')
            sub_object_count = []
            sub_object_count.extend(['Views', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}
        # oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
        #                                                        parent_tag='OraclecountExtractorvalidation')
        # count_viewquery_oracle = orc_count_query_tag['count_view_query'].replace('@schemaname', cschema_type)
        sub_objects_count = []
        query_oracle = str(count_view_query).replace('@schemaname', cschema_type)
        count_view_query = query_oracle.replace('@schemaname', cschema_type)
        data = get_count(single_connection, count_view_query)
        if data:
            part1 = data[0][0]
            part2 = data[0][1]
            esimate = round((obj_views_mviews_est_factor * part1) / 60, 2)
            # sub_objects_count.extend(['Views', part1, (estimate_factor * part1), esimate])
            sub_objects_count.extend(['Views', part1, esimate])
            statements_list.append(sub_objects_count)
    print("VIEWS SIMPLE AND COMPLEX COUNT ENDED ", simple_so_count, complex_so_count)
    return simple_so_count, complex_so_count


def mview(cschema_type, statements_list, single_connection, ceecount, simple_so_count,
          complex_so_count):
    print("MVIEWS SIMPLE AND COMPLEX COUNT ENDED ", simple_so_count, complex_so_count)
    materialized_list_query = "SELECT object_name FROM dba_objects WHERE owner = upper('@schemaname') AND object_type IN ('MATERIALIZED VIEW') and object_name not like '%$%'"

    materialized_def_query = """
                            select LOWER('CREATE MATERIALIZED VIEW  '||owner||'.'||MVIEW_NAME||' AS '),QUERY ,';' from dba_mviews where lower(owner)=lower('@schemaname') and lower(MVIEW_NAME)=lower('@name')
                       """

    count_mview_query = """
        select COUNT(DISTINCT dtc.TABLE_NAME) as mv_count,COUNT(dtc.COLUMN_NAME) as column_count  from dba_tab_columns dtc
                 where LOWER(dtc.owner)=LOWER('@schemaname') AND dtc.TABLE_NAME NOT LIKE '%$$'
                 AND  EXISTS (SELECT 1 FROM DBA_MVIEWS mv  WHERE LOWER(mv.OWNER)=LOWER('@schemaname') and mv.MVIEW_NAME=dtc.TABLE_NAME)
        """

    print("mview def")
    mviews_list = []
    query_oracle = str(materialized_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))

    for item in lists:
        query_oracle2 = str(materialized_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('ERROR', e)
            continue
        if len(def_orac_df.values):
            query_text1 = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text2 = def_orac_df.loc[0][list(def_orac_df.columns)[1]]
            query_text1 = str(query_text1).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # query_text = query_text.read()
            if not query_text1.strip().endswith(';'):
                query_text2 = query_text2 + ";"
            # read_text = query_text.read()
            query_text = query_text1 + query_text2
            query_text = re.sub(r' +', ' ', query_text)
            query_text = str(query_text).lower()
            mviews_list.append(query_text)

            # --SK 8/5
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS' + '/' + 'materialized.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue
    if str(ceecount).upper() == 'D':
        try:

            # IMPLEMENT SIMPLE AND COMPLEX STORAGE COUNT FOR MVIEWS --SK 8/5
            simple_so_count, complex_so_count = checking_count(mviews_list, simple_so_count, complex_so_count)

            # with open('Source' + '/' + str(
            #         cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS' + '/' + 'materialized.sql') as f:
            mviews_str = '\n'.join(mviews_list)
            mviews_str = re.sub(r' +', ' ', mviews_str)
            # read_text_single = [line.lower().strip() for line in f if line.strip()]
            # read_text_single_str = "".join(read_text_single)
            create_or_replace_split = mviews_str.split('create')
            create_or_replace_split = [i.strip() for i in create_or_replace_split if i != '']
            simple_count = 0
            medium_count = 0
            complex_count = 0
            highly_complex_count = 0
            mviews_count = 0
            for i in create_or_replace_split:
                mviews_count = mviews_count + 1
                i = 'create ' + i
                from_split = re.findall(r'from(.*?)where|from(.*?);|from(.*?)order by|\bfrom\b', i, re.DOTALL)
                if from_split:
                    from_split = from_split[0]
                    from_btw_where = str([i.strip() for i in from_split if i != ''])
                    from_btw_where_comma_split = from_btw_where.split(' ')
                    i_select_split = i.split('select')
                    number_select_sts = len(i_select_split) - 1
                    if len(from_btw_where_comma_split) == 1 and number_select_sts == 1:
                        simple_count = simple_count + 1
                    elif len(from_btw_where_comma_split) > 1 and len(
                            from_btw_where_comma_split) < 5 and number_select_sts == 1:
                        medium_count = medium_count + 1
                    elif len(from_btw_where_comma_split) > 5 and len(
                            from_btw_where_comma_split) < 10 and number_select_sts == 1:
                        complex_count = complex_count + 1
                    else:
                        # i_select_split = i.split('select')
                        # number_select_sts = len(i_select_split) - 1
                        result = (3 * number_select_sts) + 5
                        highly_complex_count = highly_complex_count + result
            total_estimation_mview = (simple_count * detail_views_mviews_simple_est_factor) + (
                    medium_count * detail_views_mviews_medium_est_factor) + (
                                             complex_count * detail_views_mviewscomplex_est_factor) + highly_complex_count
            sub_object_count = []
            # sub_object_count.extend(['Mviews', len(lists), total_estimation_mview, round(float((total_estimation_mview) / 60), 2)])
            sub_object_count.extend(['Mviews', len(lists), round(float((total_estimation_mview) / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as f:
            print('File Not Found for MVIEWS')
            sub_object_count = []
            sub_object_count.extend(['Mviews', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}
        # oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
        #                                                        parent_tag='OraclecountExtractorvalidation')
        # count_mviewquery_oracle = orc_count_query_tag['count_mview_query'].replace('@schemaname', cschema_type)
        sub_objects_count = []
        query_oracle = str(count_mview_query).replace('@schemaname', cschema_type)
        count_mview_query = query_oracle.replace('@schemaname', cschema_type)
        data = get_count(username, password, port, hostname, sid, count_mview_query)
        if data:
            part1 = data[0][0]
            part2 = data[0][1]
            esimate = round((obj_views_mviews_est_factor * part1) / 60, 2)
            # sub_objects_count.extend(['MViews', part1, (estimate_factor * part1), esimate])
            sub_objects_count.extend(['MViews', part1, esimate])
            statements_list.append(sub_objects_count)
    print("MVIEWS SIMPLE AND COMPLEX COUNT ENDED ", simple_so_count, complex_so_count)
    return simple_so_count, complex_so_count


def types(cschema_type, statements_list, single_connection, ceecount):
    type_list_query = """
                        SELECT object_name
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('TYPE')
                        """
    type_def_query = """
                        with x as
                (
                SELECT owner, object_name, object_type
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('TYPE')
                AND object_name like '@name'
                )
                SELECT DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                            """

    print("Types def")
    type_list = []
    query_oracle = str(type_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))
    type_count = 0
    for item in lists:
        query_oracle2 = str(type_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2,single_connection)
        except Exception as e:
            print('error', e)
            continue
        if len(def_orac_df.values):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = query_text.read()
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"

            query_text_split = query_text.split(';')
            query_text_split = [i for i in query_text_split if i != '']
            type_count = type_count + len(query_text_split)
            type_list.append(query_text)
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'TYPES' + '/' + 'types.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'TYPES' + '/' + 'types.sql') as f:
            type_str = '\n'.join(type_list)
            type_str = re.sub(r' +', ' ', type_str)
            data = type_str
            # data = f.read().strip()
            data = data.lower().replace('  ', '')
            # query_text_rm_comm = data_remove_comments(query_text)
            single_line_text = function_pre_lower(data)
            single_line_text = single_line_text + "\n"
            single_line_text = regexarrowencrypt(single_line_text)
            single_line_text = rm_singlequ(single_line_text)
            single_line_text = regexarrowdecrypt(single_line_text)
            data_split = single_line_text.split('create or replace')
            del data_split[0]
            types_estimation_count = 0
            type_comma_count = 0
            type_semicolon_count = 0
            for i in data_split:
                if 'type "' in i:
                    types_estimation_count = types_estimation_count + 1
                    i.count(',')
                    type_comma_count = type_comma_count + i.count(',')
                if 'type body' in i:
                    i.count(';')
                    type_semicolon_count = type_semicolon_count + i.count(';')
            total_estimation_types = type_comma_count + type_semicolon_count
            # sub_object_count.extend(['Types', len(lists), total_estimation_types, round(float((total_estimation_types) / 60), 2)])
            sub_object_count.extend(['Types', len(lists), round(float((total_estimation_types) / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as f:
            print('File NOT Found for types')
            sub_object_count.extend(['Types', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        types_est_factor_h = round((obj_types_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(["Types", len(lists), (types_est_factor * len(lists)), types_est_factor_h])
        sub_object_count.extend(["Types", len(lists), types_est_factor_h])
        statements_list.append(sub_object_count)

    # types_est_factor = 15
    # types_est_factor_h = round((types_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["User Defined Types", len(lists), type_count, types_est_factor, types_est_factor_h])
    # statements_list.append(sub_objects_count)


def dblink_storage(cschema_type, statements_list, single_connection, ceecount):
    schema_dblink_query = """select count(1) from dba_objects where object_type = 'DATABASE LINK' AND upper(owner) = '@schemaname'"""
    count_querys1 = schema_dblink_query
    obj_list1 = 'Dblink'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())
    data = get_count(single_connection, count_query)
    if data:
        part1 = data[0][0]
        if str(ceecount).upper() == 'D':
            esimate = round((dblink_estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend([obj_list1, part1, part1 * dblink_estimate_factor, esimate])
            sub_objects_count.extend([obj_list1, part1, esimate])
            statements_list.append(sub_objects_count)
        elif str(ceecount).upper() == 'O':
            esimate = round((dblink_estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend([obj_list1, part1, part1 * dblink_estimate_factor, esimate])
            sub_objects_count.extend([obj_list1, part1, esimate])
            statements_list.append(sub_objects_count)


# taking data between commas
def split_main(s):
    parts = []
    bracket_level = 0
    current = []
    for c in (s + ","):
        if c == "," and bracket_level == 0:
            parts.append("".join(current))
            current = []
        else:
            if c == "(":
                bracket_level += 1
            elif c == ")":
                bracket_level -= 1
            current.append(c)
    return parts


def select_stmt_logic(read_text_single):
    arg_all_select_statement = read_text_single.split(';')
    arg_all_sel_st = len(arg_all_select_statement)
    return arg_all_sel_st


def arg_calculation_old(read_text_single_str, codeobj_list, read_text_single, whole_detail_count, arg_type,
                        total_inclusive_dict):
    read_store_package = read_text_single_str.split('."', 1)
    arg_package_name = None
    arg_package_type = arg_type
    content = "".join([s for s in read_text_single.strip().splitlines(True) if s.strip()])
    arg_lines = len(content.split('\n'))
    arg_all_select_statement = select_stmt_logic(read_text_single)
    if len(read_store_package) >= 2:
        arg_package_name = read_store_package[1].split('"', 1)[0].strip()
    arg_identifiers = None
    id_list = []
    arg_total_identifiers_list = []
    read_text_single_str = re.sub(r"'.*?'", '', read_text_single_str)
    read_text_single_str = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace(
        '[', ' [ ').replace(']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%',
                                                                                          ' % ').replace(
        '=', ' = ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+',
                                                                                          ' + ').replace(
        '/', ' / ').replace('<', ' < ').replace('*', ' * ').replace('"', ' " ').replace('>>', ' >> ').replace(
        '--', '').replace('---', '').replace('----', '').replace('-----', '').replace('>', ' > ').replace('  ',
                                                                                                          ' ').replace(
        "'", " ' ").replace("-", " - ")
    for id, id1 in total_inclusive_dict.items():
        # print(id1)
        id = id.lower()
        if id == 'dblink':
            id = '@'
            if id in read_text_single_str:
                id_count = read_text_single_str.count(id)
                id = id + ':' + str(id1)
                id_str = id + '(' + str(id_count) + ')'
                id_list.append(id_str)
                arg_total_identifiers_list.append(id_count)
        my_regex = r"\b(?=\w)" + re.escape(id) + r"\b(?!\w)"
        if re.search(my_regex, read_text_single_str, re.IGNORECASE):
            id_count = len(re.findall(my_regex, read_text_single_str, re.IGNORECASE))
            id = id + ':' + str(id1)
            id_str = id + '(' + str(id_count) + ')'
            id_list.append(id_str)
            arg_total_identifiers_list.append(id_count)
    if id_list:
        arg_identifiers = ','.join(id_list)
    else:
        arg_identifiers = None
    sum_identifiers = sum(arg_total_identifiers_list)
    Five_factors = 5 * sum_identifiers
    Ten_factors = 10 * sum_identifiers
    codeobj_list.extend(
        [arg_package_name, arg_package_type, arg_lines, arg_all_select_statement, arg_identifiers,
         sum_identifiers, Five_factors, Ten_factors])
    return codeobj_list


def common_logic_package(read_text_single, codeobj_list, total_inclusive_dict):
    read_text_single_str = read_text_single.lower().replace('  ', ' ').replace(')is', ') is').replace(')as',
                                                                                                      ') as').replace(
        '  create or replace', 'create or replace').replace(' package', 'package')
    read_data = read_text_single_str.split('create or replace')
    del read_data[0]
    package_estimation_count = 0  # total packages
    package_comma_count = 0  # comma_count
    package_semicolon_count = 0  # semicolon count
    body_comma_data = 0
    body_semicolon_data = 0
    varchar_count = 0
    number_count = 0
    date_count = 0
    percenttype_count = 0
    percentrowtype_count = 0
    body_varchar_count = 0
    body_number_count = 0
    body_date_count = 0
    body_percenttype_count = 0
    body_percentrowtype_count = 0
    pack_dblink_count = 0  # dblink count
    body_pack_dblink_count = 0  # dblink count
    pack_constants = 0
    body_pack_constants = 0
    pack_records = 0
    body_pack_records = 0
    pack_types = 0
    body_pack_types = 0
    simple_count = 0
    medium_count = 0
    complex_count = 0
    highly_complex_count = 0
    body_simple_count = 0
    body_medium_count = 0
    body_complex_count = 0
    body_highly_complex_count = 0
    bulk_collect = 0
    body_bulk_collect = 0
    total_estimation_cursor = 0
    total_body_estimation_cursor = 0
    pack_pragma = 0
    body_pragma = 0
    body_connect_by = 0
    dml_count_exce = 0
    body_dml_count_exce = 0
    pack_raise = 0
    body_raise = 0
    body_row_number = 0
    body_with = 0
    body_type = 0
    body_list_agg = 0
    body_pivot = 0
    body_unpivot = 0
    # adding semicolons and commas and storing into total_semi_comma_count
    for j in read_data:
        if ' package ' in j and 'package body' not in j:
            package_estimation_count = package_estimation_count + 1
            before_proc_func = re.findall(r'package(.*?)procedure|package(.*?)function', j, re.DOTALL)
            if before_proc_func:
                before_proc_func_list = [item for t in before_proc_func for item in t]
                before_proc_func_list = [i.strip() for i in before_proc_func_list if i != '']
                for i in before_proc_func_list:
                    if ';' in i:
                        package_semicolon_count = package_semicolon_count + i.count(';')
                    if ',' in i:
                        i_comma = split_main(i)
                        length_comma = len(i_comma) - 1
                        package_comma_count = package_comma_count + length_comma
                    if 'varchar2' in i:
                        varchar_count = varchar_count + i.count('varchar2')
                    if 'number' in i:
                        number_count = number_count + i.count('number')
                    if 'date' in i:
                        date_count = date_count + i.count('date')
                    if '%type' in i:
                        percenttype_count = percenttype_count + i.count('%type')
                    if '%rowtype' in i:
                        percentrowtype_count = percentrowtype_count + i.count('%rowtype')
                    if '@' in i:
                        i_atfinding = re.findall(r'@(.*?),|@(.*?);', i)
                        i_atfinding_list = [item for t in i_atfinding for item in t]
                        i_atfinding_list = [i.strip() for i in i_atfinding_list if i != '']
                        length_dblink = len(i_atfinding_list)
                        pack_dblink_count = pack_dblink_count + length_dblink * dblink_est_factor
                    if 'constant' in i:
                        pack_constants = pack_constants + i.count('constant ')
                    if 'record' in i:
                        record_split = re.split(r'\brecord \b', i)
                        record_length = len(record_split) - 1
                        pack_records = pack_records + record_length
                    if 'type' in i:
                        type_split = re.split(r'\btype \b', i)
                        type_length = len(type_split) - 1
                        pack_types = pack_types + type_length
                    if 'bulk collect' in i:
                        i.count('bulk collect')
                        bulk_collect = bulk_collect + i.count('bulk collect') * bulkcollect_est_factor
            if 'cursor' in j:
                cursor_colon = re.findall(r'\bcursor\b(.*?);', j, re.DOTALL)
                for i in cursor_colon:
                    i = 'cursor ' + i + ';'
                    from_btw_where = \
                        re.findall(r'\bfrom\b(.*?)where|\bfrom\b(.*?);|\bfrom\b(.*?)order by', i, re.DOTALL)
                    if from_btw_where:
                        from_btw_where = from_btw_where[0]
                        from_btw_where = str([i.strip() for i in from_btw_where if i != ''])
                        from_btw_where_comma_split = from_btw_where.split(',')
                        i_select_split = i.split('select')
                        number_select_sts = len(i_select_split) - 1
                        if len(from_btw_where_comma_split) == 1 and number_select_sts == 1:
                            simple_count = simple_count + 1
                        elif len(from_btw_where_comma_split) > 1 and len(
                                from_btw_where_comma_split) < 5 and number_select_sts == 1:
                            medium_count = medium_count + 1
                        elif len(from_btw_where_comma_split) > 5 and len(
                                from_btw_where_comma_split) < 10 and number_select_sts == 1:
                            complex_count = complex_count + 1
                        else:
                            result = (3 * number_select_sts) + 5
                            highly_complex_count = highly_complex_count + result
                total_estimation_cursor = (simple_count * detail_simple_est_factor) + (
                        medium_count * detail_medium_est_factor) + (
                                                  complex_count * detail_complex_est_factor) + highly_complex_count
            if 'pragma' in j:
                pack_pragma = pack_pragma + (j.count('pragma') * pragma_est_factor)
            if 'exception' in j:
                begin_when_end = re.findall(r'\bbegin\b(.*?)\bexception\b(.*?)\bend\b', j, re.DOTALL)
                begin_when_end_list = [i[1] for i in begin_when_end]
                when_end_str = ''.join(begin_when_end_list)
                if 'insert' in when_end_str or 'update' in when_end_str or 'delete' in when_end_str or 'select' in when_end_str:
                    dml_count_exce = dml_count_exce + (4 * when_end_str.count(';'))
                else:
                    dml_count_exce = dml_count_exce + (2 * when_end_str.count(';'))
            if 'raise' in j:
                begin_raise = re.findall(r'\bbegin\b(.*?)\braise\b', j, re.DOTALL)
                begin_raise_str = ''.join(begin_raise)
                pack_raise = pack_raise + (begin_raise_str.count(';') * 2)

        if 'package body' in j:
            is_as_declare_begin = re.findall(r'\bis\b(.*?)\bbegin\b|\bas\b(.*?)\bbegin\b|declare(.*?)begin', j,
                                             re.DOTALL)
            is_as_declare_begin_list = [item for t in is_as_declare_begin for item in t]
            is_as_declare_begin_list = [i.strip() for i in is_as_declare_begin_list if i != '']
            for i in is_as_declare_begin_list:
                if ';' in i:
                    body_semicolon_data = body_semicolon_data + i.count(';')
                if ',' in i:
                    i_comma = split_main(i)
                    length_comma = len(i_comma) - 1
                    body_comma_data = body_comma_data + length_comma
                if 'varchar2' in i:
                    body_varchar_count = body_varchar_count + i.count('varchar2')
                if 'number' in i:
                    body_number_count = body_number_count + i.count('number')
                if 'date' in i:
                    body_date_count = body_date_count + i.count('date')
                if '%type' in i:
                    body_percenttype_count = body_percenttype_count + i.count('%type')
                if '%rowtype' in i:
                    body_percentrowtype_count = body_percentrowtype_count + i.count('%rowtype')
                if '@' in i:
                    i_atfinding = re.findall(r'@(.*?),|@(.*?);', i)
                    i_atfinding_list = [item for t in i_atfinding for item in t]
                    i_atfinding_list = [i.strip() for i in i_atfinding_list if i != '']
                    length_dblink = len(i_atfinding_list)
                    body_pack_dblink_count = body_pack_dblink_count + length_dblink * dblink_est_factor
                if 'constant' in i:
                    body_pack_constants = body_pack_constants + i.count('constant')
                if 'record' in i:
                    record_split = re.split(r'\brecord \b', i)
                    record_length = len(record_split) - 1
                    body_pack_records = body_pack_records + record_length
                if 'type' in i:
                    type_split = re.split(r'\btype \b', i)
                    type_length = len(type_split) - 1
                    body_pack_types = body_pack_types + type_length
            if 'cursor' in j:
                cursor_colon_body = re.findall(r'cursor(.*?);', j, re.DOTALL)
                for i in cursor_colon_body:
                    i = 'cursor ' + i + ';'
                    from_btw_where_body = \
                        re.findall(r'\bfrom\b(.*?)where|\bfrom\b(.*?);|\bfrom\b(.*?)order by', i, re.DOTALL)
                    if from_btw_where_body:
                        from_btw_where_body = from_btw_where_body[0]
                        from_btw_where_tup = str([i.strip() for i in from_btw_where_body if i != ''])
                        from_btw_where_comma_split_tup = from_btw_where_tup.split(',')
                        i_select_split_body = i.split('select')
                        number_select_sts_body = len(i_select_split_body) - 1
                        if len(from_btw_where_comma_split_tup) == 1 and number_select_sts_body == 1:
                            body_simple_count = body_simple_count + 1
                        elif len(from_btw_where_comma_split_tup) > 1 and len(
                                from_btw_where_comma_split_tup) < 5 and number_select_sts_body == 1:
                            body_medium_count = body_medium_count + 1
                        elif len(from_btw_where_comma_split_tup) > 5 and len(
                                from_btw_where_comma_split_tup) < 10 and number_select_sts_body == 1:
                            body_complex_count = body_complex_count + 1
                        else:
                            result_body = (3 * number_select_sts_body) + 5
                            body_highly_complex_count = body_highly_complex_count + result_body
                total_body_estimation_cursor = (body_simple_count * detail_simple_est_factor) + (
                        body_medium_count * detail_medium_est_factor) + (
                                                       body_complex_count * detail_complex_est_factor) + body_highly_complex_count
            if 'pragma' in j:
                body_pragma = body_pragma + (j.count('pragma') * pragma_est_factor)
            if 'connect by prior' in j:
                body_connect_by = body_connect_by + (j.count('connect by prior') * conn_by_est_factor)
            if 'row_num' in j:
                body_row_number = body_row_number + (j.count('row_num') * row_num_est_factor)
            if 'pivot' in j:
                body_pivot = body_pivot + (j.count('pivot') * pivot_est_factor)
            if 'unpivot' in j:
                body_unpivot = body_unpivot + (j.count('unpivot') * unpivot_est_factor)
            if 'exception' in j:
                begin_when_end = re.findall(r'\bbegin\b(.*?)\bexception\b(.*?)\bend\b', j, re.DOTALL)
                begin_when_end_list = [i[1] for i in begin_when_end]
                when_end_str = ''.join(begin_when_end_list)
                if 'insert' in when_end_str or 'update' in when_end_str or 'delete' in when_end_str or 'select' in when_end_str:
                    body_dml_count_exce = body_dml_count_exce + (4 * when_end_str.count(';'))
                else:
                    body_dml_count_exce = body_dml_count_exce + (2 * when_end_str.count(';'))
            if 'raise' in j:
                begin_raise = re.findall(r'\bbegin\b(.*?)\braise\b', j, re.DOTALL)
                begin_raise_str = ''.join(begin_raise)
                body_raise = body_raise + (begin_raise_str.count(';') * 2)
            if 'bulk collect' in j:
                body_bulk_collect = body_bulk_collect + j.count('bulk collect') * bulkcollect_est_factor

    comma_semi_count = package_semicolon_count + package_comma_count + body_semicolon_data + body_comma_data
    varibles_count = (varchar_count * varchar_est_fac) + (number_count * number_est_fac) + (
            date_count * date_est_fac) + (percenttype_count * percentiletype_est_fac) + (
                             percentrowtype_count * percentiler_rowtype_est_fac) + (
                             body_varchar_count * varchar_est_fac) + (body_number_count * number_est_fac) + (
                             body_date_count * date_est_fac) + (body_percenttype_count * percentiletype_est_fac) + (
                             body_percentrowtype_count * percentiler_rowtype_est_fac)
    dblink_count = pack_dblink_count + body_pack_dblink_count
    constants_count = (pack_constants + body_pack_constants) * const_est_factor
    records_count = (pack_records + body_pack_records) * records_est_fac
    type_count = pack_types + body_pack_types
    cursors_count = total_estimation_cursor + total_body_estimation_cursor
    bulk_collect_count = bulk_collect + body_bulk_collect
    pragma_count = pack_pragma + body_pragma
    exception_count = dml_count_exce + body_dml_count_exce
    raise_count = pack_raise + body_raise
    total_package_count = comma_semi_count + varibles_count + raise_count + exception_count + dblink_count + constants_count + records_count + type_count + cursors_count + bulk_collect_count + pragma_count + body_connect_by + body_row_number + body_pivot + body_unpivot
    whole_detail_count = round(float(total_package_count))
    codeobj_list = arg_calculation_old(read_text_single_str, codeobj_list, read_text_single, whole_detail_count,
                                       'Package', total_inclusive_dict)
    return whole_detail_count, codeobj_list


def common_logic_proc_func(read_text_single, codeobj_list, total_inclusive_dict):
    read_text_single_str = read_text_single.lower().replace('  ', ' ').replace(')is', ') is').replace(')as', ') as')
    proc_semicolon_count = 0
    proc_varchar_count = 0
    proc_comma_count = 0
    proc_number_count = 0
    proc_percenttype_count = 0
    proc_percentrowtype_count = 0
    proc_date_count = 0
    proc_dblink_count = 0
    proc_constants = 0
    proc_records = 0
    proc_types = 0
    proc_bulk_collect = 0
    simple_count = 0
    medium_count = 0
    complex_count = 0
    highly_complex_count = 0
    total_estimation_cursor_proc = 0
    body_pragma = 0
    body_dml_count_exce = 0
    body_raise = 0
    whole_detail_count = 0
    body_connect_by = 0
    body_row_number = 0
    body_pivot = 0
    body_unpivot = 0
    total_proc_count = 0
    j = read_text_single_str
    if 'procedure' in j:
        detail_factor = detail_proc_est_factor
        detail_type = 'Procedure'
    else:
        detail_factor = detail_func_est_factor
        detail_type = 'Function'
    if 'procedure' in j or 'function' in j:
        proce_data = re.findall(r'\bis\b(.*?)\bbegin\b|\bas\b(.*?)\bbegin\b|declare(.*?)begin', j, re.DOTALL)
        proc_data_list = [item for t in proce_data for item in t]
        proc_data_list = [i.strip() for i in proc_data_list if i != '']
        for i in proc_data_list:
            if ';' in i:
                proc_semicolon_count = proc_semicolon_count + i.count(';')
            if ',' in i:
                i_comma = split_main(i)
                length_comma = len(i_comma) - 1
                proc_comma_count = proc_comma_count + length_comma
            if 'varchar2' in i:
                proc_varchar_count = proc_varchar_count + i.count('varchar2')
            if 'number' in i:
                proc_number_count = proc_number_count + i.count('number')
            if 'date' in i:
                proc_date_count = proc_date_count + i.count('date')
            if '%type' in i:
                proc_percenttype_count = proc_percenttype_count + i.count('%type')
            if '%rowtype' in i:
                proc_percentrowtype_count = proc_percentrowtype_count + i.count('%rowtype')
            if '@' in i:
                i_atfinding = re.findall(r'@(.*?),|@(.*?);', i)
                i_atfinding_list = [item for t in i_atfinding for item in t]
                i_atfinding_list = [i.strip() for i in i_atfinding_list if i != '']
                length_dblink = len(i_atfinding_list)
                proc_dblink_count = proc_dblink_count + length_dblink * dblink_est_factor
            if 'constant' in i:
                proc_constants = proc_constants + i.count('constant ')
            if 'record' in i:
                record_split = re.split(r'\brecord \b', i)
                record_length = len(record_split) - 1
                proc_records = proc_records + record_length
            if 'type' in i:
                type_split = re.split(r'\btype \b', i)
                type_length = len(type_split) - 1
                proc_types = proc_types + type_length
    if 'cursor' in j:
        cursor_colon = re.findall(r'\bcursor\b(.*?);', j, re.DOTALL)
        for i in cursor_colon:
            i = 'cursor ' + i + ';'
            if 'from' in i and ('where' in i or 'order by' in i):
                from_btw_where = \
                    re.findall(r'\bfrom\b(.*?)where|\bfrom\b(.*?);|\bfrom\b(.*?)order by', i, re.DOTALL)
                if from_btw_where:
                    from_btw_where = from_btw_where[0]
                    from_btw_where = str([i.strip() for i in from_btw_where if i != ''])
                    from_btw_where_comma_split = from_btw_where.split(',')
                    i_select_split = i.split('select')
                    number_select_sts = len(i_select_split) - 1
                    if len(from_btw_where_comma_split) == 1 and number_select_sts == 1:
                        simple_count = simple_count + 1
                    elif len(from_btw_where_comma_split) > 1 and len(
                            from_btw_where_comma_split) < 5 and number_select_sts == 1:
                        medium_count = medium_count + 1
                    elif len(from_btw_where_comma_split) > 5 and len(
                            from_btw_where_comma_split) < 10 and number_select_sts == 1:
                        complex_count = complex_count + 1
                    else:
                        result = (3 * number_select_sts) + 5
                        highly_complex_count = highly_complex_count + result
        total_estimation_cursor_proc = (simple_count * detail_simple_est_factor) + (
                medium_count * detail_medium_est_factor) + (
                                               complex_count * detail_complex_est_factor) + highly_complex_count
    if 'pragma' in j:
        body_pragma = body_pragma + (j.count('pragma') * dblink_estimate_factor)
    if 'connect by prior' in j:
        body_connect_by = body_connect_by + (j.count('connect by prior') * conn_by_est_factor)
    if 'row_num' in j:
        body_row_number = body_row_number + (j.count('row_num') * row_num_est_factor)
    if 'pivot' in j:
        body_pivot = body_pivot + (j.count('pivot') * pivot_est_factor)
    if 'unpivot' in j:
        body_unpivot = body_unpivot + (j.count('unpivot') * unpivot_est_factor)
    if 'exception' in j:
        begin_when_end = re.findall(r'\bbegin\b(.*?)\bexception\b(.*?)\bend\b', j, re.DOTALL)
        begin_when_end_list = [i[1] for i in begin_when_end]
        when_end_str = ''.join(begin_when_end_list)
        if 'insert' in when_end_str or 'update' in when_end_str or 'delete' in when_end_str or 'select' in when_end_str:
            body_dml_count_exce = body_dml_count_exce + (4 * when_end_str.count(';'))
        else:
            body_dml_count_exce = body_dml_count_exce + (2 * when_end_str.count(';'))
    if 'raise' in j:
        begin_raise = re.findall(r'\bbegin\b(.*?)\braise\b', j, re.DOTALL)
        begin_raise_str = ''.join(begin_raise)
        body_raise = body_raise + (begin_raise_str.count(';') * 2)
    if 'bulk collect' in j:
        proc_bulk_collect = proc_bulk_collect + j.count('bulk collect') * bulkcollect_est_factor
    total_proc_count = proc_semicolon_count + proc_varchar_count + proc_comma_count + proc_number_count + proc_date_count + proc_percenttype_count + proc_percentrowtype_count + proc_dblink_count + proc_constants + proc_records + proc_types + proc_bulk_collect + total_estimation_cursor_proc + body_pragma + body_dml_count_exce + body_raise + body_connect_by + body_row_number + body_pivot + body_unpivot
    whole_detail_count = whole_detail_count + total_proc_count
    codeobj_list = arg_calculation_old(read_text_single_str, codeobj_list, read_text_single, whole_detail_count,
                                       detail_type, total_inclusive_dict)

    return whole_detail_count, codeobj_list


# code objects

def packages(cschema_type, statements_list, single_connection, ceecount, simple_co_count,
             complex_co_count, total_inclusive_dict, all_codeobjects_list):
    print("PACKAGE SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)

    Pack_list_query = """
                SELECT object_name
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('PACKAGE')
                """
    Pack_def_query = """
                       with x as
                (
                SELECT owner, object_name, object_type
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('PACKAGE')
                AND object_name like '@name'
                )
                SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                            """

    sub_objects_count = []
    packages_list = []
    print("package def")
    query_oracle = str(Pack_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    # print(query_oracle)
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    pack_count = 0
    for item in lists:
        query_oracle2 = str(Pack_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('error', e)
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # query_text = query_text.read()
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"

            # Commented -- SK 8/5
            packages_list.append(query_text)
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'PACKAGES' + '/' + 'packages.sql'
            # create_and_append_sqlfile(sql_path_data, query_text) --SK ENded
        else:
            continue
    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        try:

            # Implementing the simple and complex code objects count logi --Sk 8/5
            simple_co_count, complex_co_count = checking_count(packages_list, simple_co_count, complex_co_count)

            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'PACKAGES' + '/' + 'packages.sql') as f:
            total_package_count = 0
            whole_pack = 0
            for pack_check in packages_list:
                codeobj_list = []
                data_new_sheet = []
                single_line_text = function_pre_lower(pack_check)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                pack_check_data = re.sub(r' +', ' ', single_line_text)
                total_package_count, codeobj_list = common_logic_package(pack_check_data, codeobj_list,
                                                                         total_inclusive_dict)

                if codeobj_list[4] != None:
                    data_val_four = re.findall(r'\d+\.\d+\(\d+\)', codeobj_list[4])
                    second_mod = []
                    for fn in data_val_four:
                        fn = fn.replace(')', '')
                        values_open = fn.split('(')
                        data_vales_open = float(values_open[0]) * float(values_open[1])
                        second_mod.append(data_vales_open)
                    min_second = sum(second_mod)
                    tot_sec_hr = min_second + int(codeobj_list[3])
                else:
                    tot_sec_hr = codeobj_list[3]
                    min_second = 0
                code_min_sec_list = [codeobj_list[0], codeobj_list[1], codeobj_list[2], codeobj_list[3],
                                     codeobj_list[4], codeobj_list[5], min_second, tot_sec_hr]
                whole_pack = whole_pack + total_package_count
                all_codeobjects_list.append(code_min_sec_list)
            sub_objects_count.extend(['Packages', len(lists), round(float(whole_pack / 60), 2)])
            statements_list.append(sub_objects_count)
        except FileNotFoundError as e:
            print('File Not Found for Packages')
            sub_objects_count.extend(['Packages', len(lists), 0])
            statements_list.append(sub_objects_count)
    elif str(ceecount).upper() == 'O':
        pack_est_h = round((obj_packages_est_factor * len(lists)) / 60, 2)
        # sub_objects_count.extend(["Packages", len(lists), (pack_est_factor * len(lists)), pack_est_h])
        sub_objects_count.extend(["Packages", len(lists), pack_est_h])
        statements_list.append(sub_objects_count)
    return simple_co_count, complex_co_count, all_codeobjects_list


def procedures(cschema_type, statements_list, single_connection, ceecount, simple_co_count,
               complex_co_count, total_inclusive_dict, all_codeobjects_list):
    print("PROCEDURE SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)
    procedure_list_query = """
               SELECT object_name
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('PROCEDURE')
                """
    procedure_def_query = """
                       with x as
                (
                SELECT owner, object_name, object_type
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('PROCEDURE')
                AND upper(object_name) = upper('@name')
                )
                SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                       """

    print("procedure def")
    sub_objects_count = []
    procedure_list = []
    query_oracle = str(procedure_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    # print(len(lists))
    proc_count = 0
    for item in lists:
        query_oracle2 = str(procedure_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('error', e)
            continue
        if len(def_orac_df.values):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            # read_text = query_text.read()
            query_text = query_text.read()
            query_text = query_text.replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            query_text_split = query_text.split(';')
            query_text_split = [i for i in query_text_split if i != '']
            proc_count = proc_count + len(query_text_split)
            procedure_list.append(query_text)

            # Commneted -- SK 8/5
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'PROCEDURES' + '/' + 'procedures.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:

            # Implementing the simple and complex code objects count logi --Sk 8/5
            simple_co_count, complex_co_count = checking_count(procedure_list, simple_co_count, complex_co_count)

            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'PROCEDURES' + '/' + 'procedures.sql') as f:
            whole_proc = 0
            for proc in procedure_list:
                codeobj_list = []
                data_new_sheet_proc_func = []
                # comments_remove = data_remove_comments(proc)
                single_line_text = function_pre_lower(proc)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                pack_check_data = re.sub(r' +', ' ', single_line_text)
                total_procedure_count, codeobj_list = common_logic_proc_func(pack_check_data,
                                                                             codeobj_list,
                                                                             total_inclusive_dict)

                if codeobj_list[4] != None:
                    data_val_four = re.findall(r'\d+\.\d+\(\d+\)', codeobj_list[4])
                    second_mod = []
                    for fn in data_val_four:
                        fn = fn.replace(')', '')
                        values_open = fn.split('(')
                        data_vales_open = float(values_open[0]) * float(values_open[1])
                        second_mod.append(data_vales_open)
                    min_second = sum(second_mod)
                    tot_sec_hr = min_second + int(codeobj_list[3])
                else:
                    tot_sec_hr = codeobj_list[3]
                    min_second = 0
                code_min_sec_list = [codeobj_list[0], codeobj_list[1], codeobj_list[2], codeobj_list[3],
                                     codeobj_list[4], codeobj_list[5], min_second, tot_sec_hr]
                whole_proc = whole_proc + total_procedure_count
                all_codeobjects_list.append(code_min_sec_list)
            sub_object_count.extend(['Procedures', len(lists), round(float(whole_proc / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as f:
            print('File Not Found for Procedures')
            sub_object_count.extend(['Procedures', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        proc_est_h = round((obj_procedure_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(["Procedures", len(lists), (proc_est_factor * len(lists)), proc_est_h])
        sub_object_count.extend(["Procedures", len(lists), proc_est_h])
        statements_list.append(sub_object_count)

    # proc_est_factor = 360
    # proc_est_h = round((proc_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Procedures", len(lists), proc_count, proc_est_factor, proc_est_h])
    # statements_list.append(sub_objects_count)
    print("PROCEDURE1 SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)
    return simple_co_count, complex_co_count, all_codeobjects_list


def functions(cschema_type, statements_list, single_connection, ceecount, simple_co_count,
              complex_co_count, total_inclusive_dict, all_codeobjects_list):
    print("FUNCTION SIMPLE AND COMPLEX COUNT STARTED ", simple_co_count, complex_co_count)
    function_list_query = """
                SELECT object_name
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('FUNCTION')
                        """
    function_def_query = """
                      with x as
                (
                SELECT owner, object_name, object_type
                FROM dba_objects
                WHERE owner = upper('@schemaname')
                AND object_type IN ('FUNCTION')
                AND upper(object_name) = upper('@name')
                )
                SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                      """

    print("function def")
    sub_objects_count = []
    functions_list = []
    query_oracle = str(function_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))
    func_count = 0
    for item in lists:
        query_oracle2 = str(function_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('error', e)
            continue
        if len(def_orac_df.values):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            # read_text = query_text.read()
            query_text = query_text.read()
            query_text = query_text.replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            # query_text_rm_comm = data_remove_comments(query_text)
            query_text_split = query_text.split(';')
            query_text_split = [i for i in query_text_split if i != '']
            func_count = func_count + len(query_text_split)
            functions_list.append(query_text)

            # comm -SK 8/5
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'FUNCTIONS' + '/' + 'functions.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue

    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:

            # Implementing the simple and complex code objects count logi --Sk 7/22
            simple_co_count, complex_co_count = checking_count(functions_list, simple_co_count, complex_co_count)

            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'FUNCTIONS' + '/' + 'functions.sql') as f:
            whole_func = 0
            for func in functions_list:
                codeobj_list = []
                data_new_sheet_proc_func = []
                # comments_remove = data_remove_comments(func)
                single_line_text = function_pre_lower(func)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                comments_remove = re.sub(r' +', ' ', single_line_text)
                total_functions_count, codeobj_list = common_logic_proc_func(comments_remove,
                                                                             codeobj_list,
                                                                             total_inclusive_dict)

                if codeobj_list[4] != None:
                    data_val_four = re.findall(r'\d+\.\d+\(\d+\)', codeobj_list[4])
                    second_mod = []
                    for fn in data_val_four:
                        fn = fn.replace(')', '')
                        values_open = fn.split('(')
                        data_vales_open = float(values_open[0]) * float(values_open[1])
                        second_mod.append(data_vales_open)
                    min_second = sum(second_mod)
                    tot_sec_hr = min_second + int(codeobj_list[3])
                else:
                    tot_sec_hr = codeobj_list[3]
                    min_second = 0
                code_min_sec_list = [codeobj_list[0], codeobj_list[1], codeobj_list[2], codeobj_list[3],
                                     codeobj_list[4], codeobj_list[5], min_second, tot_sec_hr]
                whole_func = whole_func + total_functions_count
                all_codeobjects_list.append(code_min_sec_list)
            sub_object_count.extend(['Functions', len(lists), round(float(whole_func / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as e:
            print('File Not Found for Functions')
            sub_object_count.extend(['Functions', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        func_est_h = round((obj_function_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(["Functions", len(lists), (func_est_factor * len(lists)), func_est_h])
        sub_object_count.extend(["Functions", len(lists), func_est_h])
        statements_list.append(sub_object_count)

    # func_est_factor = 360
    # func_est_h = round((func_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Functions", len(lists), func_count, func_est_factor, func_est_h])
    # statements_list.append(sub_objects_count)

    print("FUNCTION SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)
    return simple_co_count, complex_co_count, all_codeobjects_list


def triggers(cschema_type, statements_list, single_connection, ceecount, simple_co_count,
             complex_co_count, all_codeobjects_list,
             total_inclusive_dict):
    print("TRIGGER SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)
    trigger_list_query = "SELECT object_name FROM dba_objects WHERE owner = upper('@schemaname') AND object_type IN ('TRIGGER')"
    trigger_def_query = """
                        with x as
                                (
                                SELECT owner, object_name, object_type
                                FROM dba_objects
                                WHERE owner = upper('@schemaname')
                                AND object_type IN ('TRIGGER')
                                AND object_name = '@name'
                                )
                                SELECT  DBMS_METADATA.get_ddl (x.object_type, x.object_name, x.owner) as ddlcode FROM x
                        """

    print("trigger def")
    sub_objects_count = []
    triggers_list = []
    query_oracle = str(trigger_list_query).replace('@schemaname', cschema_type).replace('@order', '')
    orac_df = oracle_connection(query_oracle, single_connection)
    lists = orac_df[orac_df.columns[0]].values.tolist()
    print(len(lists))
    trigger_count = 0
    for item in lists:
        query_oracle2 = str(trigger_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:
            def_orac_df = exe_oracle_connection(query_oracle2, single_connection)
        except Exception as e:
            print('ERROR', e)
            continue
        if len(def_orac_df.values):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # read_text = query_text.read()
            # query_text = query_text.read()
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"

            query_text_split = query_text.split(';')
            query_text_split = [i for i in query_text_split if i != '']
            trigger_count = trigger_count + len(query_text_split)
            triggers_list.append(query_text)
            # sql_path_data = 'Source_' + cschema_type + '/' + str(
            #     cschema_type).upper() + '/' + 'TRIGGERS' + '/' + 'triggers.sql'
            # create_and_append_sqlfile(sql_path_data, query_text)
        else:
            continue

    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
            # Implementing the simple and complex code objects count logi --Sk 7/22
            simple_co_count, complex_co_count = checking_count(triggers_list, simple_co_count, complex_co_count)

            # with open('Source' + '/' + str(cschema_type).upper() + '/' + 'TRIGGERS' + '/' + 'triggers.sql') as f:
            aa_triggers_str = '\n'.join(triggers_list)
            aa_triggers_str = re.sub(r' +', ' ', aa_triggers_str)
            data = aa_triggers_str
            # data = f.read().strip()
            data = data.replace('  ', '')
            single_line_text = function_pre_lower(data)
            single_line_text = single_line_text + "\n"
            single_line_text = regexarrowencrypt(single_line_text)
            single_line_text = rm_singlequ(single_line_text)
            single_line_text = regexarrowdecrypt(single_line_text)
            data_split = single_line_text.split('create or replace')
            type_comma_count = 0
            type_semicolon_count = 0
            whole_detail_count = 0
            whole_pack = 0
            for triggers_check in triggers_list:
                codeobj_list = []
                data = function_pre_lower(triggers_check)
                # single_line_text = function_pre_lower(pack_check)
                single_line_text = data + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                single_line_text = re.sub(r' +', ' ', single_line_text)
                if 'trigger' in single_line_text:
                    type_comma_count = type_comma_count + single_line_text.count(',')
                    type_semicolon_count = type_semicolon_count + single_line_text.count(';')
                    whole_detail_count = type_comma_count + type_semicolon_count
                    whole_pack = whole_pack + whole_detail_count
                    codeobj_list = arg_calculation_old(single_line_text, codeobj_list, single_line_text,
                                                       whole_detail_count, 'Triggers', total_inclusive_dict)

                    if codeobj_list[4] != None:
                        data_val_four = re.findall(r'\d+\.\d+\(\d+\)', codeobj_list[4])
                        second_mod = []
                        for fn in data_val_four:
                            fn = fn.replace(')', '')
                            values_open = fn.split('(')
                            data_vales_open = float(values_open[0]) * float(values_open[1])
                            second_mod.append(data_vales_open)
                        min_second = sum(second_mod)
                        tot_sec_hr = min_second + int(codeobj_list[3])
                    else:
                        tot_sec_hr = codeobj_list[3]
                        min_second = 0
                    code_min_sec_list = [codeobj_list[0], codeobj_list[1], codeobj_list[2], codeobj_list[3],
                                         codeobj_list[4], codeobj_list[5], min_second, tot_sec_hr]

                    all_codeobjects_list.append(code_min_sec_list)
            whole_pack = whole_pack
            sub_object_count.extend(['Triggers', len(lists), round(float(whole_pack / 60), 2)])
            statements_list.append(sub_object_count)
        except FileNotFoundError as e:
            print("File Not Found for Triggers")
            sub_object_count.extend(['Triggers', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        trig_est_h = round((obj_trigger_est_factor * len(lists)) / 60, 2)
        sub_object_count.extend(["Triggers", len(lists), trig_est_h])
        statements_list.append(sub_object_count)

    # trig_est_factor = 360
    # trig_est_h = round((trig_est_factor * len(lists)) / 60, 2)
    # sub_objects_count.extend(["Triggers", len(lists), trigger_count, trig_est_factor, trig_est_h])
    # statements_list.append(sub_objects_count)
    print("TRIGGER! SIMPLE AND COMPLEX COUNT ENDED ", simple_co_count, complex_co_count)
    return simple_co_count, complex_co_count, all_codeobjects_list


def job_schedule_program(cschema_type, statements_list, single_connection, ceecount):
    schema_job_query = """select count(1) from dba_objects where object_type = 'JOB' AND upper(owner) = '@schemaname'"""
    schema_schedule_query = """select count(1) from dba_objects where object_type = 'SCHEDULE' AND upper(owner) = '@schemaname'"""
    schema_program_query = """select count(1) from dba_objects where object_type = 'PROGRAM' AND upper(owner) = '@schemaname'"""

    count_querys1 = [schema_job_query, schema_schedule_query, schema_program_query]
    obj_list1 = ['Jobs', 'Schedule', 'Program']
    for i, j, k in zip(count_querys1, obj_list1, job_schedule_program_esti_factor):
        sub_objects_count = []
        count_query = i.replace('@schemaname', cschema_type.upper())
        data = get_count(single_connection, count_query)
        if data:
            part1 = data[0][0]
            # part2 = data[0][1]
            estimate_factor = k
            if str(ceecount).upper() == 'D':
                esimate = round((estimate_factor * part1) / 60, 2)
                # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
                sub_objects_count.extend([j, part1, esimate])
                statements_list.append(sub_objects_count)
            elif str(ceecount).upper() == 'O':
                esimate = round((estimate_factor * part1) / 60, 2)
                # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
                sub_objects_count.extend([j, part1, esimate])
                statements_list.append(sub_objects_count)


def all_users(username, password, hostname, port, sid):
    extract_data = """SELECT username FROM DBA_USERS WHERE username NOT IN ( 'SYSTEM', 'SYS', 'APPQOSSYS', 'REMOTE_SCHEDULER_AGENT', 'DBSFWUSER', 'CTXSYS', 'SI_INFORMTN_SCHEMA', 'PUBLIC', 'AUDSYS', 'OJVMSYS', 'DVSYS', 'GSMADMIN_INTERNAL', 'ORDPLUGINS', 'ORDDATA', 'MDSYS', 'LBACSYS', 'OLAPSYS', 'OUTLN', 'ORACLE_OCM', 'XDB', 'WMSYS', 'ORDSYS', 'DBSNMP', 'DVF', 'APEX_030200', 'EXFSYS', 'OWBSYS', 'OWBSYS_AUDIT', 'SYSMAN', 'SCOTT') AND Account_status = 'OPEN' order by Username"""
    query_oracle = str(extract_data)
    connection = oracle_single_connection(username, password, hostname, port, sid)
    orac_df = oracle_connection_for_mainscreen(query_oracle, connection)
    lists_all_schemas = orac_df[orac_df.columns[0]].values.tolist()
    return connection, lists_all_schemas


def storage_utility(cschema_type, statements_list, single_connection, ceecount):
    simple_so_count = 0
    complex_so_count = 0

    # Commented by --Sk 8/5
    datatypes(cschema_type, statements_list, single_connection,
              ceecount)  # for datatype extraction

    tables(cschema_type, statements_list, single_connection, ceecount)  # for Tables extraction

    if str(ceecount).upper() == 'D':
        column(cschema_type, statements_list, single_connection,
               ceecount)  # for extracting columns
        partitions(cschema_type, statements_list, single_connection,
                   ceecount)  # for extracting partitions

    check_constarints(cschema_type, statements_list, single_connection,
                      ceecount)  # for check constraints extraction

    # if str(ceecount).upper() == 'O':
    prim_forei(cschema_type, statements_list, single_connection, ceecount)

    indexes(cschema_type, statements_list, single_connection, ceecount)  # for indexes extraction

    sequences(cschema_type, statements_list, single_connection,
              ceecount)  # for sequences extraction

    synonyms(cschema_type, statements_list, single_connection,
             ceecount)  # for synonyms extraction

    simple_so_count, complex_so_count = views(cschema_type, statements_list, single_connection,
                                              ceecount, simple_so_count,
                                              complex_so_count)  # for views and mviews extraction

    simple_so_count, complex_so_count = mview(cschema_type, statements_list, single_connection,
                                              ceecount, simple_so_count, complex_so_count)

    types(cschema_type, statements_list, single_connection, ceecount)  # for types extraction
    dblink_storage(cschema_type, statements_list, single_connection,
                   ceecount)  # for counting dblink

    return simple_so_count, complex_so_count


def codeobjects_utility(cschema_type, statements_list, single_connection, ceecount,
                        all_codeobjects_list, total_inclusive_dict):
    simple_co_count = 0
    complex_co_count = 0

    simple_co_count, complex_co_count, all_codeobjects_list = packages(cschema_type, statements_list, single_connection, ceecount,
                                                                       simple_co_count,
                                                                       complex_co_count, total_inclusive_dict,
                                                                       all_codeobjects_list)  # for packages extraction

    simple_co_count, complex_co_count, all_codeobjects_list = procedures(cschema_type, statements_list,single_connection,
                                                                         ceecount, simple_co_count,
                                                                         complex_co_count, total_inclusive_dict,
                                                                         all_codeobjects_list)  # for procedures extraction

    simple_co_count, complex_co_count, all_codeobjects_list = functions(cschema_type, statements_list, single_connection,
                                                                        ceecount, simple_co_count,
                                                                        complex_co_count, total_inclusive_dict,
                                                                        all_codeobjects_list)  # for functions extraction

    simple_co_count, complex_co_count, all_codeobjects_list = triggers(cschema_type, statements_list,single_connection,
                                                                       ceecount, simple_co_count, complex_co_count,
                                                                       all_codeobjects_list,
                                                                       total_inclusive_dict)  # for triggers extraction

    job_schedule_program(cschema_type, statements_list, single_connection,
                         ceecount)  # for job, program, schedule codeonjects

    return simple_co_count, complex_co_count, all_codeobjects_list


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


# def oracle_connection_for_mainscreen(query, username, password, hostname, port, sid):
#     dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
#     connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
#     print("connected to the Oracle Database")
#     df_ora = pd.read_sql(query, con=connection)
#     # df_ora = df_ora.apply(lambda x: x.astype(str).str.upper())
#     print("data", df_ora)
#     print("length of oracle df", len(df_ora))
#     return df_ora

def oracle_single_connection(username, password, hostname, port, sid):
    dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    print("connected to the Oracle Database For Single Time")
    return connection

def oracle_connection_for_mainscreen(query, connection):
    # dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    # connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    # print("connected to the Oracle Database")
    df_ora = pd.read_sql(query, con=connection)
    # df_ora = df_ora.apply(lambda x: x.astype(str).str.upper())
    print("data", df_ora)
    print("length of oracle df", len(df_ora))
    return df_ora


def oracle_connection(query,connection):
    # dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    # connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    # print("connected to the Oracle Database")
    df_ora = pd.read_sql(query, con=connection)
    # df_ora = df_ora.apply(lambda x: x.astype(str).str.upper())
    print("data", df_ora)
    print("length of oracle df", len(df_ora))
    return df_ora


def oracle_conn_data_type(query, connection):
    # dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    # connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)  -- Commnetd By SK 12/27
    # print("connected to the Oracle Database")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception as err:
        print(err)
    finally:
        cursor.close()
    return data


def exe_oracle_connection(query, connection):
    # dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    # connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    cursor = connection.cursor()
    cursor.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false); end;")
    cursor.execute(
        "begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false); end;")
    # cursor.callproc('DBMS_METADATA.SET_TRANSFORM_PARAM',
    #                 [-1, 'TABLESPACE', False])
    # cursor.callproc('DBMS_METADATA.SET_TRANSFORM_PARAM',
    #                 [-1, 'SEGMENT_ATTRIBUTES', False])
    cursor.execute(query)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df_ora = pd.DataFrame(rows, columns=names)
    return df_ora


def create_and_append_sqlfile(path_of_file_sql, data):
    with open(path_of_file_sql, 'a') as f:
        f.write("{}\n\n\n\n".format(data))
        f.close()


def meta_excel(cschema_type, username, password, hostname, port, sid):
    Meta_list_tables = """select DISTINCT OBJECT_NAME from dba_objects a where NOT exists (select 1 from dba_objects b where a.object_name=b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            AND A.OWNER=upper('@schemaname') and OBJECT_NAME not like '%$%'
            AND A.OBJECT_TYPE='TABLE' and TEMPORARY='N' ORDER BY 1"""

    Meta_list_DatatypeMapping = """
            select table_name,column_name,data_type, DATA_LENGTH ,DATA_PRECISION,DATA_SCALE from dba_tab_columns where OWNER=upper('@schemaname') and table_name not like '%$%' order by table_name,COLUMN_ID
        """

    Meta_list_views = """SELECT view_name
            FROM dba_views
            WHERE owner = upper('@schemaname') and view_name not like '%$%'
            order by view_name"""
    Meta_list_Sequences = """
                   SELECT Sequence_name
            FROM dba_sequences
            WHERE sequence_owner = upper('@schemaname') and Sequence_name not like 'ISEQ$$_%'
            ORDER BY Sequence_name
        """
    Meta_list_mviews = """
    select mview_name from dba_mviews where upper(owner)=upper('@schemaname')  and mview_name not like '%$%' order by 1
          """
    Meta_list_primary_keys = """
        Select dba_cons_columns.table_name, dba_cons_columns.column_name as PKey
            from dba_constraints, dba_cons_columns
            where
            dba_constraints.constraint_type = 'P'
            and dba_constraints.constraint_name = dba_cons_columns.constraint_name
            and dba_constraints.owner = dba_cons_columns.owner
            and dba_cons_columns.owner = upper('@schemaname') and dba_cons_columns.table_name not like '%$%'
            AND NOT EXISTS (select 1 from dba_objects b where dba_cons_columns.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            order by
            dba_cons_columns.table_name,
            dba_cons_columns.column_name
        """
    Meta_list_Indexes = """
        select * from (
                WITH cols AS (
                SELECT idx.owner AS schema_name, idx.table_name, idx.index_name, cols.column_name, cols.column_position,
                idx.uniqueness, decode(cols.descend, 'ASC', '', ' '||cols.descend) descend
                FROM DBA_INDEXES idx, DBA_IND_COLUMNS cols
                WHERE idx.owner = cols.index_owner AND idx.table_name = cols.table_name AND idx.index_name = cols.index_name
                AND idx.owner = upper('@schemaname') and idx.table_name not like '%$%'
                ),
                expr AS (
                SELECT extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
                , extractValue(xs.object_value, '/ROW/INDEX_NAME') AS index_name
                , extractValue(xs.object_value, '/ROW/COLUMN_EXPRESSION') AS column_expression
                , extractValue(xs.object_value, '/ROW/COLUMN_POSITION') AS column_position
                FROM (
                SELECT XMLTYPE(
                DBMS_XMLGEN.GETXML( 'SELECT table_name, index_name, column_expression, column_position FROM
                DBA_IND_EXPRESSIONS WHERE index_owner = upper(''@schemaname'')  and table_name not like ''%$%'' '
                ||' union all SELECT null, null, null, null FROM dual '
                )
                ) AS xml FROM DUAL
                ) x
                , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
                )
                SELECT
                cols.table_name,
                listagg(CASE WHEN cols.column_name LIKE 'SYS_N%' THEN expr.column_expression || cols.descend ELSE
                cols.column_name || cols.descend END, ', ') within group(order by cols.column_position) as Index_Cols,
                cols.index_name
                FROM cols
                LEFT OUTER JOIN expr ON cols.table_name = expr.table_name
                AND cols.index_name = expr.index_name
                AND cols.column_position = expr.column_position
                GROUP BY cols.schema_name, cols.table_name, cols.index_name, cols.uniqueness
                ORDER BY cols.table_name, cols.index_name, cols.uniqueness
                ) res  where not exists (select 1 from dba_mviews mv where mv.MVIEW_NAME=res.table_name and lower(owner)=lower('@schemaname'))

        """

    Meta_list_checkcponst_count = """
            select count(1) as count from (
            SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
            cols.column_name
            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
            cols.constraint_name
            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            AND cons.owner = upper('@schemaname') AND cons.constraint_type = 'C'   AND cons.table_name NOT LIKE '%$%')
            """
    Meta_list_check_constrains = """
         select * from ( WITH ref AS (
            SELECT extractValue(xs.object_value, '/ROW/OWNER') AS schema_name
            , extractValue(xs.object_value, '/ROW/TABLE_NAME') AS table_name
            , extractValue(xs.object_value, '/ROW/CONSTRAINT_NAME') AS object_name
            , extractValue(xs.object_value, '/ROW/SEARCH_CONDITION') AS condition_column
            , extractValue(xs.object_value, '/ROW/COLUMN_NAME') AS column_name
            FROM (
            SELECT XMLTYPE(
            DBMS_XMLGEN.GETXML('SELECT cons.owner, cons.table_name, cons.constraint_name, cons.search_condition,
            cols.column_name
            FROM dba_CONSTRAINTS cons, dba_CONS_COLUMNS cols
            WHERE cons.owner = cols.owner AND cons.table_name = cols.table_name AND cons.constraint_name =
            cols.constraint_name
            AND NOT EXISTS (select 1 from dba_objects b where cons.table_name =b.object_name AND OBJECT_TYPE=''MATERIALIZED VIEW'')
            AND cons.owner = upper(''@schemaname'') and cons.constraint_type = ''C'' '
            )
            ) AS xml FROM DUAL
            ) x
            , TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs
            )
            SELECT table_name AS table_name,
            trim(upper(replace(check_condition, '"', ''))) AS check_condition
            FROM (
            SELECT
            schema_name,
            table_name,
            object_name,
            'CHECK' AS constraint_type,
            condition_column AS check_condition
            FROM ref
            UNION
            SELECT
            owner AS schema_name,
            table_name,
            'SYS_C0000'||column_id AS object_name,
            'CHECK' AS constraint_type,
            '"'||column_name||'" IS NOT NULL' AS check_condition
            FROM DBA_tab_columns tcols where owner = upper('@schemaname') and nullable = 'N'
            AND NOT EXISTS (select 1 from dba_objects b where tcols.table_name =b.object_name AND OBJECT_TYPE='MATERIALIZED VIEW')
            AND NOT EXISTS ( SELECT 1 FROM ref WHERE ref.table_name = tcols.table_name
            AND ref.schema_name = tcols.owner
            AND ref.column_name = tcols.column_name
            AND ref.condition_column = '"'||tcols.column_name||'" IS NOT NULL')
            /* ALL_TAB_COLUMNS contains Tables and Views. Add below to exclude Views NOT NULL constraints */
            AND NOT EXISTS ( SELECT 1 FROM DBA_VIEWS vw WHERE vw.view_name = tcols.table_name
            AND vw.owner = tcols.owner
            )
            )
            ) where table_name not like '%$%'

        """
    Meta_list_foreign_keys = """
        SELECT  /*+ RULE */
            c.table_name,
            'FOREIGN KEY ('|| cc.fk_column || ') REFERENCES ' || p.parent_tab_owner || '.' || p.table_name || '('||
            pc.ref_column ||')' AS Foreign_Key
            FROM ( SELECT owner child_tab_owner, table_name, constraint_name, r_constraint_name FROM dba_CONSTRAINTS
            WHERE owner = upper('@schemaname') AND constraint_type = 'R' and table_name not like '%$%') c,
            ( SELECT owner parent_tab_owner, table_name, constraint_name FROM dba_CONSTRAINTS WHERE owner =
            upper('@schemaname') AND constraint_type IN('P', 'U')  and table_name not like '%$%') p,
            ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
            fk_column
            FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) cc,
            ( SELECT owner, table_name, constraint_name, listagg(column_name, ', ') WITHIN group(ORDER BY position)
            ref_column
            FROM dba_CONS_COLUMNS WHERE owner = upper('@schemaname') and table_name not like '%$%' GROUP BY owner, table_name, constraint_name ) pc
            WHERE c.r_constraint_name = p.constraint_name
            AND c.table_name = cc.table_name AND c.constraint_name = cc.constraint_name AND c.child_tab_owner = cc.owner
            AND p.table_name = pc.table_name AND p.constraint_name = pc.constraint_name AND p.parent_tab_owner =
            pc.owner
            order by c.table_name

        """
    Meta_list_synonyms = """select SYNONYM_NAME,TABLE_NAME,TABLE_OWNER from dba_synonyms  where owner=upper('@schemaname')"""
    Meta_list_Code_objects = """
         SELECT
                object_type,
                case
                when object_type= 'PACKAGE' then object_name||'_'||Method_name
                when object_type= 'TYPE' then object_name||'_'||Method_name
                when object_type = 'PROCEDURE' then object_name
                when object_type = 'FUNCTION' then object_name
                END as Code_Object_Name
                FROM
                (
                SELECT
                object_type,
                object_name,
                case
                when object_type='PACKAGE' then procedure_name
                when object_type='TYPE' then procedure_name
                when object_type = 'PROCEDURE' then object_name
                when object_type = 'FUNCTION' then object_name
                END as Method_name
                FROM dba_procedures
                where owner = upper('@schemaname')
                and
                OBJECT_TYPE IN
                (
                'PROCEDURE',
                'FUNCTION',
                'PACKAGE',
                'TYPE'
                )
                )a
                where method_name is not null
                order by object_type, Code_Object_Name
        """
    Meta_list_userdefined_types = """
            select type_name from dba_types
            where owner = upper('@schemaname')
            order by type_name
        """
    Meta_list_triggeres = """
         SELECT
            table_name,
            trigger_name
            FROM DBA_TRIGGERS
            WHERE owner = upper('@schemaname') and table_name not like '%$%'
            ORDER BY table_name,
            trigger_name
        """
    Meta_list_GTT = """select table_name from dba_tables where TEMPORARY = 'Y' AND OWNER = upper('@schemaname') AND table_name NOT LIKE '%$%'"""

    query_oracle_datatype = str(Meta_list_DatatypeMapping).replace('@schemaname', cschema_type)
    query_oracle_table = str(Meta_list_tables).replace('@schemaname', cschema_type)
    query_oracle_view = str(Meta_list_views).replace('@schemaname', cschema_type)
    query_oracle_sequence = str(Meta_list_Sequences).replace('@schemaname', cschema_type)
    query_oracle_mview = str(Meta_list_mviews).replace('@schemaname', cschema_type)
    query_oracle_primary_key = str(Meta_list_primary_keys).replace('@schemaname', cschema_type)
    query_oracle_index = str(Meta_list_Indexes).replace('@schemaname', cschema_type)
    query_oracle_check_constrain = str(Meta_list_check_constrains).replace('@schemaname', cschema_type)
    query_oracle_foreign_key = str(Meta_list_foreign_keys).replace('@schemaname', cschema_type)
    query_oracle_code_object = str(Meta_list_Code_objects).replace('@schemaname', cschema_type)
    query_oracle_userdefined_type = str(Meta_list_userdefined_types).replace('@schemaname', cschema_type)
    query_oracle_trigger = str(Meta_list_triggeres).replace('@schemaname', cschema_type)
    query_oracle_sys = str(Meta_list_synonyms).replace('@schemaname', cschema_type)
    query_oracle_gtt = str(Meta_list_GTT).replace('@schemaname', cschema_type)

    print(" Excel Query for Tables")
    orac_df_table = oracle_connection(query_oracle_table, username, password, hostname, port, sid, cschema_type)
    print(" Excel Query for Datatype")
    orac_df_datatype = oracle_connection(query_oracle_datatype, username, password, hostname, port, sid, cschema_type)

    print(" Excel Query for Views")
    orac_df_view = oracle_connection(query_oracle_view, username, password, hostname, port, sid, cschema_type)
    print(" Excel Query for Sequence")
    orac_df_sequence = oracle_connection(query_oracle_sequence, username, password, hostname, port, sid, cschema_type)

    print(" Excel Query for MVIEW")
    orac_df_mview = oracle_connection(query_oracle_mview, username, password, hostname, port, sid, cschema_type)
    print(" Excel Query for Primary Key")
    orac_df_primary_key = oracle_connection(query_oracle_primary_key, username, password, hostname, port, sid,
                                            cschema_type)
    print(" Excel Query for Index")
    orac_df_index = oracle_connection(query_oracle_index, username, password, hostname, port, sid, cschema_type)

    print(" Excel Query for CheckConstr  Count")
    orac_df_checkconstcount = oracle_connection(Meta_list_checkcponst_count, username, password, hostname, port, sid,
                                                cschema_type)
    # print('completed uptp')
    check_count = orac_df_checkconstcount['COUNT'][0]
    if int(check_count) != 0:
        print(" Excel Query for Check const count after checking")
        orac_df_check_constrain = oracle_connection(query_oracle_check_constrain, username, password, hostname, port,
                                                    sid,
                                                    cschema_type)
    else:
        print(" Excel Query for Check const count after checking if it is zero taking empty")
        orac_df_check_constrain = pd.DataFrame(columns=['TABLE_NAME', 'CHECK_CONDITION'])
    # print('completed uptp')
    print(" Excel Query for Foreign Key")
    orac_df_foreign_key = oracle_connection(query_oracle_foreign_key, username, password, hostname, port, sid,
                                            cschema_type)
    print(" Excel Query for CodeObjects")
    orac_df_code_object = oracle_connection(query_oracle_code_object, username, password, hostname, port, sid,
                                            cschema_type)
    print(" Excel Query for Userdefined types")
    orac_df_userdefined_type = oracle_connection(query_oracle_userdefined_type, username, password, hostname, port, sid,
                                                 cschema_type)
    print(" Excel Query for Triggers")
    orac_df_trigger = oracle_connection(query_oracle_trigger, username, password, hostname, port, sid, cschema_type)
    print(" Excel Query for Sys")
    orac_df_sys = oracle_connection(query_oracle_sys, username, password, hostname, port, sid, cschema_type)
    print(" Excel Query for GTT")
    orac_df_gtt = oracle_connection(query_oracle_gtt, username, password, hostname, port, sid, cschema_type)

    #
    # Meta_list_checkcponst_count

    MetaData_excelpath_all = 'Source' + '/' + str(cschema_type).upper() + '/' + 'MetaData' + '/' + str(
        cschema_type) + '_ALL_validation.xlsx'
    MetaData_excelpath_so = 'Source' + '/' + str(cschema_type).upper() + '/' + 'MetaData' + '/' + str(
        cschema_type) + '_SO_validation.xlsx'
    MetaData_excelpath_co = 'Source' + '/' + str(cschema_type).upper() + '/' + 'MetaData' + '/' + str(
        cschema_type) + 'CO_validation.xlsx'

    with pd.ExcelWriter(MetaData_excelpath_all) as writer1:
        orac_df_table.to_excel(writer1, sheet_name='Tables', index=False)
        orac_df_datatype.to_excel(writer1, sheet_name='DataTypeMapping', index=False)
        orac_df_view.to_excel(writer1, sheet_name='Views', index=False)
        orac_df_sequence.to_excel(writer1, sheet_name='Sequences', index=False)
        orac_df_primary_key.to_excel(writer1, sheet_name='Primary_Keys', index=False)
        orac_df_index.to_excel(writer1, sheet_name='Indexes', index=False)
        orac_df_check_constrain.to_excel(writer1, sheet_name='Check_Constraints', index=False)
        orac_df_foreign_key.to_excel(writer1, sheet_name='Foreign_Keys', index=False)
        orac_df_code_object.to_excel(writer1, sheet_name='Code_Objects', index=False)
        orac_df_userdefined_type.to_excel(writer1, sheet_name='User_Defined_Types', index=False)
        orac_df_trigger.to_excel(writer1, sheet_name='Triggers', index=False)
        orac_df_mview.to_excel(writer1, sheet_name='Materialized_views', index=False)
        orac_df_sys.to_excel(writer1, sheet_name='Synonym', index=False)
        orac_df_gtt.to_excel(writer1, sheet_name='Global_Temp_Tables', index=False)
    with pd.ExcelWriter(MetaData_excelpath_so) as writer2:
        orac_df_table.to_excel(writer2, sheet_name='Tables', index=False)
        orac_df_datatype.to_excel(writer2, sheet_name='DataTypeMapping', index=False)
        orac_df_view.to_excel(writer2, sheet_name='Views', index=False)
        orac_df_sequence.to_excel(writer2, sheet_name='Sequences', index=False)
        orac_df_primary_key.to_excel(writer2, sheet_name='Primary_Keys', index=False)
        orac_df_index.to_excel(writer2, sheet_name='Indexes', index=False)
        orac_df_check_constrain.to_excel(writer2, sheet_name='Check_Constraints', index=False)
        orac_df_foreign_key.to_excel(writer2, sheet_name='Foreign_Keys', index=False)
        orac_df_userdefined_type.to_excel(writer2, sheet_name='User_Defined_Types', index=False)
        orac_df_mview.to_excel(writer2, sheet_name='Materialized_views', index=False)
        orac_df_sys.to_excel(writer2, sheet_name='Synonym', index=False)
        orac_df_gtt.to_excel(writer2, sheet_name='Global_Temp_Tables', index=False)
    with pd.ExcelWriter(MetaData_excelpath_co) as writer3:
        orac_df_code_object.to_excel(writer3, sheet_name='Code_Objects', index=False)
        orac_df_trigger.to_excel(writer3, sheet_name='Triggers', index=False)
    print("Excel File Created")


def utility_script(single_connection, cschema_type, ceecount):
    total_inclusive_dict = {'.db_link': 10.0, 'all_arguments': 10.0, 'all_cons_columns': 1.0, 'all_constraints': 1.0,
                            'all_tab_columns': 1.0, 'all_tables': 1.0, 'alter': 10.0, 'cast_to_raw': 1.0,
                            'cast_to_varchar2': 1.0, 'checksum': 1.0, 'chr': 1.0, 'coalesce': 1.0, 'connect by': 10.0,
                            'constant': 2.0, 'dba_objects': 10.0, 'dbms_lob': 5.0, 'dbms_mview': 10.0,
                            'dbms_obfuscation_toolkit': 20.0, 'dbms_sql': 3.0, 'dbms_standard': 2.0,
                            'dbms_utility': 1.0, 'decode': 15.0, 'desencrypt': 1.0, 'encrypted_data': 1.0,
                            'execute immediate': 15.0, 'extract': 5.0, 'extractvalue': 5.0, 'fopen': 1.0, 'forall': 1.0,
                            'format_error_backtrace': 1.0, 'from table(': 5.0, 'getnumberval': 1.5, 'getstringval': 1.5,
                            'initcap': 1.0, 'instr': 5.0, 'is record': 5.0, 'is table of': 15.0, 'level': 1.0,
                            'merge': 10.0, 'mod': 1.0, 'out sys_refcursor': 5.0, 'partition': 1.0, 'pipe': 20.0,
                            'pragma autonomous_transaction': 3.0, 'rank': 1.0, 'rawtohex': 1.0, 'record': 1.0,
                            'refersh': 1.0, 'regexp': 6.0, 'regexp_count': 1.0, 'regexp_replace': 1.0,
                            'regexp_like': 1.0, 'regexp_substr': 1.0, 'regexp_instr': 3.0, 'round(avg(to_number': 5.0,
                            'rownum': 1.0, 'save_exception': 1.0, 'savepoint': 20.0, 'set_transform_param': 5.0,
                            'soundex': 1.0, 'standard_hash': 1.0, 'start': 1.0, 'sys_context': 2.0,
                            'sys_refcursor': 1.0, 'to_date(to_char(': 3.0, 'user_mviews': 20.0, 'user_tab_cols': 5.0,
                            'user_tab_columns': 10.0, 'utl_mail.send': 15.0, 'utl_raw': 1.0, 'xmlsequence': 5.0,
                            'xmltype': 5.0, 'to_char': 3.0, '@': 1.0, 'listagg': 1.0, 'row_number': 1.0, 'type': 1.0,
                            'bulk collect': 1.0, 'pivot': 1.0, 'unpivot': 1.0}
    global screen
    import time
    progressbar = ttk.Progressbar(screen2, length=250, mode='determinate', orient=HORIZONTAL)
    progressbar.pack(pady=10)
    progressbar.place(x=350, y=410)
    progressbar['value'] = 0
    screen.update_idletasks()
    time.sleep(1)

    cschema_type = str(cschema_type).upper()
    if os.path.isdir('Source_' + cschema_type):
        shutil.rmtree('Source_' + cschema_type)
    # if os.path.isdir('MetaData'):
    #     shutil.rmtree('MetaData')

    # folder structure for  Source DB, if not present craeting one as per the pattern

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'INDEXES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'INDEXES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'INDEXES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'INDEXES')
    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'CHECKCONSTRAINS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'CHECKCONSTRAINS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'CHECKCONSTRAINS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'CHECKCONSTRAINS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PG_MATERIALIZED_VIEWS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SEQUENCES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SEQUENCES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SEQUENCES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SEQUENCES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SYNONYMS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SYNONYMS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SYNONYMS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'SYNONYMS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TABLES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TABLES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TABLES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TABLES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'VIEWS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'VIEWS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'VIEWS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'VIEWS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'DATATYPES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'DATATYPES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'DATATYPES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'DATATYPES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'FUNCTIONS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'FUNCTIONS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'FUNCTIONS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'FUNCTIONS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PACKAGES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PACKAGES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PACKAGES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PACKAGES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PROCEDURES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PROCEDURES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PROCEDURES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'PROCEDURES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TYPES'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TYPES')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TYPES'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TYPES')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TRIGGERS'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TRIGGERS')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TRIGGERS'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'TRIGGERS')

    if os.path.isdir('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'MetaData'):
        shutil.rmtree('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'MetaData')
    if not os.path.exists('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'MetaData'):
        os.makedirs('Source_' + cschema_type + '/' + str(cschema_type).upper() + '/' + 'MetaData')

    progressbar['value'] = 10
    screen.update_idletasks()
    time.sleep(1)

    sub_list_job = []

    statements_list = []
    all_codeobjects_list = []

    simple_so_count, complex_so_count = storage_utility(cschema_type, statements_list, single_connection, ceecount)
    progressbar['value'] = 40
    screen.update_idletasks()
    time.sleep(1)

    simple_co_count, complex_co_count, all_codeobjects_list = codeobjects_utility(cschema_type, statements_list,
                                                                                  single_connection, ceecount,
                                                                                  all_codeobjects_list,
                                                                                  total_inclusive_dict)
    progressbar['value'] = 80
    screen.update_idletasks()
    time.sleep(1)

    procedure_estimation_hours = []
    function_estimation_hrs = []
    package_estimation_hrs = []
    trigger_estimation_hrs = []

    for codtype in all_codeobjects_list:
        if codtype[1] == 'Procedure':
            procedure_estimation_hours.append(codtype[7])
    total_code_proc = sum(procedure_estimation_hours) / 60

    for codfun in all_codeobjects_list:
        if codfun[1] == 'Function':
            function_estimation_hrs.append(codfun[7])
    total_code_func = sum(function_estimation_hrs) / 60

    for codpack in all_codeobjects_list:
        if codpack[1] == 'Package':
            package_estimation_hrs.append(codpack[7])
    total_code_pack = sum(package_estimation_hrs) / 60

    for codtrig in all_codeobjects_list:
        if codtrig[1] == 'Triggers':
            trigger_estimation_hrs.append(codtrig[7])
    total_code_trigg = sum(trigger_estimation_hrs) / 60

    print('all objects done')
    DATA_COUNT = []
    for i in statements_list:
        if ('Packages' in i) or ('Procedures' in i) or ('Functions' in i) or ('Triggers' in i):
            if str(ceecount).upper() == 'O':
                DATA_COUNT.append(i[1])
            else:
                DATA_COUNT.append(i[1])

    total_statements_list = []

    if DATA_COUNT:
        Testing_Object_type = 'Testing'
        Testing_Count = sum(DATA_COUNT)
        Testing_Estimation_Factor = 200
        Testing_estimate_min = Testing_Estimation_Factor * Testing_Count
        try:
            Testing_Estimation_Hours = round((Testing_Estimation_Factor * Testing_Count) / 60)
            total_testing_factor = int(Testing_Estimation_Hours) / Testing_Count
        except:
            total_testing_factor = 0
        for i in statements_list:
            sub_objects_count = []
            if ('Packages' in i) or ('Procedures' in i) or ('Functions' in i) or ('Triggers' in i):
                obj_count = i[1] * total_testing_factor
                obj_count = math.ceil(obj_count)
                obj_hrs = round(float(i[2] + obj_count), 2)
                if 'Procedures' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_proc])
                elif 'Functions' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_func])
                elif 'Packages' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_pack])
                elif 'Triggers' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_trigg])
                total_statements_list.append(sub_objects_count)
            else:
                sub_objects_count.extend([i[0], i[1], i[2]])
                total_statements_list.append(sub_objects_count)
    else:
        for i in statements_list:
            sub_objects_count = []
            sub_objects_count.extend([i[0], i[1], i[2]])
            total_statements_list.append(sub_objects_count)
    progressbar['value'] = 90
    screen.update_idletasks()
    time.sleep(1)

    print('completed Data count')
    all_count_df = pd.DataFrame()
    if str(ceecount).upper() == 'D':
        all_count_df = pd.DataFrame(total_statements_list,
                                    columns=["Object Type", "Detail Count",
                                             "Estimate(in Hours)"])
    elif str(ceecount).upper() == 'O':
        all_count_df = pd.DataFrame(total_statements_list,
                                    columns=["Object Type", "Object Count",
                                             "Estimate(in Hours)"])

    est_total_counts = all_count_df["Estimate(in Hours)"].values.tolist()
    print('total esti count')
    total_count = 0
    for i in est_total_counts:
        total_count = i + total_count
    total_count = round(float(total_count), 2)
    new_row1 = ["Projected Hours", '', total_count]

    all_count_df.loc[len(all_count_df.index)] = new_row1

    if os.path.isdir('Source_' + cschema_type):
        shutil.rmtree('Source_' + cschema_type)
    if os.path.exists('Source_' + cschema_type):
        os.rmdir('Source_' + cschema_type)

    co_obj = ['Packages', 'Procedures', 'Functions', 'Triggers', 'Jobs', 'Schedule', 'Program']
    so_obj = ['Tables', 'Partitions', 'Indexes', 'Sequences', 'Synonyms', 'Views', 'Mviews', 'Types', 'Dblink']
    so_count = 0
    co_count = 0
    projected_h_count = 0
    for index, row in all_count_df.iterrows():
        print(index, row)
        if (row[0] in co_obj):
            co_count = co_count + int(row[1])
        if (row[0] in so_obj):
            so_count = so_count + int(row[1])
        if 'Projected Hours' in row[0]:
            projected_h_count = projected_h_count + float(row[2])
    total_co_so = so_count + co_count

    complex_storage_Objects_final = complex_so_count
    simple_storage_Objects_final = so_count - complex_storage_Objects_final
    total_so_count = complex_storage_Objects_final + simple_storage_Objects_final

    complex_code_Objects_final = complex_co_count
    simple_code_Objects_final = co_count - complex_code_Objects_final
    total_co_count = complex_code_Objects_final + simple_code_Objects_final

    if total_so_count != 0:
        complex_so_percentage = math.ceil((complex_storage_Objects_final / total_so_count) * 100)
        simple_so_percentage = 100 - complex_so_percentage
    else:
        complex_so_percentage = 0
        simple_so_percentage = 0

    if total_co_count != 0:
        complex_co_percentage = math.ceil((complex_code_Objects_final / total_co_count) * 100)
        simple_co_percentage = 100 - complex_co_percentage
    else:
        complex_co_percentage = 0
        simple_co_percentage = 0

    qmigrator_hours = math.ceil(projected_h_count)
    total_so_count = so_count
    total_co_count = co_count
    manual_mig_hours = math.ceil(((qmigrator_hours / 70) * 100) + ((qmigrator_hours / 70)) * 30)
    if manual_mig_hours != 0:
        percentage_saving = math.ceil(((manual_mig_hours - qmigrator_hours) / manual_mig_hours) * 100)
    else:
        percentage_saving = 0

    progressbar['value'] = 100
    screen.update_idletasks()
    time.sleep(1)

    html_report(total_co_so, qmigrator_hours, manual_mig_hours, total_co_count, total_so_count, percentage_saving,
                cschema_type, simple_so_percentage, complex_so_percentage, simple_co_percentage, complex_co_percentage,
                html_string)
    return 'Done'


def mainscriptrunning(single_connection,res_list):

    title1 = Label(screen2, text="BackGround Running Please Do Not Close", bg="#60A3D9", fg="blue", font=('Aerial', 12,'bold'))
    title1.place(x=235, y=90)
    try:
        my_frame = Frame(screen2)
        my_scrollbar = Scrollbar(my_frame, orient=VERTICAL)
        list1 = Listbox(my_frame, selectmode="multiple", height=1, width=15, yscrollcommand=my_scrollbar.set,bg = '#D3D3D3')
        # my_scrollbar.config(command=list1.yview)
        # my_scrollbar.pack(side=RIGHT, fill=Y)
        list1.pack(ipady=60)
        #list1.place(x=400,y=500)
        #my_frame.pack(pady=70)
        my_frame.place(x=350,y=510)
        if res_list:
            flag = 0
            for i in res_list:
                print(i)
                try:
                    global progressbar
                    global filelocation
                    global completedschemalist
                    progressbar = StringVar()
                    filelocation = StringVar()
                    completedschemalist = StringVar()
                    progressbar = Label(screen2, text="ProgressBar", fg = "black", font = ('Aerial', 10, 'bold'), bg = "#60A3D9")
                    progressbar.place(x=150, y=410)
                    filelocation = Label(screen2, text="Assessment file location",fg = "black", font = ('Aerial', 10, 'bold'), bg = "#60A3D9")
                    filelocation.place(x=150, y=460)
                    completedschemalist = Label(screen2, text="Completed Schema List",fg = "black", font = ('Aerial', 10, 'bold'), bg = "#60A3D9")
                    completedschemalist.place(x=150, y=510)
                    ret = utility_script(single_connection, i, val)
                    curr_dir = os.getcwd()
                    title1 = Label(screen2, text=" " + curr_dir,fg = "black", font = ('Aerial', 10, 'bold'), bg = "#60A3D9")
                    title1.place(x=350, y=460)
                    list1.insert(END, i)
                    list1.itemconfig("end", bg="green")
                except:
                    title1 = Label(screen2, text="Something went wrong  " + i, fg="red", bg="#60A3D9",
                                   font = ('Aerial', 10, 'bold'))
                    title1.place(x=350, y=460)
                    list1.insert(END, i)
                    list1.itemconfig("end", bg="red")

            messagebox.showinfo('Success', 'All Schemas Completed')
            restart(screen)
        else:
            messagebox.showerror('Error', 'Please select select atleast one schema')

    except cx_Oracle.DatabaseError as x:
        Label(screen, text="Invalid Credentials", bg="#60A3D9", fg="red", font=('Aerial', 12,'bold')).pack()
        messagebox.showerror('Error', 'Invalid Credentials Please Try Again')
        print("error", x)
        restart(screen)

    except Exception as cx:
        Label(screen, text="Something Went Wrong", bg="#60A3D9", fg="red", font=('Aerial', 12,'bold')).pack()
        messagebox.showerror('Error', 'Something Went Wrong Please Try Again')
        print("error", cx)
        restart(screen)


def validate(var):
    choose = var
    global val
    if choose == 1:
        val = 'O'
    elif choose == 2:
        val = 'D'
    else:
        val = ''
    global screen
    x = t_username.get()
    y = t_password.get()
    z = t_hostname.get()
    a = t_port.get()
    b = t_sid.get()

    try:
        if x == '' or y == '' or z == '' or a == '' or b == '':
            messagebox.showerror('Error', 'Please Enter Database Details')
        elif val == '':
            messagebox.showerror('Error', 'Please select anyone')
        else:
            Label(screen, text="BackGround Running Please Do Not Close", bg="#60A3D9", fg="blue",
                  font=('Aerial', 12,'bold')).pack()
            main_screen_2(x, y, z, a, b)

    except cx_Oracle.DatabaseError as x:
        Label(screen, text="Invalid Credentials", bg="#60A3D9", fg="red", font=('Aerial', 12,'bold')).pack()
        messagebox.showerror('Error', 'Invalid Credentials Please Try Again')
        print("error", x)
        restart(screen)

    except Exception as cx:
        Label(screen, text="Something Went Wrong", bg="#60A3D9", fg="red", font=('Aerial', 12,'bold')).pack()
        messagebox.showerror('Error', 'Something Went Wrong Please Try Again')
        print("error", cx)
        restart(screen)


def restart(root):
    root.destroy()
    main_screen()


def main_screen():
    global screen
    screen = Tk()
    screen.title("QMigrator")
    screen.geometry("800x700")

    screen.configure(bg='#60A3D9')
    my_heading = Label(screen, text="QMIGRATOR", font=('Aerial', 35, 'bold'), fg="black", bg="#60A3D9")
    my_heading.pack(pady=20)


    global username
    global password
    global hostname
    global sid
    global port
    global Databasename
    global t_username
    global t_password
    global t_hostname
    global t_port
    global t_sid
    global t_Databasename
    username = StringVar()
    password = StringVar()
    hostname = StringVar()
    port = StringVar()
    sid = StringVar()
    username = Label(screen, text="User Name *", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9' )
    username.place(x=150, y=150)
    password = Label(screen, text="Password *", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9')
    password.place(x=150, y=230)
    hostname = Label(screen, text="Host Name *", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9')
    hostname.place(x=150, y=310)
    port = Label(screen, text="Port *", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9')
    port.place(x=150, y=390)
    sid = Label(screen, text="Service Name *", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9')
    sid.place(x=150, y=470)

    t_username = Entry(highlightthickness=2, highlightcolor="#89CFF0", highlightbackground="#BFD7ED",
                       fg="black", font=('Aerial', 12,'bold'), relief="flat")
    t_username.place(height=30, width=550, x=150, y=190)

    t_password = Entry(show="*", highlightthickness=2, highlightcolor="#89CFF0", highlightbackground="#BFD7ED",
                       fg="black", font=('Aerial', 12,'bold'), relief="flat")
    t_password.place(height=30, width=550, x=150, y=270)

    t_hostname = Entry(highlightthickness=2, highlightcolor="#89CFF0", highlightbackground="#BFD7ED",
                       fg="black", font=('Aerial', 12,'bold'), relief="flat")
    t_hostname.place(height=30, width=550, x=150, y=350)

    t_port = Entry(highlightthickness=2, highlightcolor="#89CFF0", highlightbackground="#BFD7ED",
                   fg="black", font=('Aerial', 12,'bold'), relief="flat")
    t_port.place(height=30, width=550, x=150, y=430)

    t_sid = Entry(highlightthickness=2, highlightcolor="#89CFF0", highlightbackground="#BFD7ED",
                  fg="black", font=('Aerial', 12,'bold'), relief="flat")
    t_sid.place(height=30, width=550, x=150, y=510)

    Button(screen, text="Connect", bg="white", fg="black",command=partial(validate, 2), font=('Aerial', 15, 'bold'),
           relief="groove").place(x=350, y=570)

    screen.mainloop()


def main_screen_2(username, password, hostname, port, sid):
    vars = []
    checkbuttons = []
    single_connection, x = all_users(username, password, hostname, port, sid)

    def showSelected():
        print("entering")
        res_list = []
        for c, var in zip(checkbuttons, vars):
            text = c.cget("text")
            value = var.get()
            print("%s: %d" % (text, value))
            if value == 1:
                res_list.append(text)
        print(res_list)
        mainscriptrunning(single_connection, res_list)

    global screen2
    screen2 = Toplevel()
    screen2.title("QMigrator")
    screen2.geometry("800x700")
    screen2.configure(bg='#60A3D9')
    my_heading = Label(screen2, text="QMIGRATOR",font=('Aerial', 35, 'bold'), fg="black", bg="#60A3D9")
    my_heading.pack(pady=20)
    global schemalist
    schemalist = StringVar()
    schema_select = Label(screen2, text="Select Schemas from below list", fg="black", font=('Aerial', 15, 'bold'),bg='#60A3D9')
    schema_select.pack(pady=25)
    checkbox_frame = Frame(screen2)
    myscrollbar = Scrollbar(checkbox_frame, orient=VERTICAL)
    myscrollbar.pack(side=RIGHT, fill=Y)
    checklist = Text(checkbox_frame, width=28, height=10,bg = '#D3D3D3')
    checklist.pack()

    for j in x:
        var = IntVar()
        var.set(0)
        vars.append(var)
        c = Checkbutton(checklist, text=j,font = ('Aerial', 9,'bold'),variable=var, onvalue=1, offvalue=0,fg ='black',bg='#D3D3D3')
        checklist.window_create("end", window=c)
        checklist.insert("end", "\n")
        checkbuttons.append(c)
    checklist.config(yscrollcommand=myscrollbar.set)
    myscrollbar.config(command=checklist.yview)
    checkbox_frame.pack(pady=1)
    checklist.configure(state="disabled")
    Button(screen2, text="Submit", bg="white", fg="black",command=showSelected, font=('Aerial', 15, 'bold'),
           relief="groove").place(x=350, y=350)

    screen2.mainloop()


def check_program_expired():
    app_date = datetime.datetime(year=2022, month=2,day=15)
    # app_date = datetime.datetime(year=2022, month=2, day=15)
    now = datetime.datetime.now()
    return app_date, now


appdate, currentdate = check_program_expired()

if ((appdate - currentdate).days > 30) or ((appdate - currentdate).days < 0):
    screen = Tk()
    screen.title("QMigrator")
    screen.geometry("800x700")
    screen.configure(bg='#60a3d9')
    my_heading = Label(screen, text="QMIGRATOR", font=('Aerial', 35, 'bold'), fg="black", bg="#60A3D9")
    my_heading.pack(pady=20)
    messagebox.showinfo('info', 'Extraction Tool Has Expired')


    sys.exit()
else:
    main_screen()

