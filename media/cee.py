import cx_Oracle
import pandas as pd
import shutil
import os, re  # zipfile
import argparse
from qmig_config import *
from authentication import decryptstr, decrypt_source_details, decrypt_application_details, logging_db
import logging
import json, math
from collections import Counter

# path for config folder
path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

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

dir = os.listdir(path)
dir = sorted(dir)
find_db = dir.index('DB')
config_path = None
db_transit_path = None
# path for temp folder in DB
transit_path = path
transit_dirs = os.listdir(transit_path)
transit_dirs = sorted(transit_dirs)

# we are retrieveing up to V4+DB+Source in folder_path
if transit_dirs[find_db].lower() == str('DB').lower():
    db_dirs = os.listdir(transit_path + '/' + transit_dirs[find_db])
    if 'Pre_O2P' not in db_dirs:
        db_dirs.append("Pre_O2P")
    if 'Source' not in db_dirs:
        db_dirs.append("Source")
    db_dirs = sorted(db_dirs)
    find_temp = db_dirs.index('Pre_O2P')
    find_source = db_dirs.index('Source')
    folder_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_source]
    if db_dirs[find_temp].lower() == str('Pre_O2P').lower():
        db_transit_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_temp]
    else:
        db_transit_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_temp]


# # # Commented By Srujan 12/24
# def oracle_connection(query, username, password, hostname, port, sid, cschema_type):
#     dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
#     connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
#     print("connected to the Oracle Database")
#     df_ora = pd.read_sql(query, con=connection)
#     # df_ora = df_ora.apply(lambda x: x.astype(str).str.upper())
#     print("length of oracle df", len(df_ora))
#     return df_ora
# Added By Srujan 12/24
def single_oracle_connection(username, password, hostname, port, sid):
    dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
    connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    print("Connected to the Oracle Database")
    return connection


def query_oracle_connection(query, connection):
    print("Executing the Query in the Oracle Database")
    df_ora = pd.read_sql(query, con=connection)
    print("length of oracle df", len(df_ora))
    return df_ora


def create_and_append_configfile(config_path, data):
    with open(config_path, 'w') as f:
        f.write("{}\n".format(data))


def create_and_append_configfile_otherexclusion(config_path, data):
    with open(config_path, 'a') as f:
        f.write("{}\n".format(data))


## oracle connection for the object validation tool
def oracle_conn_data_type(oracle_data, query):
    connection = source_connection_details(oracle_data)
    print("connected to the Oracle Database")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception as err:
        print(err)
    finally:
        cursor.close()
    return data


def function_pre_lower(data):
    # function to Convert the whole string to lower case except the data which is in single quotes --SK 7/12
    # Example:: """ the Data had the ' The data Main forum ' values Main to Copy """ ==> """ the data had the ' The data Main forum ' values main to copy """

    data = re.sub(r"\b(?<!')(\w+)(?!')\b", lambda match: match.group().lower(), data)

    return data


def appli_db_connection_call(tokendata, projectid, connection_type, connection_name):
    app_data = decrypt_application_details(tokendata, projectid, connection_type, connection_name)
    return app_data


# calling stored procedure for summery data count updation
def appli_db_get_iteration(xmlpath, projectid, connection, schmename):
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        cursor.execute('select public.fn_prj_current_iteration1(%s,%s,%s);', (projectid, 'Assessment', schmename))
        data = cursor.fetchall()
        print("iteration selecetd")
    except Exception as err:
        # print("error", err)
        data = None
        pass
    finally:
        connection.commit()
        cursor.close()
    return data


def appli_db_powerbireport(xmlpath, projectid, connection_name, iter, connection, cschema_type, pb_objecttype, pb_count,
                           est, status, statusmsg):
    cursor = connection.cursor()
    try:
        print(projectid, str(connection_name), int(iter), str(cschema_type), str(pb_objecttype), int(pb_count), est,
              str(status), str(statusmsg), None, 'Python', 'dataset')
        cursor.execute(
            'CALL public.sp_prj_Assessments_progress_insert(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
            (
                projectid, str(connection_name), int(iter), str(cschema_type), str(pb_objecttype), int(pb_count), est,
                str(status), str(statusmsg), None, 'Python', 'dataset'))
    except Exception as err:
        print("error", err)
    finally:
        connection.commit()
        cursor.close()


def appli_db_powerbireport_sheet2(projectid, iter, connection, p_code_objectname, p_type, p_lines, p_statements,
                                  p_identifiers,
                                  p_total_indentifiers, p_varyng_factor, p_totalmins):
    cursor = connection.cursor()
    try:
        cursor.execute(
            'call public.prj_assessdetail_object_cnt_insert(%s,%s,%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
            (projectid, int(iter), p_code_objectname, p_type, int(p_lines), int(p_statements), p_identifiers,
             int(p_total_indentifiers), int(p_varyng_factor), int(p_totalmins), None, 'Python', 'dataset'))
    except Exception as err:
        print("error", err)
    finally:
        connection.commit()
        cursor.close()


# Commented -- Sk 12/24
# ## oracle connection for the object validation tool
# def get_count(oracle_data, query):
#     connection = source_connection_details(oracle_data)
#     print("connected to the Oracle Database")
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         data = cursor.fetchall()
#     except Exception as err:
#         print(err)
#         data = None
#     finally:
#         cursor.close()
#     return data
# Added -- SK 12/24
def get_count(connection, query):
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
    single_line_text = re.sub(r'extract\s+\(', 'extract(', single_line_text)
    single_line_text = re.sub(r'extractvalue\s+\(', 'extractvalue(', single_line_text)
    single_line_text = re.sub(r'\-\-+', '--', single_line_text)
    extractpartsdictformat = {}
    listformat = commonlogicextract(single_line_text, 'extract')
    listformat1 = commonlogicextract(single_line_text, 'extractvalue')
    if len(listformat):
        arya = 0
        for arc in listformat:
            single_line_text = single_line_text.replace(arc, 'arya' + str(arya) + 'arya', 1)
            extractpartsdictformat['arya' + str(arya) + 'arya'] = arc
            arya = arya + 1
    if len(listformat1):
        arya = 0
        for arc1 in listformat1:
            single_line_text = single_line_text.replace(arc1, 'aryan' + str(arya) + 'aryan', 1)
            extractpartsdictformat['aryan' + str(arya) + 'aryan'] = arc1

            arya = arya + 1
    singlequoye = re.findall(r"\'.*?\'", single_line_text)
    for i in singlequoye:
        cond = bool(re.search(r'\-\-+', i))
        if cond:
            if '--' in i or '/*' in i or '*/' in i:
                # main_split = i.split('--')
                # part1 = main_split[0]
                # part2 = main_split[1]
                # '-- Table ' | | v_table_name | | ' not found';
                # combine_str = part1 + 'singquote' + part2
                combine_str = i.replace('--', 'singquote').replace('/*', 'slash_start_pattern').replace('*/',
                                                                                                        'start_slash_pattern')
                single_line_text = single_line_text.replace(i, combine_str)
    dir_all = re.findall(r'\/\*[\s\S]*?\*/', single_line_text)
    for j in dir_all:
        single_line_text = single_line_text.replace(j, '\n', 1)
    dirall = re.findall(r"\-\-.*?\n", single_line_text)
    for dashcomments in dirall:
        single_line_text = single_line_text.replace(dashcomments, "\n", 1)
    single_line_text = re.sub("singquote", "--", single_line_text)
    single_line_text = re.sub("slash_start_pattern", "/*", single_line_text)
    single_line_text = re.sub("start_slash_pattern", "*/", single_line_text)
    for barc in extractpartsdictformat:
        single_line_text = single_line_text.replace(barc, extractpartsdictformat[barc], 1)
    # print(single_line_text)
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


# calling get connection details for source database from api
def calling_api(tokendata, projectid, connection_type, connection_name):
    oracle_data_source = decrypt_source_details(tokendata, projectid, connection_type, connection_name)
    # print(oracle_data_source)
    return oracle_data_source['name'], oracle_data_source['password'], oracle_data_source['port'], oracle_data_source[
        'host'], oracle_data_source['sid']


# Commented Based on the Single connection for The Entire Module -- Sk 12/24
# def exe_oracle_connection(query, username, password, hostname, port, sid, cschema_type):
#     dsn = cx_Oracle.makedsn(host=hostname, port=port, service_name=sid)
#     connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
#     cursor = connection.cursor()
#     cursor.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false); end;")
#     cursor.execute(
#         "begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false); end;")
#     # cursor.callproc('DBMS_METADATA.SET_TRANSFORM_PARAM',
#     #                 [-1, 'TABLESPACE', False])
#     # cursor.callproc('DBMS_METADATA.SET_TRANSFORM_PARAM',
#     #                 [-1, 'SEGMENT_ATTRIBUTES', False])
#     cursor.execute(query)
#     names = [x[0] for x in cursor.description]
#     rows = cursor.fetchall()
#     df_ora = pd.DataFrame(rows, columns=names)
#     return df_ora
# Added By -- Sk 12/24
def exe_oracle_connection(query, singeconnection):
    cursor = singeconnection.cursor()
    cursor.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false); end;")
    cursor.execute(
        "begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false); end;")
    cursor.execute(query)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df_ora = pd.DataFrame(rows, columns=names)
    return df_ora


def create_and_append_sqlfile(path_of_file_sql, data):
    with open(path_of_file_sql, 'a') as f:
        f.write("{}\n\n\n\n".format(data))


# path for config folder
path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

check_constarinsdefination = """
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

check_constarinsdefination1 = """
            select 'ALTER TABLE'||' '|| child_tab_owner || '.' || TABLE_NAME ||' '|| 'ADD constraint '||r_constraint_name||' '|| Foreign_Key ||'; ' as Foreign_Key_query
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

"""


def metadata_excel(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path, meta_dir,
                   object_type):
    # metascript started
    # call xml extractor to get db connection and queries from the xml file
    oracle_data, orc_query_tag = xml_extractor(xmlpath, None, parent_tag='OracleSchemaTag')
    orc_query_tag['Tables'] = orc_query_tag['Tables'].replace('@schemaname', cschema_type)
    orc_query_tag['DataTypeMapping'] = orc_query_tag['DataTypeMapping'].replace('@schemaname', cschema_type)
    orc_query_tag['Views'] = orc_query_tag['Views'].replace('@schemaname', cschema_type)
    orc_query_tag['Sequences'] = orc_query_tag['Sequences'].replace('@schemaname', cschema_type)
    orc_query_tag['Materialized_views'] = orc_query_tag['Materialized_views'].replace('@schemaname', cschema_type)
    orc_query_tag['Primary_Keys'] = orc_query_tag['Primary_Keys'].replace('@schemaname', cschema_type)
    orc_query_tag['Indexes'] = orc_query_tag['Indexes'].replace('@schemaname', cschema_type)
    orc_query_tag['Check_Constraints'] = orc_query_tag['Check_Constraints'].replace('@schemaname', cschema_type)
    orc_query_tag['Foreign_Keys'] = orc_query_tag['Foreign_Keys'].replace('@schemaname', cschema_type)
    orc_query_tag['Code_Objects'] = orc_query_tag['Code_Objects'].replace('@schemaname', cschema_type)
    orc_query_tag['User_Defined_Types'] = orc_query_tag['User_Defined_Types'].replace('@schemaname', cschema_type)
    orc_query_tag['Triggers'] = orc_query_tag['Triggers'].replace('@schemaname', cschema_type)
    orc_query_tag['Synonym'] = orc_query_tag['Synonym'].replace('@schemaname', cschema_type)
    orc_query_tag['Global_Temp_Tables'] = orc_query_tag['Global_Temp_Tables'].replace('@schemaname', cschema_type)

    Meta_list_tables = orc_query_tag['Tables']
    Meta_list_datatypemapping = orc_query_tag['DataTypeMapping']
    Meta_list_views = orc_query_tag['Views']
    Meta_list_Sequences = orc_query_tag['Sequences']
    Meta_list_mviews = orc_query_tag['Materialized_views']
    Meta_list_primary_keys = orc_query_tag['Primary_Keys']
    Meta_list_Indexes = orc_query_tag['Indexes']
    Meta_list_check_constrains = orc_query_tag['Check_Constraints']
    Meta_list_foreign_keys = orc_query_tag['Foreign_Keys']
    Meta_list_Code_objects = orc_query_tag['Code_Objects']
    Meta_list_userdefined_types = orc_query_tag['User_Defined_Types']
    Meta_list_triggeres = orc_query_tag['Triggers']
    Meta_list_synonyms = orc_query_tag['Synonym']
    Meta_list_GTT = orc_query_tag['Global_Temp_Tables']

    query_oracle_table = str(Meta_list_tables).replace('@schemaname', cschema_type)
    query_oracle_datatypemapping = str(Meta_list_datatypemapping).replace('@schemaname', cschema_type)
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
    query_oracle_synonyms = str(Meta_list_synonyms).replace('@schemaname', cschema_type)
    query_oracle_gtt = str(Meta_list_GTT).replace('@schemaname', cschema_type)

    MetaData_excelpath_all = meta_dir + '/' + str(cschema_type) + '_ALL_validation.xlsx'
    MetaData_excelpath_so = meta_dir + '/' + str(cschema_type) + '_SO_validation.xlsx'
    MetaData_excelpath_co = meta_dir + '/' + str(cschema_type) + '_CO_validation.xlsx'
    print("meta excel sheet exe started")

    with pd.ExcelWriter(MetaData_excelpath_all) as writer3:
        oracle_data_check, orc_check_query_tag = xml_extractor(xmlpath, None,
                                                               parent_tag='OraclecheckconstrainsExtractorvalidation')
        list_query_oracle_check = orc_check_query_tag['check_constarintcountcheck'].replace('@schemaname',
                                                                                            cschema_type)
        print("check constraint count check")

        orac_df_checkconstcount = query_oracle_connection(list_query_oracle_check, oracle_singleconnection)

        check_count = orac_df_checkconstcount['COUNT'][0]

        orac_df_table = query_oracle_connection(query_oracle_table, oracle_singleconnection)

        orac_df_datatypemapp = query_oracle_connection(query_oracle_datatypemapping, oracle_singleconnection)

        orac_df_view = query_oracle_connection(query_oracle_view, oracle_singleconnection)
        orac_df_sequence = query_oracle_connection(query_oracle_sequence, oracle_singleconnection)
        orac_df_mview = query_oracle_connection(query_oracle_mview, oracle_singleconnection)
        orac_df_primary_key = query_oracle_connection(query_oracle_primary_key, oracle_singleconnection)
        orac_df_index = query_oracle_connection(query_oracle_index, oracle_singleconnection)
        if int(check_count) != 0:
            orac_df_check_constrain = query_oracle_connection(query_oracle_check_constrain, oracle_singleconnection)
        else:
            orac_df_check_constrain = pd.DataFrame(columns=['TABLE_NAME', 'CHECK_CONDITION'])
        orac_df_foreign_key = query_oracle_connection(query_oracle_foreign_key, oracle_singleconnection)
        orac_df_code_object = query_oracle_connection(query_oracle_code_object, oracle_singleconnection)
        orac_df_userdefined_type = query_oracle_connection(query_oracle_userdefined_type, oracle_singleconnection)
        orac_df_trigger = query_oracle_connection(query_oracle_trigger, oracle_singleconnection)
        orac_df_synonyms = query_oracle_connection(query_oracle_synonyms, oracle_singleconnection)
        print('GTT')
        orac_df_gtt = query_oracle_connection(query_oracle_gtt, oracle_singleconnection)

        orac_df_table.to_excel(writer3, sheet_name='Tables', index=False)
        orac_df_datatypemapp.to_excel(writer3, sheet_name='DataTypeMapping', index=False)
        orac_df_view.to_excel(writer3, sheet_name='Views', index=False)
        orac_df_sequence.to_excel(writer3, sheet_name='Sequences', index=False)
        orac_df_primary_key.to_excel(writer3, sheet_name='Primary_Keys', index=False)
        orac_df_index.to_excel(writer3, sheet_name='Indexes', index=False)
        orac_df_check_constrain.to_excel(writer3, sheet_name='Check_Constraints', index=False)
        orac_df_foreign_key.to_excel(writer3, sheet_name='Foreign_Keys', index=False)
        orac_df_code_object.to_excel(writer3, sheet_name='Code_Objects', index=False)
        orac_df_userdefined_type.to_excel(writer3, sheet_name='User_Defined_Types', index=False)
        orac_df_trigger.to_excel(writer3, sheet_name='Triggers', index=False)
        orac_df_mview.to_excel(writer3, sheet_name='Materialized_views', index=False)
        orac_df_synonyms.to_excel(writer3, sheet_name='Synonym', index=False)
        orac_df_gtt.to_excel(writer3, sheet_name='Global_Temp_Tables', index=False)

    with pd.ExcelWriter(MetaData_excelpath_co) as writer1:
        print("Code objects exe")
        orac_df_code_object = query_oracle_connection(query_oracle_code_object, oracle_singleconnection)
        print("trigger exe")
        orac_df_trigger = query_oracle_connection(query_oracle_trigger, oracle_singleconnection)
        orac_df_code_object.to_excel(writer1, sheet_name='Code_Objects', index=False)
        orac_df_trigger.to_excel(writer1, sheet_name='Triggers', index=False)

    with pd.ExcelWriter(MetaData_excelpath_so) as writer2:
        oracle_data_check, orc_check_query_tag = xml_extractor(xmlpath, None,
                                                               parent_tag='OraclecheckconstrainsExtractorvalidation')
        list_query_oracle_check = orc_check_query_tag['check_constarintcountcheck'].replace('@schemaname',
                                                                                            cschema_type)
        print("check constraint count check")
        orac_df_checkconstcount = query_oracle_connection(list_query_oracle_check, oracle_singleconnection)
        check_count = orac_df_checkconstcount['COUNT'][0]
        print('Tables')
        orac_df_table = query_oracle_connection(query_oracle_table, oracle_singleconnection)
        print('Data type mapping')
        orac_df_datatypemapp = query_oracle_connection(query_oracle_datatypemapping, oracle_singleconnection)
        print('views')
        orac_df_view = query_oracle_connection(query_oracle_view, oracle_singleconnection)
        print('Sequences')
        orac_df_sequence = query_oracle_connection(query_oracle_sequence, oracle_singleconnection)
        print("Mviews")
        orac_df_mview = query_oracle_connection(query_oracle_mview, oracle_singleconnection)
        print("Primary keys")
        orac_df_primary_key = query_oracle_connection(query_oracle_primary_key, oracle_singleconnection)
        print('Indeexes')
        orac_df_index = query_oracle_connection(query_oracle_index, oracle_singleconnection)
        print('check constraint')
        if int(check_count) != 0:
            orac_df_check_constrain = query_oracle_connection(query_oracle_check_constrain, oracle_singleconnection)
        else:
            orac_df_check_constrain = pd.DataFrame(columns=['TABLE_NAME', 'CHECK_CONDITION'])
        print('Foreign key')
        orac_df_foreign_key = query_oracle_connection(query_oracle_foreign_key, oracle_singleconnection)
        print("Userdefined types")
        orac_df_userdefined_type = query_oracle_connection(query_oracle_userdefined_type, oracle_singleconnection)
        print("Synonyms")
        orac_df_synonyms = query_oracle_connection(query_oracle_synonyms, oracle_singleconnection)
        print('GTT')
        orac_df_gtt = query_oracle_connection(query_oracle_gtt, oracle_singleconnection)

        orac_df_table.to_excel(writer2, sheet_name='Tables', index=False)
        orac_df_datatypemapp.to_excel(writer2, sheet_name='DataTypeMapping', index=False)
        orac_df_view.to_excel(writer2, sheet_name='Views', index=False)
        orac_df_sequence.to_excel(writer2, sheet_name='Sequences', index=False)
        orac_df_primary_key.to_excel(writer2, sheet_name='Primary_Keys', index=False)
        orac_df_index.to_excel(writer2, sheet_name='Indexes', index=False)
        orac_df_check_constrain.to_excel(writer2, sheet_name='Check_Constraints', index=False)
        orac_df_foreign_key.to_excel(writer2, sheet_name='Foreign_Keys', index=False)
        orac_df_userdefined_type.to_excel(writer2, sheet_name='User_Defined_Types', index=False)
        orac_df_mview.to_excel(writer2, sheet_name='Materialized_views', index=False)
        orac_df_synonyms.to_excel(writer2, sheet_name='Synonym', index=False)
        orac_df_gtt.to_excel(writer2, sheet_name='Global_Temp_Tables', index=False)
        # pass
    print("meta excel sheet exe ended")


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
#

# Replaced the brackets data with count(*********) with count(1)
def count_data(data):
    data = re.sub(r"\b(?<!')(\w+)(?!')\b", lambda match: match.group().lower(), data)
    data = re.sub(r' +', ' ', data)
    if ('is' in data) or ('as' in data) or ('declare' in data) and ('begin' in data):
        startIndex = [m.start() for m in re.finditer('is |as |declare ', data)]
        charindex_data = []
        for index in startIndex:
            current = []
            bracket_level = 0
            for s in data[index + len(data[index]):]:
                if s != data[index] and s != 'begin ' and bracket_level >= 0:
                    current.append(s)
                elif s == data[index]:
                    current.append(s)
                    bracket_level += 1
                elif s == 'begin ':
                    bracket_level -= 1
                    if bracket_level < 0:
                        current.append(s)
                        break
                    else:
                        current.append(s)
            charindex_data.append(data[index] + ''.join(current))
    else:
        charindex_data = data

    return charindex_data


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


# # new sheet1 extraction Quries
# def object_name_quries(cschema_type, hostname, port, sid, username, password, xmlpath):
#     oracle_objectsheet1_data, orc_objectsheet1_list_tag = xml_extractor(xmlpath, None,
#                                                                         parent_tag='OracleObjectsheet1Extractor')
#     type_objectsheet1_oracle = orc_objectsheet1_list_tag['OBJSHEET1LIST'].replace('@schemaname', cschema_type)
#     query_oracle = str(type_objectsheet1_oracle).replace('@schemaname', cschema_type)
#     orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
#     object_sheet1_lists = orac_df[orac_df.columns[0]].values.tolist()
#
#     oracle_columnsheet1_data, orc_columnsheet1_list_tag = xml_extractor(xmlpath, None,
#                                                                         parent_tag='Oraclecolumnsheet1Extractor')
#     type_columnsheet1_oracle = orc_columnsheet1_list_tag['COLUMNSHEET1LIST'].replace('@schemaname', cschema_type)
#     query_oracle = str(type_columnsheet1_oracle).replace('@schemaname', cschema_type)
#     orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
#     column_sheet1_lists = orac_df[orac_df.columns[0]].values.tolist()
#     for x in object_sheet1_lists:
#         column_sheet1_lists.append(x)
#     ignored_both_object_column = column_sheet1_lists
#     ignored_both_object_column_lower = (map(lambda x: x.lower(), ignored_both_object_column))
#     finals_ignored_list = list(ignored_both_object_column_lower)
#     return finals_ignored_list


def data_btw_brackets(read_text_single_str):
    read_text_single_str = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace('[',
                                                                                                                    ' [ ').replace(
        ']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%', ' % ').replace('=',
                                                                                          ' = ').replace(
        ':=', ' := ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+', ' + ').replace('/',
                                                                                                                ' / ').replace(
        '<', ' < ').replace('*', ' * ').replace('>>', ' >> ').replace('--', '').replace('---',
                                                                                        '').replace(
        '----', '').replace('-----', '').replace('"', ' " ').replace('>', ' > ').replace('  ', ' ').replace("'",
                                                                                                            " ' ").replace(
        "-", " - ")
    read_create_split = read_text_single_str.split('create or replace')
    main_braces_data = []
    for taking_data in read_create_split:
        if 'package "' in taking_data:
            remove_btw_brackets = re.findall(r'\(([^()]+)\)', taking_data)
            remove_btw_brackets_str = ' '.join(remove_btw_brackets)
            remove_btw_brackets_str_split = remove_btw_brackets_str.split()
            main_braces_data.append(remove_btw_brackets_str_split)
    lists_main = [x for t in main_braces_data for x in t]
    return lists_main


def unwanted_data_is_as_declare(read_text_single_str):
    read_text_single_str = re.sub(r' +', ' ', read_text_single_str)
    read_data = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace('[',
                                                                                                         ' [ ').replace(
        ']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%', ' % ').replace('=',
                                                                                          ' = ').replace(
        ':=', ' := ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+', ' + ').replace('/',
                                                                                                                ' / ').replace(
        '<', ' < ').replace('*', ' * ').replace('>>', ' >> ').replace('--', '').replace('---',
                                                                                        '').replace(
        '----', '').replace('-----', '').replace('"', ' " ').replace('>', ' > ').replace('  ', ' ').replace("'",
                                                                                                            " ' ").replace(
        "-", " - ")
    read_data_is_begin = re.findall(r'\sis\s.*?\sbegin\s', read_data, re.DOTALL)
    read_data_as_begin = re.findall(r'\bas\b.*?\bbegin\b', read_data, re.DOTALL)
    read_data_declare_begin = re.findall(r'\bdeclare\b.*?\bbegin\b', read_data, re.DOTALL)
    total_data = read_data_is_begin + read_data_as_begin + read_data_declare_begin
    main_is_as_begins = []
    for i_unwanted in total_data:
        i_unwanted_split = i_unwanted.split()
        for data_is_as in i_unwanted_split:
            main_is_as_begins.append(data_is_as)
    return main_is_as_begins


def unwanted_for_while(read_text_single_str):
    read_text_single_str = re.sub(r' +', ' ', read_text_single_str)
    read_text_single_str = read_text_single_str.replace('= ', '=')
    read_text_for_in = re.findall(r"\sfor\s.*?\sin\s", read_text_single_str, re.DOTALL)
    read_text_while_in = re.findall(r"\swhile\s.*?\sin\s", read_text_single_str, re.DOTALL)
    read_text_from_comma = re.findall(r"\sfrom\s.*?;", read_text_single_str, re.DOTALL)
    read_text_as_from = re.findall(r"\sas\s.*?\sfrom\s", read_text_single_str, re.DOTALL)
    total_for_while = read_text_for_in + read_text_while_in + read_text_from_comma + read_text_as_from
    tl_for_while = []
    for fw in total_for_while:
        for_while_btw = fw.split()
        for fwb in for_while_btw:
            tl_for_while.append(fwb)
    return tl_for_while


def remove_schemaname_is_pack(read_text_single_str):
    read_text_single_str_body = read_text_single_str.replace('package body', 'package')
    read_create_split = re.findall(r'create or replace.*?\bis|as\b', read_text_single_str_body, re.DOTALL)
    main_braces_data_proc_schema = []
    for i_data in read_create_split:
        if '.' in i_data:
            i_data_schema = re.split('package', i_data)[1].split('.', 1)[0]
            i_data_schema_str = ''.join(i_data_schema).replace(' "', '').replace('"', '')
            main_braces_data_proc_schema.append(i_data_schema_str)
    return main_braces_data_proc_schema


def remove_schemaname_is(read_text_single_str):
    read_create_split = re.findall(r'create or replace.*?\bis\b', read_text_single_str, re.DOTALL)
    main_braces_data_proc_schema = []
    for i_data in read_create_split:
        if '.' in i_data:
            i_data_schema = re.split('procedure|function', i_data)[1].split('.', 1)[0]
            i_data_schema_str = ''.join(i_data_schema).replace(' "', '').replace('"', '')
            main_braces_data_proc_schema.append(i_data_schema_str)
    return main_braces_data_proc_schema


def remove_schemaname_as(read_text_single_str):
    read_create_split = re.findall(r'create or replace.*?\bas\b', read_text_single_str, re.DOTALL)
    main_braces_data_proc_schema_as = []
    for i_data in read_create_split:
        if '.' in i_data:
            i_data_schema = re.split('procedure|function', i_data)[1].split('.', 1)[0]
            i_data_schema_str = ''.join(i_data_schema).replace(' "', '').replace('"', '')
            main_braces_data_proc_schema_as.append(i_data_schema_str)
    return main_braces_data_proc_schema_as


def unwanted_before_dot(read_text_single_str):
    read_text_single_str = re.sub(r' +', ' ', read_text_single_str)
    read_text_single_str = read_text_single_str.replace('= ', '=')
    read_text_single_str_data_dot = re.findall(r'\S+\.', read_text_single_str)
    read_text_single_str_data_eql = re.findall(r'=\S+', read_text_single_str)
    read_dot_eql = read_text_single_str_data_dot + read_text_single_str_data_eql
    tl_dot_eql = []
    for de in read_dot_eql:
        de = de.replace('.', '').replace('=', '')
        tl_dot_eql_data = de.split()
        for ded in tl_dot_eql_data:
            tl_dot_eql.append(ded)
    return tl_dot_eql


def data_btw_brackets_proc_func_is(read_text_single_str):
    read_text_single_str = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace('[',
                                                                                                                    ' [ ').replace(
        ']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%', ' % ').replace('=',
                                                                                          ' = ').replace(
        ':=', ' := ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+', ' + ').replace('/',
                                                                                                                ' / ').replace(
        '<', ' < ').replace('*', ' * ').replace('>>', ' >> ').replace('--', '').replace('---',
                                                                                        '').replace(
        '----', '').replace('-----', '').replace('"', ' " ').replace('>', ' > ').replace('  ', ' ').replace("'",
                                                                                                            " ' ").replace(
        "-", " - ")
    read_create_split = re.findall(r'create or replace.*?\bis\b', read_text_single_str, re.DOTALL)
    main_braces_data_proc = []
    for i_data in read_create_split:
        if '(' in i_data:
            data_btw_brackets = re.findall(r'\(([^()]+)\)', i_data)
            remove_btw_brackets_str_prc = ' '.join(data_btw_brackets)
            remove_btw_brackets_str_split = remove_btw_brackets_str_prc.split()
            main_braces_data_proc.append(remove_btw_brackets_str_split)
    lists_main_proc = [x for t in main_braces_data_proc for x in t]
    return lists_main_proc


def data_btw_brackets_proc_func_as(read_text_single_str):
    read_text_single_str = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace('[',
                                                                                                                    ' [ ').replace(
        ']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%', ' % ').replace('=',
                                                                                          ' = ').replace(
        ':=', ' := ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+', ' + ').replace('/',
                                                                                                                ' / ').replace(
        '<', ' < ').replace('*', ' * ').replace('>>', ' >> ').replace('--', '').replace('---',
                                                                                        '').replace(
        '----', '').replace('-----', '').replace('"', ' " ').replace('>', ' > ').replace('  ', ' ').replace("'",
                                                                                                            " ' ").replace(
        "-", " - ")
    read_create_split = re.findall(r'create or replace.*?\bas\b', read_text_single_str, re.DOTALL)
    main_braces_data_proc_as = []
    for i_data in read_create_split:
        if '(' in i_data:
            data_btw_brackets_as = re.findall(r'\(([^()]+)\)', i_data)
            remove_btw_brackets_str_prc_as = ' '.join(data_btw_brackets_as)
            remove_btw_brackets_str_split = remove_btw_brackets_str_prc_as.split()
            main_braces_data_proc_as.append(remove_btw_brackets_str_split)
    lists_main_proc_as = [x for t in main_braces_data_proc_as for x in t]
    return lists_main_proc_as


def select_stmt_logic(read_text_single):
    arg_all_select_statement = read_text_single.split(';')
    arg_all_sel_st = len(arg_all_select_statement)
    return arg_all_sel_st


def common_logic_package(read_text_single, codeobj_list, codeobjnew_list, codeobj_list_sheet1, total_inclusive_dict,
                         third_sheet_changes_packages):
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
    codeobj_list_sheet1 = last_sheet_package(read_text_single_str, third_sheet_changes_packages)
    codeobj_list = arg_calculation_old(read_text_single_str, codeobj_list, read_text_single, whole_detail_count,
                                       'Package', total_inclusive_dict)
    return whole_detail_count, codeobj_list, codeobj_list_sheet1


def common_logic_proc_func(read_text_single, codeobj_list, codeobjnew_list, codeobj_list_sheet1, total_inclusive_dict,
                           third_sheet_changes):
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
    codeobj_list_sheet1 = last_sheet_proc_func(read_text_single_str, third_sheet_changes)
    codeobj_list = arg_calculation_old(read_text_single_str, codeobj_list, read_text_single, whole_detail_count,
                                       detail_type, total_inclusive_dict)

    return whole_detail_count, codeobj_list, codeobj_list_sheet1


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


def insert_into(read_text_single_str):
    read_text_single_str = re.sub(r' +', ' ', read_text_single_str)
    read_text_single_str_data = re.findall(r'\binsert\sinto\b(.*?)\(', read_text_single_str, re.DOTALL)
    insert_data = []
    for ins in read_text_single_str_data:
        insert_val = ins.split()
        for val_ins in insert_val:
            insert_data.append(val_ins)
    return insert_data


def last_sheet_package(read_text_single_str, third_sheet_changes_packages):
    read_text_single_str = re.sub(r":=(.*?);", ' ', read_text_single_str)
    data_from_in_while = unwanted_for_while(read_text_single_str)
    data_insert_into = insert_into(read_text_single_str)
    data_dot_eql = unwanted_before_dot(read_text_single_str)
    data_is_as_begin = unwanted_data_is_as_declare(read_text_single_str)
    data_btw_braces = data_btw_brackets(read_text_single_str)
    removing_schema = remove_schemaname_is_pack(read_text_single_str)
    total_data_ignored = data_btw_braces + data_is_as_begin + removing_schema + data_from_in_while + data_insert_into + data_dot_eql
    total_data_ignored_dupicates = list(set(total_data_ignored))
    read_text_single_datas = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace(
        '[', ' [ ').replace(']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%',
                                                                                          ' % ').replace(
        '=', ' = ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+',
                                                                                          ' + ').replace(
        '/', ' / ').replace('<', ' < ').replace('*', ' * ').replace('"', ' " ').replace('>>', ' >> ').replace(
        '--', '').replace('---', '').replace('----', '').replace('-----', '').replace('>', ' > ').replace('  ',
                                                                                                          ' ').replace(
        "'", " ' ").replace("-", " - ")
    single_datas = re.findall(r"'.*?'", read_text_single_datas)
    for sn in single_datas:
        read_text_single_datas = read_text_single_datas.replace(sn, '')
    read_text_dbms_remove = re.findall(r'dbms_output.*?;', read_text_single_datas)
    if len(read_text_dbms_remove):
        for dbms_remove in read_text_dbms_remove:
            read_text_single_datas = read_text_single_datas.replace(dbms_remove, '')
    list_graterthan_two = []
    read_text_single_datas_split = read_text_single_datas.split()
    if len(read_text_single_datas_split):
        for i_morethan in read_text_single_datas_split:
            if len(i_morethan) > 2:
                list_graterthan_two.append(i_morethan)
    final_data_ignore = []
    data_compares_duplicate = set(total_data_ignored_dupicates)
    for ele in list_graterthan_two:
        if ele not in data_compares_duplicate:
            final_data_ignore.append(ele)
            final_data_ignore = [item for item in final_data_ignore if not item.isnumeric()]
    third_sheet_changes_packages.append(final_data_ignore)


def last_sheet_proc_func(read_text_single_str, third_sheet_changes):
    read_text_single_str = re.sub(r":=(.*?);", ' ', read_text_single_str)
    data_from_in_while = unwanted_for_while(read_text_single_str)
    data_insert_into = insert_into(read_text_single_str)
    data_dot_eql = unwanted_before_dot(read_text_single_str)
    data_is_as_begin = unwanted_data_is_as_declare(read_text_single_str)
    data_btw_braces_is = data_btw_brackets_proc_func_is(read_text_single_str)
    data_btw_braces_as = data_btw_brackets_proc_func_as(read_text_single_str)
    remove_schema_name_is = remove_schemaname_is(read_text_single_str)
    remove_schema_name_as = remove_schemaname_as(read_text_single_str)
    total_data_ignored = data_btw_braces_is + data_btw_braces_as + data_is_as_begin + remove_schema_name_is + remove_schema_name_as + data_from_in_while + data_insert_into + data_dot_eql
    total_data_ignored_dupicates = list(set(total_data_ignored))
    read_text_single_datas = read_text_single_str.replace('(', ' ( ').replace(',', ' , ').replace(')', ' ) ').replace(
        '[', ' [ ').replace(']', ' ] ').replace('.', ' . ').replace('||', ' || ').replace('%',
                                                                                          ' % ').replace(
        '=', ' = ').replace('->', ' -> ').replace(';', ' ; ').replace(':', ' : ').replace('+',
                                                                                          ' + ').replace(
        '/', ' / ').replace('<', ' < ').replace('*', ' * ').replace('"', ' " ').replace('>>', ' >> ').replace(
        '--', '').replace('---', '').replace('----', '').replace('-----', '').replace('>', ' > ').replace('  ',
                                                                                                          ' ').replace(
        "'", " ' ").replace("-", " - ")
    single_datas = re.findall(r"'.*?'", read_text_single_datas)
    for sn in single_datas:
        read_text_single_datas = read_text_single_datas.replace(sn, '')
    read_text_dbms_remove = re.findall(r'dbms_output.*?;', read_text_single_datas)
    if len(read_text_dbms_remove):
        for dbms_remove in read_text_dbms_remove:
            read_text_single_datas = read_text_single_datas.replace(dbms_remove, '')
    list_graterthan_two = []
    read_text_single_datas_split = read_text_single_datas.split()
    if len(read_text_single_datas_split):
        for i_morethan in read_text_single_datas_split:
            if len(i_morethan) > 2:
                list_graterthan_two.append(i_morethan)
    final_data_ignore = []
    data_compares_duplicate = set(total_data_ignored_dupicates)
    for ele in list_graterthan_two:
        if ele not in data_compares_duplicate:
            final_data_ignore.append(ele)
            final_data_ignore = [item for item in final_data_ignore if not item.isnumeric()]
    third_sheet_changes.append(final_data_ignore)


def packages(cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
             all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type, total_inclusive_dict,
             sql_exclusive, all_sheet_new, oracle_singleconnection, check_dir, other_exclusive_data):
    # list and defination query for packages
    print('packages')
    packages_list = []
    oracle_data, orc_pack_query_tag = xml_extractor(xmlpath, None, parent_tag='OraclePackeageExtractor')
    pkglist_query_oracle = orc_pack_query_tag['PackagesList'].replace('@schemaname', cschema_type).replace('@order',
                                                                                                           '')
    # orac_df = oracle_connection(pkglist_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(pkglist_query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    pack_msg = ''
    pack_count = 0
    whole_pack = 0
    # try:
    for item in lists:
        pkgdfe_query = orc_pack_query_tag['PackageDefinition'].replace('@schemaname', cschema_type).replace('@name',
                                                                                                            item)
        query_oracle2 = str(pkgdfe_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:

            def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

            pack_count = pack_count + 1
        except Exception as e:
            logging.error('From Query getting issue %s:', e)
            print('error', e)
            pack_msg = e
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # query_text = query_text.read()   1234
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"

            # single_line_text = function_pre_lower(query_text)
            # single_line_text = single_line_text + "\n"
            # single_line_text = regexarrowencrypt(single_line_text)
            # single_line_text = rm_singlequ(single_line_text)
            # single_line_text = regexarrowdecrypt(single_line_text)

            packages_list.append(query_text)
            sql_path_data = check_dir + '/' + 'PACKAGES' + '/' + item + '.sql'
            create_and_append_sqlfile(sql_path_data, query_text)
        else:
            logging.info('Data Not Found for the %s', item)
            continue
    sub_objects_count = []
    if str(ceecount).upper() == 'D':
        try:
            total_package_count = 0
            whole_pack = 0
            codeobjnew_list = all_codeobjectsnew_list
            third_sheet_changes_packages = []
            for pack_check in packages_list:
                codeobj_list = []
                codeobj_list_sheet1 = []
                single_line_text = function_pre_lower(pack_check)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                pack_check_data = re.sub(r' +', ' ', single_line_text)
                total_package_count, codeobj_list, codeobj_list_sheet1 = common_logic_package(pack_check_data,
                                                                                              codeobj_list,
                                                                                              codeobjnew_list,
                                                                                              codeobj_list_sheet1,
                                                                                              total_inclusive_dict,
                                                                                              third_sheet_changes_packages)
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
                appli_db_powerbireport_sheet2(projectid, iter, app_data, codeobj_list[0], codeobj_list[1],
                                                     codeobj_list[2],
                                                     codeobj_list[3],
                                                     codeobj_list[4],
                                                     codeobj_list[5], min_second, tot_sec_hr)


                whole_pack = whole_pack + total_package_count
                all_codeobjects_list.append(code_min_sec_list)
                all_dataobjectsnew_list.append(codeobj_list_sheet1)
            third_sheet_duplicate_packages = [item for sublist in third_sheet_changes_packages for item in sublist]
            key_list = list(total_inclusive_dict.keys())
            key_list = [x.lower() for x in key_list]
            four_sql_data_estimate = key_list + sql_exclusive + codeobjnew_list + other_exclusive_data
            four_sql_data_estimate_duplicate = list(set(four_sql_data_estimate))
            final_data_ignore_sheet3 = []
            for ele_sheet3 in third_sheet_duplicate_packages:
                if ele_sheet3 not in four_sql_data_estimate_duplicate:
                    final_data_ignore_sheet3.append(ele_sheet3)
                    final_data_ignore_sheet3 = [item for item in final_data_ignore_sheet3 if not item.isnumeric()]
            final_fourth_list_data = []
            for inan in final_data_ignore_sheet3:
                inan = inan.replace('null', '').strip()
                final_fourth_list_data.append(inan)
                final_fourth_list_data = [i.strip() for i in final_fourth_list_data if i != '']

            thrid_count_values = Counter(final_fourth_list_data)
            dist_third = dict(thrid_count_values)
            all_sheet_new.append(dist_third)
            whole_pack = whole_pack
            sub_objects_count.extend(['Packages', len(lists), round(float(whole_pack / 60), 2)])
            statements_list.append(sub_objects_count)

            if str(object_type).upper() == 'ALL':

                if len(lists) == pack_count:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Packages',
                                           pack_count,
                                           round(float((whole_pack) / 60), 2), 'Success',
                                           'Packages Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Packages',
                                           pack_count,
                                           round(float((whole_pack) / 60), 2), 'Failed',
                                           pack_msg)

            #  open(check_dir + '/' + 'PACKAGES' + '/' + 'packages.sql', 'r') as f:
            #     read_test = f.read()
            #     comments_remove = data_remove_comments(read_test)
            #     pack_check_data = re.sub(r' +', ' ', comments_remove)
            #     total_package_count = common_logic_package(pack_check_data)
            #     total_package_count = total_package_count * 45
            #     sub_objects_count.extend(
            #         ['Packages', len(lists), round(float(total_package_count / 60), 2)])
            #     statements_list.append(sub_objects_count)
        except Exception as e:
            print('File Not Found for Packages', e)
            sub_objects_count.extend(['Packages', len(lists), 0])
            statements_list.append(sub_objects_count)

    elif str(ceecount).upper() == 'O':
        pack_est_h = round((obj_packages_est_factor * len(lists)) / 60, 2)
        # sub_objects_count.extend(
        #     ["Packages", len(lists), (pack_est_factor * len(lists)), pack_est_h])
        sub_objects_count.extend(
            ["Packages", len(lists), pack_est_h])
        statements_list.append(sub_objects_count)
    whole_pack_est = round(float((whole_pack) / 60), 2)
    pack_lists = len(lists)
    return all_codeobjects_list, all_dataobjectsnew_list, pack_count, pack_msg, whole_pack_est, pack_lists, all_sheet_new


def procedures(cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
               all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type,
               total_inclusive_dict, sql_exclusive, all_sheet_new, oracle_singleconnection, check_dir,
               other_exclusive_data):
    # list and defination query for procedures
    print("procedures")
    procedure_list = []
    oracle_data, orc_proc_query_tag = xml_extractor(xmlpath, None, parent_tag='OracleProcedureExtractor')
    proclist_query_oracle = orc_proc_query_tag['ProcedureList'].replace('@schemaname',
                                                                        cschema_type).replace(
        '@order',
        '')
    proc_query_oracle = str(proclist_query_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(proc_query_oracle, username, password, hostname, port, sid, cschema_type)  Sk 12/24
    orac_df = query_oracle_connection(proc_query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    proc_count = 0
    proc_msg = ''
    whole_pack = 0
    for item in lists:
        procdfe_query = orc_proc_query_tag['ProcedureDefinition'].replace('@schemaname',
                                                                          cschema_type).replace(
            '@name',
            item)
        try:

            def_orac_df = exe_oracle_connection(procdfe_query, oracle_singleconnection)

            proc_count = proc_count + 1
        except Exception as e:
            logging.error('From Query getting issue %s:', e)
            print('error', e)
            proc_msg = e
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = query_text.read()
            query_text = query_text.replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            procedure_list.append(query_text)
            sql_path_data = check_dir + '/' + 'PROCEDURES' + '/' + item + '.sql'
            create_and_append_sqlfile(sql_path_data, query_text)
        else:
            logging.info('Data Not Found for the %s', item)
            continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
            third_sheet_changes = []
            total_procedure_count = 0
            whole_pack = 0
            codeobjnew_list = all_codeobjectsnew_list
            for proc in procedure_list:
                codeobj_list = []
                codeobj_list_sheet1 = []
                single_line_text = function_pre_lower(proc)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                pack_check_data = re.sub(r' +', ' ', single_line_text)
                total_procedure_count, codeobj_list, codeobj_list_sheet1 = common_logic_proc_func(pack_check_data,
                                                                                                  codeobj_list,
                                                                                                  codeobjnew_list,
                                                                                                  codeobj_list_sheet1,
                                                                                                  total_inclusive_dict,
                                                                                                  third_sheet_changes)

                if codeobj_list[4] != None:
                    # print(codeobj_list[4])
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
                appli_db_powerbireport_sheet2(projectid, iter, app_data, codeobj_list[0], codeobj_list[1],
                                              codeobj_list[2],
                                              codeobj_list[3],
                                              codeobj_list[4],
                                              codeobj_list[5], min_second, tot_sec_hr)
                whole_pack = whole_pack + total_procedure_count
                all_codeobjects_list.append(code_min_sec_list)
                all_dataobjectsnew_list.append(codeobj_list_sheet1)
            third_sheet_duplicate = [item for sublist in third_sheet_changes for item in sublist]
            key_list = list(total_inclusive_dict.keys())
            key_list = [x.lower() for x in key_list]
            four_sql_data_estimate = key_list + sql_exclusive + codeobjnew_list + other_exclusive_data
            four_sql_data_estimate_duplicate = list(set(four_sql_data_estimate))
            final_data_ignore_sheet3 = []
            for ele_sheet3 in third_sheet_duplicate:
                if ele_sheet3 not in four_sql_data_estimate_duplicate:
                    final_data_ignore_sheet3.append(ele_sheet3)
                    final_data_ignore_sheet3 = [item for item in final_data_ignore_sheet3 if not item.isnumeric()]
            final_fourth_list_data = []
            for inan in final_data_ignore_sheet3:
                inan = inan.replace('null', '').strip()
                final_fourth_list_data.append(inan)
                final_fourth_list_data = [i.strip() for i in final_fourth_list_data if i != '']

            thrid_count_values = Counter(final_fourth_list_data)
            dist_third = dict(thrid_count_values)
            all_sheet_new.append(dist_third)
            whole_pack = whole_pack
            sub_object_count.extend(['Procedures', len(lists), round(float(whole_pack / 60), 2)])
            statements_list.append(sub_object_count)
            if str(object_type).upper() == 'ALL':
                if len(lists) == proc_count:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Procedures',
                                           proc_count,
                                           round(float((whole_pack) / 60), 2), 'Success',
                                           'Procedures Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Procedures',
                                           proc_count,
                                           round(float((whole_pack) / 60), 2), 'Failed',
                                           proc_msg)
        except Exception as f:
            print('File Not Found for Procedures')
            sub_object_count.extend(['Procedures', len(lists), 0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        proc_est_h = round((obj_procedure_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(
        #     ["Procedures", len(lists), (proc_est_factor * len(lists)), proc_est_h])
        sub_object_count.extend(
            ["Procedures", len(lists), proc_est_h])
        statements_list.append(sub_object_count)
    proc_est = round(float((whole_pack) / 60), 2)
    proc_list = len(lists)
    return all_codeobjects_list, all_dataobjectsnew_list, proc_count, proc_msg, proc_est, proc_list, all_sheet_new


def functions(cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
              all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type, total_inclusive_dict,
              sql_exclusive, all_sheet_new, oracle_singleconnection, check_dir, other_exclusive_data):
    # list and defination query for functions
    print('functions')
    functions_list = []
    oracle_data, orc_fun_query_tag = xml_extractor(xmlpath, None, parent_tag='OracleFunctionExtractor')
    funlist_query_oracle = orc_fun_query_tag['FunctionList'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    fun_query_oracle = str(funlist_query_oracle).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(fun_query_oracle, oracle_singleconnection)  # -- SK 12/24

    lists = orac_df[orac_df.columns[0]].values.tolist()
    func_count = 0
    func_msg = ''
    whole_pack = 0
    for item in lists:
        fundfe_query = orc_fun_query_tag['FunctionDefinition'].replace('@schemaname', cschema_type).replace(
            '@name',
            item)
        try:

            def_orac_df = exe_oracle_connection(fundfe_query, oracle_singleconnection)

            func_count = func_count + 1
        except Exception as e:
            logging.error('From Query getting issue %s:', e)
            print('error', e)
            func_msg = e
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = query_text.read()
            query_text = query_text.replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            functions_list.append(query_text)
            sql_path_data = check_dir + '/' + 'FUNCTIONS' + '/' + item + '.sql'
            create_and_append_sqlfile(sql_path_data, query_text)
        else:
            logging.info('Data Not Found for the %s', item)
            continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
            total_functions_count = 0
            whole_pack = 0
            codeobjnew_list = all_codeobjectsnew_list
            third_sheet_changes = []
            for func in functions_list:
                codeobj_list = []
                codeobj_list_sheet1 = []
                single_line_text = function_pre_lower(func)
                single_line_text = single_line_text + "\n"
                single_line_text = regexarrowencrypt(single_line_text)
                single_line_text = rm_singlequ(single_line_text)
                single_line_text = regexarrowdecrypt(single_line_text)
                comments_remove = re.sub(r' +', ' ', single_line_text)
                total_functions_count, codeobj_list, codeobj_list_sheet1 = common_logic_proc_func(comments_remove,
                                                                                                  codeobj_list,
                                                                                                  codeobjnew_list,
                                                                                                  codeobj_list_sheet1,
                                                                                                  total_inclusive_dict,
                                                                                                  third_sheet_changes)


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
                appli_db_powerbireport_sheet2(projectid, iter, app_data, codeobj_list[0], codeobj_list[1],
                                              codeobj_list[2],
                                              codeobj_list[3],
                                              codeobj_list[4],
                                              codeobj_list[5], min_second, tot_sec_hr)
                whole_pack = whole_pack + total_functions_count
                all_codeobjects_list.append(code_min_sec_list)
                all_dataobjectsnew_list.append(codeobj_list_sheet1)
            third_sheet_duplicate = [item for sublist in third_sheet_changes for item in sublist]
            key_list = list(total_inclusive_dict.keys())
            key_list = [x.lower() for x in key_list]
            four_sql_data_estimate = key_list + sql_exclusive + codeobjnew_list + other_exclusive_data
            four_sql_data_estimate_duplicate = list(set(four_sql_data_estimate))
            final_data_ignore_sheet3 = []
            for ele_sheet3 in third_sheet_duplicate:
                if ele_sheet3 not in four_sql_data_estimate_duplicate:
                    final_data_ignore_sheet3.append(ele_sheet3)
                    final_data_ignore_sheet3 = [item for item in final_data_ignore_sheet3 if not item.isnumeric()]
            final_fourth_list_data = []
            for inan in final_data_ignore_sheet3:
                inan = inan.replace('null', '').strip()
                final_fourth_list_data.append(inan)
                final_fourth_list_data = [i.strip() for i in final_fourth_list_data if i != '']
            thrid_count_values = Counter(final_fourth_list_data)
            dist_third = dict(thrid_count_values)
            all_sheet_new.append(dist_third)
            whole_pack = whole_pack
            sub_object_count.extend(
                ['Functions', len(lists), round(float(whole_pack / 60), 2)])
            statements_list.append(sub_object_count)
            if str(object_type).upper() == 'ALL':
                if len(lists) == func_count:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Functions',
                                           func_count,
                                           round(float(whole_pack / 60), 2), 'Success',
                                           'Functions Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Functions',
                                           func_count,
                                           round(float(whole_pack / 60), 2), 'Failed',
                                           func_msg)
        except Exception as e:
            print('File Not Found for Functions')
            sub_object_count.extend(
                ['Functions', len(lists), 0])
            statements_list.append(sub_object_count)

    elif str(ceecount).upper() == 'O':
        func_est_h = round((obj_function_est_factor * len(lists)) / 60, 2)
        # sub_object_count.extend(["Functions", len(lists), (func_est_factor * len(lists)), func_est_h])
        sub_object_count.extend(["Functions", len(lists), func_est_h])
        statements_list.append(sub_object_count)
    func_est = round(float(whole_pack / 60), 2)
    func_list = len(lists)
    return all_codeobjects_list, all_dataobjectsnew_list, func_count, func_msg, func_est, func_list, all_sheet_new


def triggers(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
             statements_list, ceecount, all_codeobjects_list, app_data, iter, projectid, connection_name, object_type,
             total_inclusive_dict):
    oracle_trigger_data, orc_tri_query_tag = xml_extractor(xmlpath, None, parent_tag='OracletriggerExtractor')
    trigger_list_query_oracle = orc_tri_query_tag['listtrigger'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    trigger_def_query_oracle = orc_tri_query_tag['deftrigger'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')

    trigger_list_query = trigger_list_query_oracle
    trigger_def_query = trigger_def_query_oracle

    print('triggers')
    triggers_list = []
    query_oracle = str(trigger_list_query).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    tri_count = 0
    tri_msg = ''
    # print(lists)
    for item in lists:
        query_oracle2 = str(trigger_def_query).replace('@schemaname', cschema_type).replace('@name', item)
        try:

            def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

            tri_count = tri_count + 1
        except Exception as e:
            logging.error('From Query getting issue %s:', e)
            print('error', e)
            tri_msg = e
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            # query_text = query_text.read() 1234
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            triggers_list.append(query_text)
            sql_path_data = check_dir + '/' + 'TRIGGERS' + '/' + item + '.sql'
            create_and_append_sqlfile(sql_path_data, query_text)
        else:
            logging.info('Data Not Found for the %s', item)
            continue
    sub_object_count = []
    if str(ceecount).upper() == 'D':
        try:
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
                    appli_db_powerbireport_sheet2(projectid, iter, app_data, codeobj_list[0], codeobj_list[1],
                                                  codeobj_list[2],
                                                  codeobj_list[3],
                                                  codeobj_list[4],
                                                  codeobj_list[5], min_second, tot_sec_hr)

                    all_codeobjects_list.append(code_min_sec_list)
            whole_pack = whole_pack
            # total_estimation_triggers = type_comma_count + type_semicolon_count
            # total_estimation_triggers = (total_estimation_triggers * 7.5)
            # sub_object_count.extend(['Triggers', len(lists), total_estimation_triggers,
            #                          round(float(total_estimation_triggers / 60), 2)])
            sub_object_count.extend(['Triggers', len(lists),
                                     round(float(whole_pack / 60), 2)])
            statements_list.append(sub_object_count)
            if str(object_type).upper() == 'ALL':
                if len(lists) == tri_count:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Triggers',
                                           tri_count,
                                           round(float(whole_pack / 60), 2), 'Success',
                                           'Triggers Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Triggers',
                                           tri_count,
                                           round(float(whole_pack / 60), 2),
                                           'Failed',
                                           tri_msg)
        except Exception as e:
            print("File Not Found for Triggers")
            sub_object_count.extend(['Triggers', len(lists),
                                     0])
            statements_list.append(sub_object_count)
    elif str(ceecount).upper() == 'O':
        trig_est_h = round(float((obj_trigger_est_factor * len(lists)) / 60), 2)
        # sub_object_count.extend(["Triggers", len(lists), (trig_est_factor * len(lists)), trig_est_h])
        sub_object_count.extend(["Triggers", len(lists), trig_est_h])
        statements_list.append(sub_object_count)
    return all_codeobjects_list


def datatypes(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
              statements_list, object_type):
    oracle_datatype_data, orc_data_query_tag = xml_extractor(xmlpath, None, parent_tag='OracledatatypeExtractor')
    date_type_query_oracle = orc_data_query_tag['Oracledatatype'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    data_type_query = date_type_query_oracle
    print('DATATYPES')
    data_type_oracle = str(data_type_query).replace('@schemaname', cschema_type)

    # Commented By Sk 12/24 for using the Single Connection common for all Utility
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}
    # datatype_orac = oracle_conn_data_type(oracle_data, data_type_oracle)

    datatype_orac = query_oracle_connection(data_type_oracle, oracle_singleconnection)

    datatype_list = [i[0] for i in datatype_orac]
    datatype_orac_str = '\n'.join(datatype_list)
    sql_path_data = check_dir + '/' + 'DATATYPES' + '/' + 'datatypes.sql'
    create_and_append_sqlfile(sql_path_data, datatype_orac_str)


def synonyms(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
             statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    # list and defination query for synonyms
    print('synonyms')
    synonyms_list = []
    oracle_data, orc_sys_query_tag = xml_extractor(xmlpath, None, parent_tag='OracleSynonymExtractor')
    syslist_query_oracle = orc_sys_query_tag['SynonymList'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    sys_query_oracle = str(syslist_query_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(sys_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(sys_query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    sys_count = 0
    error_msg = ''
    try:
        for item in lists:
            sysdfe_query = orc_sys_query_tag['SynonymDefinition'].replace('@schemaname', cschema_type).replace(
                '@name',
                item)
            try:

                def_orac_df = exe_oracle_connection(sysdfe_query, oracle_singleconnection)

                sys_count = sys_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s :', e)
                print('error', e)
                error_msg = e
                continue
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = query_text.read()
                query_text = query_text.replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                synonyms_list.append(query_text)
                sql_path_data = check_dir + '/' + 'SYNONYMS' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        sub_object_count = []
        if str(ceecount).upper() == 'D':
            try:
                synonym_count = 0
                synonym_at_count = 0
                synonym_estimation_count = 0
                for i in synonyms_list:
                    synonym_estimation_count = synonym_estimation_count + 1
                    i_atfinding = re.findall(r'@(.*?),|@(.*?);', i)
                    if len(i_atfinding) != 0:
                        synonym_at_count = synonym_at_count + 1
                    else:
                        synonym_count = synonym_count + 1
                total_estimation_synonyms = (synonym_at_count * detail_synon_dblink_est_factor) + (
                        synonym_count * detail_synon_est_factor)
                # sub_object_count.extend(['Synonyms', len(lists), total_estimation_synonyms,
                #                          round(float((total_estimation_synonyms) / 60), 2)])
                sub_object_count.extend(['Synonym', len(lists),
                                         round(float((total_estimation_synonyms) / 60), 2)])
                statements_list.append(sub_object_count)
                if str(object_type).upper() == 'ALL':
                    if len(lists) == sys_count:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Synonym',
                                               sys_count,
                                               round(float((total_estimation_synonyms) / 60), 2),
                                               'Success',
                                               'Synonyms Extracted Successfully')
                    else:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Synonym',
                                               sys_count,
                                               round(float((total_estimation_synonyms) / 60), 2),
                                               'Failed',
                                               error_msg)
            except Exception as f:
                print('File Not found for Synonyms', f)
                sub_object_count.extend(['Synonym', len(lists), 0])
                statements_list.append(sub_object_count)

        elif str(ceecount).upper() == 'O':
            syn_est_factor_h = round((obj_synon_est_factor * len(lists)) / 60, 2)
            # sub_object_count.extend(["Synonyms", len(lists), (syn_est_factor * len(lists)), syn_est_factor_h])
            sub_object_count.extend(["Synonym", len(lists), syn_est_factor_h])
            statements_list.append(sub_object_count)
    except Exception as sys:
        print('Synonyms having issue ', sys)


def tables(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
           statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    oracle_tablelist_data, orc_table_list_tag = xml_extractor(xmlpath, None, parent_tag='OracletablelistExtrator')
    type_listquery_oracle = orc_table_list_tag['tablelist'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    type_defquery_oracle = orc_table_list_tag['tabledef'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')

    table_list_query = type_listquery_oracle
    table_def_query = type_defquery_oracle

    print('tables')
    query_oracle = str(table_list_query).replace('@schemaname', cschema_type)
    # Comm -- SK 12/24
    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    # print(orac_df)

    ## ADde --Sk 12/24
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    # print(lists)
    table_count_succes = 0
    error_msg = ''
    try:
        for item in lists:
            query_oracle2 = str(table_def_query).replace('@schemaname', cschema_type).replace('@name', item)
            # print(query_oracle2)
            try:
                # Commented -- Sk 12/24
                # def_orac_df = exe_oracle_connection(query_oracle2, username, password, hostname, port, sid,
                #                                     cschema_type)
                # Added -- Sk 12/24
                def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

                table_count_succes = table_count_succes + 1
                # print(def_orac_df)
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                error_msg = e
                continue
            # print(def_orac_df)
            # print(def_orac_df.values)
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                # query_text = query_text.read()
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                # read_text = query_text.read()
                sql_path_data = check_dir + '/' + 'TABLES' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue

        sub_objects_count = []
        if str(ceecount).upper() == 'D':
            # sub_objects_count.extend(
            #     ['Tables', len(lists), len(lists) * table_esti_factor,
            #      round(float((len(lists) * table_esti_factor) / 60), 2)])
            sub_objects_count.extend(
                ['Tables', len(lists),
                 round(float((len(lists) * detail_tables_est_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                if table_count_succes == len(lists):
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Tables',
                                           table_count_succes,
                                           round(float((table_count_succes * detail_tables_est_factor) / 60), 2),
                                           'Success',
                                           'Tables Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Tables',
                                           table_count_succes,
                                           round(float((table_count_succes * detail_tables_est_factor) / 60), 2),
                                           'Failed',
                                           error_msg)
        elif str(ceecount).upper() == 'O':
            # c-- Sk 12/24
            # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

            oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
                                                                   parent_tag='OraclecountExtractorvalidation')
            count_tablequery_oracle = orc_count_query_tag['count_table_query'].replace('@schemaname', cschema_type)
            count_table_query = count_tablequery_oracle.replace('@schemaname', cschema_type)

            # ca -- Sk 12/24
            # data = get_count(oracle_data, count_table_query)
            data = get_count(oracle_singleconnection, count_table_query)

            if data:
                table_co = data[0][0]
                column_count = data[0][1]
                # estimate_factor = 15
                esimate = round((obj_tables_est_factor * len(lists)) / 60, 2)
                # sub_objects_count.extend(
                #     ['Tables', len(lists), estimate_factor * len(lists), esimate])
                sub_objects_count.extend(
                    ['Tables', len(lists), esimate])
                statements_list.append(sub_objects_count)

    except Exception as te:
        print('Tables Extraction Having issue, ', te)


def package_data(check_dir, oracle_singleconnection, cschema_type, object_type):
    pack_str = ''' 
    select * from (
    select actual_code,(''||OBJECT_NAME||'_'||PROCEDURE_NAME||'') as modified_code from (
    select OWNER,PROCEDURE_NAME,OBJECT_TYPE,OBJECT_NAME,(OBJECT_NAME||'.'||PROCEDURE_NAME)as actual_code 
    from dba_procedures where owner='@schemaname'
    AND OBJECT_TYPE ='PACKAGE' AND PROCEDURE_NAME IS NOT NULL
    ) 
    UNION
    select actual_code,OBJECT_NAME as modified_code from (
    select OWNER,PROCEDURE_NAME,OBJECT_TYPE,OBJECT_NAME,(OBJECT_NAME)as actual_code from dba_procedures 
    where owner='@schemaname'
    AND OBJECT_TYPE ='PROCEDURE' AND PROCEDURE_NAME IS NULL
    ) ) order by 1

    '''

    packages_list_query = """
    select * from (
    select actual_code AS MODIFIED_cODE,(''||OBJECT_NAME||'_'||PROCEDURE_NAME||'') as ACT_CODE from (
    select OWNER,PROCEDURE_NAME,OBJECT_TYPE,OBJECT_NAME,(OBJECT_NAME||'.'||PROCEDURE_NAME)as actual_code from dba_procedures where owner='@schemaname'
    AND OBJECT_TYPE ='PACKAGE' AND PROCEDURE_NAME IS NOT NULL
    ) 
    )  order by 1
    """

    storing_src_path = check_dir + '/' + 'PERFORM_DATA'
    if os.path.isdir(storing_src_path):
        shutil.rmtree(storing_src_path)
    if not os.path.exists(storing_src_path):
        os.makedirs(storing_src_path)
    Excel_path = check_dir + '/' + 'PERFORM_DATA' + '/' + 'perform_excel' + '.xlsx'
    pack_excel_path = check_dir + '/' + 'PERFORM_DATA' + '/' + 'packages_list' + '.xlsx'
    pack_str = pack_str.replace('@schemaname', str(cschema_type).upper())
    packages_list_query = packages_list_query.replace('@schemaname', str(cschema_type).upper())

    # final_orac_df = oracle_connection(pack_str, username, password, hostname, port, sid, cschema_type)
    # pack_orac_df = oracle_connection(packages_list_query, username, password, hostname, port, sid, cschema_type)
    final_orac_df = query_oracle_connection(pack_str, oracle_singleconnection)
    pack_orac_df = query_oracle_connection(packages_list_query, oracle_singleconnection)

    final_orac_df.to_excel(Excel_path, index=False)
    pack_orac_df.to_excel(pack_excel_path, index=False)


def global_temp_tables(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                       statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    oracle_globaltemptable_data, orc_globaltemptable_list_tag = xml_extractor(xmlpath, None,
                                                                              parent_tag='OracleGlobalTempTableTExtractor')
    type_globaltemptable_oracle = orc_globaltemptable_list_tag['TempTableList'].replace('@schemaname', cschema_type)
    query_oracle = str(type_globaltemptable_oracle).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()

    list_str = '\n'.join(lists)
    storing_src_path = check_dir + '/' + 'GLOBAL_TEMP_TABLE'
    if os.path.isdir(storing_src_path):
        shutil.rmtree(storing_src_path)
    if not os.path.exists(storing_src_path):
        os.makedirs(storing_src_path)
    create_and_append_configfile(storing_src_path + '/' + 'global_temp_tables.txt', list_str)
    create_and_append_configfile(storing_src_path + '/' + 'global_temp_tables_count.txt', len(lists))
    for item in lists:
        temptable_def_query = orc_globaltemptable_list_tag['TempTableDef'].replace('@schemaname', cschema_type).replace(
            'TEMPTABLE', item)
        try:
            def_orac_df = exe_oracle_connection(temptable_def_query, oracle_singleconnection)
        except Exception as e:
            logging.error('From Query getting issue %s:', e)
            print('error', e)
            continue
        if len(def_orac_df):
            query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
            query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
            if not query_text.strip().endswith(';'):
                query_text = query_text + ";"
            sql_path_data = check_dir + '/' + 'TABLES' + '/' + 'gtt_tables.sql'
            create_and_append_sqlfile(sql_path_data, query_text)
        else:
            logging.info('Data Not Found for the %s', item)
            continue
    sql_path_data = check_dir + '/' + 'TABLES' + '/' + 'gtt_tables.sql'
    create_and_append_sqlfile(sql_path_data, '')

    if str(object_type).upper() == 'ALL':
        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Global_Temp_Table',
                               len(lists),
                               len(lists), 'Success',
                               'GTT Extracted Successfully')


def types(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
          statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    print('types')
    types_list = []
    # list and defination query for types
    oracle_data, orc_type_query_tag = xml_extractor(xmlpath, None, parent_tag='OracleTypeExtractor')
    typelist_query_oracle = orc_type_query_tag['TypesList'].replace('@schemaname', cschema_type).replace(
        '@order', '')
    query_oracle = str(typelist_query_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    ty_count = 0
    ty_msg = ''
    total_estimation_types = 0
    try:
        # types_count =0
        for item in lists:
            typedfe_query = orc_type_query_tag['TypeDefinition'].replace('@schemaname', cschema_type).replace(
                '@name', item)
            try:

                def_orac_df = exe_oracle_connection(typedfe_query, oracle_singleconnection)

                ty_count = ty_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                ty_msg = e
                continue
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                # query_text = query_text.read()
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                types_list.append(query_text)
                sql_path_data = check_dir + '/' + 'TYPES' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        sub_object_count = []
        if str(ceecount).upper() == 'D':
            try:
                types_estimation_count = 0
                type_comma_count = 0
                type_semicolon_count = 0
                for i in types_list:
                    single_line_text = function_pre_lower(i)
                    single_line_text = single_line_text + "\n"
                    single_line_text = regexarrowencrypt(single_line_text)
                    single_line_text = rm_singlequ(single_line_text)
                    single_line_text = regexarrowdecrypt(single_line_text)
                    single_line_text = re.sub(r' +', ' ', single_line_text)
                    if 'type "' in single_line_text:
                        types_estimation_count = types_estimation_count + 1
                        single_line_text.count(',')
                        type_comma_count = type_comma_count + single_line_text.count(',')
                    if 'type body' in single_line_text:
                        single_line_text.count(';')
                        type_semicolon_count = type_semicolon_count + single_line_text.count(';')
                total_estimation_types = type_comma_count + type_semicolon_count
                # sub_object_count.extend(
                #     ['Types', len(lists), total_estimation_types, round(float((total_estimation_types) / 60), 2)])
                # statements_list.append(sub_object_count)
                sub_object_count.extend(
                    ['Types', len(lists), round(float((total_estimation_types) / 60), 2)])
                statements_list.append(sub_object_count)
                if str(object_type).upper() == 'ALL':
                    if len(lists) == ty_count:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'User_Defined_Types',
                                               ty_count,
                                               round(float((total_estimation_types) / 60), 2),
                                               'Success',
                                               'User_Defined_Types Extracted Successfully')
                    else:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'User_Defined_Types',
                                               ty_count,
                                               round(float((total_estimation_types) / 60), 2),
                                               'Failed',
                                               ty_msg)
            except Exception as f:
                print('File NOT Found for types')
                sub_object_count.extend(['Types', len(lists), 0])
                statements_list.append(sub_object_count)

        elif str(ceecount).upper() == 'O':
            types_est_factor_h = round(((obj_types_est_factor * len(lists)) / 60), 2)
            # sub_object_count.extend(
            #     ["Types", len(lists), (types_est_factor * len(lists)), types_est_factor_h])
            sub_object_count.extend(
                ["Types", len(lists), types_est_factor_h])
            statements_list.append(sub_object_count)
    except Exception as ty:
        print("Types having issue ", ty)

    total_estimation_types_est = round(float((total_estimation_types) / 60), 2)
    ty_lists = len(lists)
    return ty_count, ty_msg, total_estimation_types_est, ty_lists


def indexes(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
            statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    oracle_indexlist_data, orc_index_list_tag = xml_extractor(xmlpath, None, parent_tag='OracleindexlistExtractor')
    type_indlistquery_oracle = orc_index_list_tag['indexlist'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    type_inddefquery_oracle = orc_index_list_tag['deflist'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')

    index_list_query = type_indlistquery_oracle
    index_def_query = type_inddefquery_oracle

    print('indexes')
    query_oracle = str(index_list_query).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    ind_success_count = 0
    query_main_detail = """
                        select count(1) from (
                        select 'CREATE INDEX '||INDEX_NAME||' ON '||TABLE_NAME||'('||INDEX_COLS||');', 5 as sno FROM
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
                        DBA_IND_EXPRESSIONS WHERE index_owner = upper(''@schemaname'') and table_name not like ''%$%'' '
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
                        ) res where not exists (select 1 from dba_mviews mv where mv.MVIEW_NAME=res.table_name and lower(owner)=lower('@schemaname'))
                        )""".replace('@schemaname', cschema_type)
    orac_df_count = query_oracle_connection(query_main_detail, oracle_singleconnection)
    check_count = orac_df_count['COUNT(1)'][0]
    error_msg = ''
    try:
        # index_count = 0
        for item in lists:
            query_oracle2 = str(index_def_query).replace('@schemaname', cschema_type).replace('@name', item)
            try:

                def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

                ind_success_count = ind_success_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                error_msg = e
                continue
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                # query_text = query_text.read()
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                # read_text = query_text.read()
                sql_path_data = check_dir + '/' + 'INDEXES' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        sub_objects_count = []
        if str(ceecount).upper() == 'D':
            # sub_objects_count.extend(
            #     ['Indexes', len(lists), len(lists * index_esti_factor),
            #      round(float(len(lists * index_esti_factor) / 60), 2)])
            sub_objects_count.extend(
                ['Indexes', len(lists),
                 round(float(len(lists * index_esti_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Indexes',
                                       check_count,
                                       round(float((check_count * index_esti_factor) / 60), 2), 'Success',
                                       'Indexes Extracted Successfully')
                # if len(lists) == ind_success_count:
                #     appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Indexes',
                #                            ind_success_count,
                #                            round(float((ind_success_count * index_esti_factor) / 60), 2), 'Success',
                #                            'Indexes Extracted Successfully')
                # else:
                #     appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Indexes',
                #                            ind_success_count,
                #                            round(float((ind_success_count * index_esti_factor) / 60), 2), 'Failed',
                #                            error_msg)
        elif str(ceecount).upper() == 'O':
            ind_est_factor_h = round((index_esti_factor * len(lists)) / 60, 2)
            # sub_objects_count.extend(
            #     ["Indexes", len(lists), (index_factor * len(lists)), ind_est_factor_h])
            sub_objects_count.extend(
                ["Indexes", len(lists), ind_est_factor_h])
            statements_list.append(sub_objects_count)
    except Exception as ind:
        print('Indexes having issue', ind)


def views_mviews(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                 statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    print('views')
    views_list = []
    oracle_viwedef_data, orc_viewlist_tag = xml_extractor(xmlpath, None, parent_tag='OracleviewdefExtractor')
    viewlistquery_oracle = orc_viewlist_tag['viewlistquery'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    viewdefquery_oracle = orc_viewlist_tag['viewdefquery'].replace('@schemaname', cschema_type).replace(
        '@order',
        '')
    view_list_query = viewlistquery_oracle
    view_def_query = viewdefquery_oracle

    query_oracle = str(view_list_query).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    views_count = 0
    views_err_msg = ''
    try:
        for item in lists:
            query_oracle2 = str(view_def_query).replace('@schemaname', cschema_type).replace('@name', item)
            try:

                def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

                views_count = views_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                views_err_msg = ''
                continue
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                # query_text = query_text.read()
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                # read_text = query_text.read()
                views_list.append(query_text)
                sql_path_data = check_dir + '/' + 'VIEWS' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        sub_object_count = []
        if str(ceecount).upper() == 'D':
            try:
                simple_count = 0
                medium_count = 0
                complex_count = 0
                highly_complex_count = 0
                views_count = 0
                for i in views_list:
                    # read_text_single_str = "".join(read_text_single)
                    # create_or_replace_split = view_check.split('create or replace')
                    # create_or_replace_split = [i.strip() for i in create_or_replace_split if i != '']
                    # for i in create_or_replace_split:
                    views_count = views_count + 1
                    # i = 'create or replace ' + i
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

                # sub_object_count.extend(
                #     ['Views', len(lists), total_estimation_view, round(float((total_estimation_view) / 60), 2)])
                # statements_list.append(sub_object_count)
                sub_object_count.extend(
                    ['Views', len(lists), round(float((total_estimation_view) / 60), 2)])
                statements_list.append(sub_object_count)
                if str(object_type).upper() == 'ALL':
                    if len(lists) == views_count:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Views',
                                               views_count,
                                               round(float((total_estimation_view) / 60), 2),
                                               'Success',
                                               'Views Extracted Successfully')
                    else:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Views',
                                               views_count,
                                               round(float((total_estimation_view) / 60), 2),
                                               'Failed',
                                               views_err_msg)
            except Exception as f:
                print('File Not Found for Views')
                sub_object_count.extend(['Views', len(lists), 0])
                statements_list.append(sub_object_count)
        elif str(ceecount).upper() == 'O':

            # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

            oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
                                                                   parent_tag='OraclecountExtractorvalidation')
            count_viewquery_oracle = orc_count_query_tag['count_view_query'].replace('@schemaname', cschema_type)
            sub_objects_count = []
            count_view_query = count_viewquery_oracle.replace('@schemaname', cschema_type)

            data = get_count(oracle_singleconnection, count_view_query)

            if data:
                part1 = data[0][0]
                part2 = data[0][1]
                esimate = round((obj_views_mviews_est_factor * part1) / 60, 2)
                # sub_objects_count.extend(
                #     ['Views', part1, (estimate_factor * part1), esimate])
                sub_objects_count.extend(
                    ['Views', part1, esimate])
                statements_list.append(sub_objects_count)
    except Exception as vi:
        print('Views Hvaing issue ', vi)

    print("mviews")
    mviews_list = []
    oracle_materialized_data, orc_materialiizedlist_tag = xml_extractor(xmlpath, None,
                                                                        parent_tag='Oraclematerializedlistdef')
    materializedlistquery_oracle = orc_materialiizedlist_tag['materializedlist'].replace('@schemaname',
                                                                                         cschema_type).replace(
        '@order',
        '')
    materializeddefquery_oracle = orc_materialiizedlist_tag['materializeddef'].replace('@schemaname',
                                                                                       cschema_type).replace(
        '@order',
        '')

    materialized_list_query = materializedlistquery_oracle

    materialized_def_query = materializeddefquery_oracle
    print('MATERIALIZED_VIEW')
    query_oracle = str(materialized_list_query).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    mv_count = 0
    mv_msg = ''
    try:
        for item in lists:
            query_oracle2 = str(materialized_def_query).replace('@schemaname', cschema_type).replace('@name', item)
            try:

                def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

                mv_count = mv_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                mv_msg = e
                continue
            if len(def_orac_df):
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
                # query_text = 'set search_path =' + cschema_type.lower() + '\n\n' + query_text
                sql_path_data = check_dir + '/' + 'MATERIALIZED_VIEWS' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        if str(ceecount).upper() == 'D':
            sub_object_count = []
            try:
                simple_count = 0
                medium_count = 0
                complex_count = 0
                highly_complex_count = 0
                mviews_count = 0
                for i in mviews_list:
                    # read_text_single_str = "".join(read_text_single)
                    # create_or_replace_split = mviews_check.split('create')
                    # create_or_replace_split = [i.strip() for i in create_or_replace_split if i != '']
                    mviews_count = mviews_count + 1
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

                # print(round(float((total_estimation_mview) / 60), 2), 'mviewssssssssssssssssssssss')
                # sub_object_count.extend(
                #     ['Mviews', len(lists), total_estimation_mview, round(float((total_estimation_mview) / 60), 2)])
                # statements_list.append(sub_object_count)
                sub_object_count.extend(
                    ['Mviews', len(lists), round(float((total_estimation_mview) / 60), 2)])
                statements_list.append(sub_object_count)
                if str(object_type).upper() == 'ALL':
                    if len(lists) == mv_count:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Materialized_views',
                                               mviews_count,
                                               round(float((total_estimation_mview) / 60), 2),
                                               'Success',
                                               'Materialized_views Extracted Successfully')
                    else:
                        appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                               'Materialized_views', mviews_count,
                                               round(float((total_estimation_mview) / 60), 2),
                                               'Failed', mv_msg)
            except Exception as f:
                print('File Not Found for MVIEWS')
                sub_object_count.extend(['Mviews', len(lists), 0])
                statements_list.append(sub_object_count)
        elif str(ceecount).upper() == 'O':
            # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}
            oracle_count_data, orc_count_query_tag = xml_extractor(xmlpath, None,
                                                                   parent_tag='OraclecountExtractorvalidation')
            count_mviewquery_oracle = orc_count_query_tag['count_mview_query'].replace('@schemaname', cschema_type)
            sub_objects_count = []
            count_mview_query = count_mviewquery_oracle.replace('@schemaname', cschema_type)

            data = get_count(oracle_singleconnection, count_mview_query)

            if data:
                part1 = data[0][0]
                part2 = data[0][1]
                esimate = round((obj_views_mviews_est_factor * part1) / 60, 2)
                # sub_objects_count.extend(
                #     ['MViews', part1, (estimate_factor * part1), esimate])
                sub_objects_count.extend(
                    ['MViews', part1, esimate])
                statements_list.append(sub_objects_count)
    except Exception as mv:
        print('Mviews having issue ', mv)


def sequences(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
              statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    oracle_sequence_data, orc_seqlist_tag = xml_extractor(xmlpath, None, parent_tag='Oraclesequencelistdef')
    sequence_list_oracle = orc_seqlist_tag['sequencelist'].replace('@schemaname', cschema_type).replace('@order', '')
    sequence_def_oracle = orc_seqlist_tag['sequencedef'].replace('@schemaname', cschema_type).replace('@order', '')

    sequence_list_query = sequence_list_oracle

    sequence_def_query = sequence_def_oracle
    print('sequence')
    query_oracle = str(sequence_list_query).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)

    lists = orac_df[orac_df.columns[0]].values.tolist()
    seq_count = 0
    error_msg = ''
    try:
        for item in lists:
            query_oracle2 = str(sequence_def_query).replace('@schemaname', cschema_type).replace('@name', item)
            try:

                def_orac_df = exe_oracle_connection(query_oracle2, oracle_singleconnection)

                seq_count = seq_count + 1
            except Exception as e:
                logging.error('From Query getting issue %s:', e)
                print('error', e)
                error_msg = e
                continue
            if len(def_orac_df):
                query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                # query_text = query_text.read()
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                # read_text = query_text.read()
                sql_path_data = check_dir + '/' + 'SEQUENCES' + '/' + item + '.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
            else:
                logging.info('Data Not Found for the %s', item)
                continue
        sub_objects_count = []
        if str(ceecount).upper() == 'D':
            # sub_objects_count.extend(['Sequences', len(lists), len(lists * sequence_esti_factor),
            #                           round(float(len(lists * sequence_esti_factor) / 60), 2)])
            sub_objects_count.extend(['Sequences', len(lists),
                                      round(float((len(lists) * sequence_esti_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                if len(lists) == seq_count:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Sequences',
                                           seq_count,
                                           round(float((seq_count * sequence_esti_factor) / 60), 2), 'Success',
                                           'Sequences Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                           'Sequences',
                                           seq_count,
                                           round(float((seq_count * sequence_esti_factor) / 60), 2), 'Failed',
                                           error_msg)

        elif str(ceecount).upper() == 'O':
            seq_est_factor_h = round((sequence_esti_factor * len(lists)) / 60, 2)
            # sub_objects_count.extend(
            #     ["Sequences", len(lists), (seq_est_factor * len(lists)), seq_est_factor_h])
            sub_objects_count.extend(
                ["Sequences", len(lists), seq_est_factor_h])
            statements_list.append(sub_objects_count)
    except Exception as seq:
        print('Sequences having issue ,', seq)


def check_constarints(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                      statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    # list and defination query for checkconstraina
    print('check constraint')
    oracle_data_check, orc_check_query_tag = xml_extractor(xmlpath, None,
                                                           parent_tag='OraclecheckconstrainsExtractorvalidation')
    list_query_oracle_check = orc_check_query_tag['check_constarintcountcheck'].replace(
        '@schemaname',
        cschema_type)

    # ca -- Sk 12/24
    # count_check_constr = oracle_connection(list_query_oracle_check, username, password, hostname, port, sid,
    #                                      cschema_type)
    count_check_constr = query_oracle_connection(list_query_oracle_check, oracle_singleconnection)
    check_count_bi = count_check_constr['COUNT'][0]

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

            # ca -- Sk 12/24
            # count_check_constr_mod = oracle_connection(query_modified, username, password, hostname, port, sid,
            #                                            cschema_type)
            count_check_constr_mod = query_oracle_connection(query_modified, oracle_singleconnection)

            check_count = count_check_constr_mod['COUNT(*)'][0]

        # sub_objects_count.extend(['Constarints', check_count, check_count * check_constarint_esti_factor,
        #                           round(float((check_count * check_constarint_esti_factor) / 60), 2)])
        sub_objects_count.extend(['Constarints', check_count,
                                  round(float((check_count * detail_check_const_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)

        if str(object_type).upper() == 'ALL':

            if check_count_bi > 0:
                Query_powrbi_report_chnaging = """
                                                    select count(1) from 
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
                                                    ) where CHECK_CONDITION not like '%IS NOT NULL' and table_name not like '%$%'
                                                    )
                                                    
                                                    
                                                    UNION
                                                    
                                                    (select 'alter table '||lower(SCHEMA_NAME)||'.'||lower(table_name)||' alter column '||REPLACE(CHECK_CONDITION,' IS ',' SET ')||'; ' AS ADDING_NOTNULLS
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
                                                    ) where CHECK_CONDITION like '%IS NOT NULL' and table_name not like '%$%'
                                                    )
                                                    ) res)
                        """

                Query_powrbi_report_chnaging = Query_powrbi_report_chnaging.replace('@schemaname',
                                                                                    str(cschema_type).upper())
                check_count_bi = \
                    query_oracle_connection(Query_powrbi_report_chnaging, oracle_singleconnection)['COUNT(1)'][0]

            appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type,
                                   'Check_Constraints',
                                   check_count_bi,
                                   round(float((check_count_bi * detail_check_const_esti_factor) / 60), 2), 'Success',
                                   'Check_Constraints Extracted Successfully')
    elif str(ceecount).upper() == 'O':
        check_count = count_check_constr['COUNT'][0]
        # sub_objects_count.extend(['Constarints', check_count, check_count * check_constarint_esti_factor,
        #                           round(float((check_count * check_constarint_esti_factor) / 60), 2)])
        sub_objects_count.extend(['Constarints', check_count,
                                  round(float((check_count * obj_check_const_esti_factor) / 60), 2)])
        statements_list.append(sub_objects_count)
    try:
        if int(check_count) != 0:
            list_query_oracle_check = check_constarinsdefination.replace('@schemaname', cschema_type)
            list_query_oracle_check1 = check_constarinsdefination1.replace('@schemaname', cschema_type)
            try:

                # ca -- Sk 12/24
                # def_orac_df = exe_oracle_connection(list_query_oracle_check, username, password, hostname, port, sid,
                #                                     cschema_type)
                def_orac_df = exe_oracle_connection(list_query_oracle_check, oracle_singleconnection)

            except Exception as e:
                def_orac_df = pd.DataFrame()
                logging.error('From Query getting issue %s :', e)
                print('error', e)
            if len(def_orac_df):
                # print(def_orac_df.columns)
                # query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = def_orac_df[def_orac_df.columns[0]].to_list()
                query_text = '\n'.join(query_text)
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                check_schemaname = """set search_path to @schemaname;""".replace('@schemaname', cschema_type.lower())
                query_text = check_schemaname + '\n' + query_text
                sql_path_data = db_dr_path + '/' + cschema_type.upper() + '/' + 'CHECKCONSTRAINS' + '/' + 'checkconstrains.sql'
                create_and_append_sqlfile(sql_path_data, query_text)

            try:
                def_orac_df = exe_oracle_connection(list_query_oracle_check1, oracle_singleconnection)  # ca -- sk 12/24
                # print(def_orac_df)
            except Exception as e:
                def_orac_df = pd.DataFrame()
                logging.error('From Query getting issue %s :', e)
                print('error', e)
            if len(def_orac_df):
                # print(def_orac_df.columns)
                # query_text = def_orac_df.loc[0][list(def_orac_df.columns)[0]]
                query_text = def_orac_df[def_orac_df.columns[0]].to_list()
                query_text = '\n'.join(query_text)
                if not query_text.strip().endswith(';'):
                    query_text = query_text + ";"
                query_text = str(query_text).replace('NONEDITIONABLE', '').replace('EDITIONABLE', '')
                check_schemaname = """set search_path to @schemaname;""".replace('@schemaname', cschema_type.lower())
                query_text = check_schemaname + '\n' + query_text
                sql_path_data = db_dr_path + '/' + cschema_type.upper() + '/' + 'CHECKCONSTRAINS' + '/' + 'foreignkey_checkconstrains.sql'
                create_and_append_sqlfile(sql_path_data, query_text)
    except Exception as cc:
        print('Check contsraint or Foreign key query got failed', cc)


def dblink_storage(cschema_type, oracle_singleconnection, statements_list, ceecount, app_data, iter,
                   projectid, connection_name, xmlpath, object_type):
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    schema_dblink_query = """select count(1) from dba_objects where object_type = 'DATABASE LINK' AND upper(owner) = '@schemaname'"""
    count_querys1 = schema_dblink_query
    obj_list1 = 'Dblink'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())

    data = get_count(oracle_singleconnection, count_query)

    if data:
        part1 = data[0][0]
        if str(ceecount).upper() == 'D':
            # esimate = round((estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend(
            #     [obj_list1, part1, part1 * dblink_estimate_factor,
            #      round(float((part1 * dblink_estimate_factor) / 60), 2)])
            sub_objects_count.extend(
                [obj_list1, part1,
                 round(float((part1 * dblink_estimate_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'DBLink',
                                       part1,
                                       round(float((part1 * dblink_estimate_factor) / 60), 2), 'Success',
                                       'DBlink Extracted Successfully')
        elif str(ceecount).upper() == 'O':
            esimate = round((dblink_estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend(
            #     [obj_list1, part1, part1 * dblink_estimate_factor, esimate])
            sub_objects_count.extend(
                [obj_list1, part1, esimate])
            statements_list.append(sub_objects_count)


def column(cschema_type, oracle_singleconnection, statements_list, ceecount, app_data, iter, projectid,
           connection_name, xmlpath, object_type):
    # c -- Sk 12/24
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    schema_column_query = """SELECT count(1) FROM dba_TAB_columns WHERE owner = '@schemaname'"""
    count_querys1 = schema_column_query
    obj_list1 = 'column'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())

    # cA --sk 12/24
    # data = get_count(oracle_data, count_query)
    data = get_count(oracle_singleconnection, count_query)

    if data:
        if str(ceecount).upper() == 'D':
            part1 = data[0][0]
            sub_objects_count.extend(
                [obj_list1, part1, round(float((part1 * detail_column_est_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Columns',
                                       part1,
                                       round(float((part1 * detail_column_est_factor) / 60), 2), 'Success',
                                       'Columns Extracted Successfully')


def partitions(cschema_type, oracle_singleconnection, statements_list, ceecount, app_data, iter,
               projectid, connection_name, xmlpath, object_type):
    # c -- sk 12/24
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    schema_partition_query = """SELECT count(1) FROM dba_TAB_PARTITIONS WHERE table_owner = '@schemaname'"""
    count_querys1 = schema_partition_query
    obj_list1 = 'Partitions'
    sub_objects_count = []
    count_query = count_querys1.replace('@schemaname', cschema_type.upper())

    # ca --Sk 12/24
    # data = get_count(oracle_data, count_query)
    data = get_count(oracle_singleconnection, count_query)

    if data:
        part1 = data[0][0]
        if str(ceecount).upper() == 'D':
            # sub_objects_count.extend(
            #     [obj_list1, part1, part1 * partition_esti_factor,
            #      round(float((part1 * partition_esti_factor) / 60), 2)])
            sub_objects_count.extend(
                [obj_list1, part1,
                 round(float((part1 * detail_parti_esti_factor) / 60), 2)])
            statements_list.append(sub_objects_count)
            if str(object_type).upper() == 'ALL':
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, 'Partitions',
                                       part1,
                                       round(float((part1 * detail_parti_esti_factor) / 60), 2), 'Success',
                                       'Partitions Extracted Successfully')


def job_program_schedule_codeobjects(cschema_type, oracle_singleconnection, statements_list, ceecount,
                                     app_data, iter, projectid, connection_name, xmlpath, object_type):
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    schema_job_query = """select count(1) from dba_objects where object_type = 'JOB' AND upper(owner) = '@schemaname'"""
    schema_schedule_query = """select count(1) from dba_objects where object_type = 'SCHEDULE' AND upper(owner) = '@schemaname'"""
    schema_program_query = """select count(1) from dba_objects where object_type = 'PROGRAM' AND upper(owner) = '@schemaname'"""
    count_querys1 = [schema_job_query, schema_schedule_query, schema_program_query]
    obj_list1 = ['Jobs', 'Schedule', 'Program']
    for i, j, k in zip(count_querys1, obj_list1, job_schedule_program_esti_factor):
        sub_objects_count = []
        count_query = i.replace('@schemaname', cschema_type.upper())

        data = get_count(oracle_singleconnection, count_query)  # -- Sk 12/24

        if data:
            part1 = data[0][0]
            estimate_factor = k
            if str(ceecount).upper() == 'D':
                esimate = round((estimate_factor * part1) / 60, 2)
                # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
                sub_objects_count.extend([j, part1, esimate])
                statements_list.append(sub_objects_count)
                if str(object_type).upper() == 'ALL':
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, j,
                                           part1,
                                           round(float((part1 * estimate_factor) / 60), 2), 'Success',
                                           str(j) + ' Extracted Successfully')
            elif str(ceecount).upper() == 'O':
                esimate = round((estimate_factor * part1) / 60, 2)
                # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
                sub_objects_count.extend([j, part1, esimate])
                statements_list.append(sub_objects_count)


def prim_forei(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
               statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    # if str(ceecount).upper() == 'O':
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

    prima_count_query_1 = """
    select count(1) from
                            (select 'ALTER TABLE '||lower(OWNER)||'.'||lower(table_name)||' ADD CONSTRAINT '||constraint_name||' PRIMARY KEY(' ||PKey||'); ' as adding_primary_keys,1 as sno from (
                            select OWNER,table_name,listagg(PKEY,',') within group (order by PKEY) as PKey,constraint_name from
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
                            -- group by dba_constraints.owner,dba_cons_columns.table_name,dba_constraints.constraint_name ,dba_cons_columns.column_name
                            order by
                            dba_cons_columns.table_name,
                            dba_cons_columns.column_name
                            )
                            group by OWNER,table_name,CONSTRAINT_NAME
                            )
                            )
    """
    prima_count_query_1 = prima_count_query_1.replace('@schemaname', cschema_type)
    data_1 = get_count(oracle_singleconnection, prima_count_query_1)
    data_1 = data_1[0][0]

    other_obj = ['Primary Keys', 'foreign keys']
    count_queries_for_pri_check = [prima_count_query, foreign_count_query]

    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    for i, j, k in zip(count_queries_for_pri_check, other_obj, primary_forign_esti_factor):
        sub_objects_count = []
        count_query = i.replace('@schemaname', cschema_type)

        data = get_count(oracle_singleconnection, count_query)
        if data:
            part1 = data[0][0]
            estimate_factor = k
            esimate = round((estimate_factor * part1) / 60, 2)
            # sub_objects_count.extend([j, part1, part1 * estimate_factor, esimate])
            sub_objects_count.extend([j, part1, esimate])
            statements_list.append(sub_objects_count)
            if j == 'Primary Keys':
                j = 'Primary_Keys'
            if j == 'foreign keys':
                j = 'Foreign_Keys'
            if str(object_type).upper() == 'ALL':

                if j == 'Primary_Keys':
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, j,
                                           data_1,
                                           round(float((data_1 * estimate_factor) / 60), 2), 'Success',
                                           str(j) + ' Extracted Successfully')
                else:
                    appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_data, cschema_type, j,
                                           part1,
                                           round(float((part1 * estimate_factor) / 60), 2), 'Success',
                                           str(j) + ' Extracted Successfully')


def job_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    job_data, job_list_tag = xml_extractor(xmlpath, None, parent_tag='JOB_SCHEDULE_PGM_Extractor')
    job_oracle = job_list_tag['JOBQUERY'].replace('@schemaname', cschema_type)
    job_query_oracle = str(job_oracle).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(job_query_oracle, oracle_singleconnection)

    job_data = orac_df[orac_df.columns[0]].values.tolist()
    for jo in job_data:
        storing_src_path = check_dir + '/' + 'JOB'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'job.sql', jo)


def schedule_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    schedule_data, schedule_list_tag = xml_extractor(xmlpath, None, parent_tag='JOB_SCHEDULE_PGM_Extractor')
    schedule_oracle = schedule_list_tag['SCHEDULEQUERY'].replace('@schemaname', cschema_type)
    schedule_query_oracle = str(schedule_oracle).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(schedule_query_oracle, oracle_singleconnection)

    schedule_job_data = orac_df[orac_df.columns[0]].values.tolist()
    for sch in schedule_job_data:
        storing_src_path = check_dir + '/' + 'SCHEDULE'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'schedule.sql', sch)


def program_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    pgm_data, pgm_list_tag = xml_extractor(xmlpath, None, parent_tag='JOB_SCHEDULE_PGM_Extractor')
    pgm_oracle = pgm_list_tag['PROGRAMQUERY'].replace('@schemaname', cschema_type)
    pgm_query_oracle = str(pgm_oracle).replace('@schemaname', cschema_type)

    orac_df = query_oracle_connection(pgm_query_oracle, oracle_singleconnection)

    pgm_job_data = orac_df[orac_df.columns[0]].values.tolist()
    for pg in pgm_job_data:
        storing_src_path = check_dir + '/' + 'PROGRAM'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'program.sql', pg)


def column_xtract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    col_data, col_list_tag = xml_extractor(xmlpath, None, parent_tag='COLUMN_PARTATION_Extractor')
    col_oracle = col_list_tag['COLUMNQUERY'].replace('@schemaname', cschema_type)
    col_query_oracle = str(col_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(col_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(col_query_oracle, oracle_singleconnection)

    column_data = orac_df[orac_df.columns[0]].values.tolist()
    for column in column_data:
        storing_src_path = check_dir + '/' + 'COLUMN'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'column.sql', column)


def data_in_gb(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    gb_data, gb_list_tag = xml_extractor(xmlpath, None, parent_tag='Oracleschema_size')
    gb_oracle = gb_list_tag['scema_length'].replace('@schemaname', cschema_type)
    gb_query_oracle = str(gb_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(gb_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(gb_query_oracle, oracle_singleconnection)

    gb_in_data = orac_df[orac_df.columns[0]].values.tolist()
    print(gb_in_data)
    for gb in gb_in_data:
        storing_src_path = check_dir + '/' + 'DATA_IN_GB'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'data_gb.sql', gb)


def dblink_xtract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    dblink_data, dblink_list_tag = xml_extractor(xmlpath, None, parent_tag='COLUMN_PARTATION_Extractor')
    db_oracle = dblink_list_tag['DBLINK'].replace('@schemaname', cschema_type)
    db_query_oracle = str(db_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(db_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(db_query_oracle, oracle_singleconnection)

    column_data = orac_df[orac_df.columns[0]].values.tolist()
    for column in column_data:
        storing_src_path = check_dir + '/' + 'DB_LINK'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'dblink.sql', column)


def partition_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    part_data, part_list_tag = xml_extractor(xmlpath, None, parent_tag='COLUMN_PARTATION_Extractor')
    part_oracle = part_list_tag['PARTIATIONQUERY'].replace('@schemaname', cschema_type)
    part_query_oracle = str(part_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(part_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(part_query_oracle, oracle_singleconnection)

    partition_data = orac_df[orac_df.columns[0]].values.tolist()
    for parti in partition_data:
        storing_src_path = check_dir + '/' + 'PARTATION'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'partiation.sql', parti)


def check_constraints_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    check_data, check_list_tag = xml_extractor(xmlpath, None, parent_tag='OraclecheckconstrainsExtractorvalidation')
    check_oracle = check_list_tag['check_constarintcountcheck'].replace('@schemaname', cschema_type)
    check_query_oracle = str(check_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(check_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(check_query_oracle, oracle_singleconnection)

    check_cons_data = orac_df[orac_df.columns[0]][0]
    if check_cons_data > 0:
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

        # orac_df = oracle_connection(query_modified, username, password, hostname, port, sid, cschema_type)
        orac_df = query_oracle_connection(query_modified, oracle_singleconnection)

        check_cons_data = orac_df['COUNT(*)'][0]
    storing_src_path = check_dir + '/' + 'CHECKCONSTRAINS'
    if os.path.isdir(storing_src_path):
        shutil.rmtree(storing_src_path)
    if not os.path.exists(storing_src_path):
        os.makedirs(storing_src_path)
        create_and_append_configfile(storing_src_path + '/' + 'checkconstrains.sql', check_cons_data)


def primary_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    prim_data, primlist_tag = xml_extractor(xmlpath, None, parent_tag='PRIMARY_FOREGIN_Extractor_CEE')
    prim_oracle = primlist_tag['PRIMARYQUERYCEE'].replace('@schemaname', cschema_type)
    prim_query_oracle = str(prim_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(prim_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(prim_query_oracle, oracle_singleconnection)

    prim_data = orac_df[orac_df.columns[0]].values.tolist()
    for prim in prim_data:
        storing_src_path = check_dir + '/' + 'PRIMARYKEY'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'primarykey.sql', prim)


def foreign_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath):
    fore_data, forelist_tag = xml_extractor(xmlpath, None, parent_tag='PRIMARY_FOREGIN_Extractor_CEE')
    fore_oracle = forelist_tag['FOREGINQUERYCEE'].replace('@schemaname', cschema_type)
    fore_query_oracle = str(fore_oracle).replace('@schemaname', cschema_type)

    # orac_df = oracle_connection(fore_query_oracle, username, password, hostname, port, sid, cschema_type)
    orac_df = query_oracle_connection(fore_query_oracle, oracle_singleconnection)

    fore_data = orac_df[orac_df.columns[0]].values.tolist()
    for fore in fore_data:
        storing_src_path = check_dir + '/' + 'FOREGINKEY'
        if os.path.isdir(storing_src_path):
            shutil.rmtree(storing_src_path)
        if not os.path.exists(storing_src_path):
            os.makedirs(storing_src_path)
            create_and_append_configfile(storing_src_path + '/' + 'foreginkey.sql', fore)


def trigger_data_excel(check_dir, oracle_singleconnection, cschema_type):
    pack_str = ''' 
    select TRIGGER_NAME from dba_triggers where owner='@schemaname'
    '''
    if not os.path.exists(check_dir + '/' + 'PERFORM_DATA'):
        os.makedirs(check_dir + '/' + 'PERFORM_DATA')
    pack_excel_path_trig = check_dir + '/' + 'PERFORM_DATA' + '/' + 'trigger_list' + '.xlsx'
    pack_str = pack_str.replace('@schemaname', str(cschema_type).upper())

    final_orac_df = query_oracle_connection(pack_str, oracle_singleconnection)

    final_orac_df.to_excel(pack_excel_path_trig, index=False)


def table_data_excel(check_dir, oracle_singleconnection, cschema_type):
    tab_str = ''' 
    select DISTINCT OBJECT_NAME from dba_objects a where NOT exists (select 1 from dba_mviews MV  where
            MV.MVIEW_NAME=a.object_name AND MV.OWNER=upper('@schemaname')) AND A.OWNER=upper('@schemaname') and OBJECT_NAME not like '%$%' AND A.OBJECT_TYPE='TABLE' and TEMPORARY='N' ORDER BY 1
    '''
    if not os.path.exists(check_dir + '/' + 'PERFORM_DATA'):
        os.makedirs(check_dir + '/' + 'PERFORM_DATA')
    pack_excel_path_trig = check_dir + '/' + 'PERFORM_DATA' + '/' + 'tables_list' + '.xlsx'
    pack_str = tab_str.replace('@schemaname', str(cschema_type).upper())

    table_orac_df = query_oracle_connection(pack_str, oracle_singleconnection)

    table_orac_df.to_excel(pack_excel_path_trig, index=False)


def storage_utility(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                    statements_list, ceecount, app_data, iter, projectid, connection_name, object_type):
    datatypes(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
              statements_list, object_type)  # for datatype extraction

    tables(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
           statements_list, ceecount, app_data, iter, projectid, connection_name, object_type)  # for Tables extraction

    if str(ceecount).upper() == 'D':
        column(cschema_type, oracle_singleconnection, statements_list,
               ceecount, app_data, iter, projectid, connection_name, xmlpath, object_type)  # for extracting columns
        partitions(cschema_type, oracle_singleconnection, statements_list,
                   ceecount, app_data, iter, projectid, connection_name, xmlpath,
                   object_type)  # for extracting partitions

    check_constarints(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                      statements_list, ceecount, app_data, iter, projectid,
                      connection_name, object_type)  # for check constraints extraction

    # if str(ceecount).upper() == 'O':
    prim_forei(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
               statements_list, ceecount, app_data, iter, projectid, connection_name, object_type)

    indexes(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
            statements_list, ceecount, app_data, iter, projectid, connection_name,
            object_type)  # for indexes extraction

    sequences(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
              statements_list, ceecount, app_data, iter, projectid, connection_name,
              object_type)  # for sequences extraction
    #
    synonyms(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
             statements_list, ceecount, app_data, iter, projectid, connection_name,
             object_type)  # for synonyms extraction

    views_mviews(cschema_type, check_dir, oracle_singleconnection, xmlpath,
                 db_dr_path,
                 statements_list, ceecount, app_data, iter, projectid,
                 connection_name, object_type)  # for views and mviews extraction

    # for rerun

    column_xtract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    partition_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    check_constraints_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    primary_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    foreign_extract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    data_in_gb(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    ty_count, ty_msg, total_estimation_types, ty_lists = types(cschema_type, check_dir, oracle_singleconnection,
                                                               xmlpath, db_dr_path,
                                                               statements_list, ceecount, app_data, iter, projectid,
                                                               connection_name, object_type)  # for types extraction

    dblink_storage(cschema_type, oracle_singleconnection,
                   statements_list, ceecount, app_data, iter, projectid, connection_name,
                   xmlpath, object_type)  # for counting dblink
    global_temp_tables(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                       statements_list, ceecount, app_data, iter, projectid,
                       connection_name, object_type)  # For global temporary tables
    package_data(check_dir, oracle_singleconnection, cschema_type, object_type)
    return ty_count, ty_msg, total_estimation_types, ty_lists


def codeobjects_utility(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                        statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
                        all_dataobjectsnew_list, app_data, iter,
                        projectid, connection_name,
                        object_type, total_inclusive_dict, sql_exclusive, all_sheet_new, other_exclusive_data):
    all_codeobjects_list, all_dataobjectsnew_list, pack_count, pack_msg, whole_pack_est, pack_lists, all_sheet_new = packages(
        cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
        all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type, total_inclusive_dict,
        sql_exclusive, all_sheet_new, oracle_singleconnection, check_dir, other_exclusive_data)

    all_codeobjects_list, all_dataobjectsnew_list, proc_count, proc_msg, whole_proc_est, proc_list, all_sheet_new = procedures(
        cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
        all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type,
        total_inclusive_dict, sql_exclusive, all_sheet_new, oracle_singleconnection,
        check_dir, other_exclusive_data)  # for procedures extraction

    all_codeobjects_list, all_dataobjectsnew_list, func_count, func_msg, whole_func_est, func_list, all_sheet_new = functions(
        cschema_type, xmlpath, statements_list, ceecount, all_codeobjects_list, all_codeobjectsnew_list,
        all_dataobjectsnew_list, app_data, iter, projectid, connection_name, object_type, total_inclusive_dict,
        sql_exclusive, all_sheet_new, oracle_singleconnection, check_dir,
        other_exclusive_data)  # for functions extraction

    all_codeobjects_list = triggers(cschema_type, check_dir, oracle_singleconnection, xmlpath,
                                    db_dr_path,
                                    statements_list, ceecount, all_codeobjects_list, app_data, iter, projectid,
                                    connection_name, object_type, total_inclusive_dict)  # for triggers extraction

    job_program_schedule_codeobjects(cschema_type, oracle_singleconnection, statements_list,
                                     ceecount, app_data, iter, projectid, connection_name,
                                     xmlpath, object_type)  # for job, program, schedule codeonjects
    package_data(check_dir, oracle_singleconnection, cschema_type, object_type)

    dblink_xtract(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    # rerun

    program_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    schedule_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    job_codeobjects(check_dir, oracle_singleconnection, cschema_type, xmlpath)

    # for triggers
    trigger_data_excel(check_dir, oracle_singleconnection, cschema_type)
    table_data_excel(check_dir, oracle_singleconnection, cschema_type)

    whole_pack_proc_func_msg = proc_msg + '\n' + func_msg + '\n' + pack_msg
    whole_pack_proc_func = proc_count + pack_count + func_count
    whole_pack_proc_func_est = whole_proc_est + whole_pack_est + whole_func_est
    whole_pack_proc_func_list_count = pack_lists + func_list + proc_list
    return all_codeobjects_list, all_dataobjectsnew_list, all_sheet_new, whole_pack_proc_func, whole_pack_proc_func_est, whole_pack_proc_func_msg, whole_pack_proc_func_list_count


# for counting schedule, job, program
def CEE(tokendata, cschema_type, folder_path, xmlpath, projectid, connection_type, connection_name,
        object_type, db_dr_path, ceecount, exclusive, total_inclusive_dict, sql_exclusive, other_exclusive_data):
    # print(total_inclusive_dict)
    #
    # print(sql_exclusive)
    #
    # print(other_exclusive_data)

    # print(total_inclusive_dict)
    # Set up for the log file location
    qmigrationlogfilelocation = qmigration_Xtractor(xmlpath, parent_tag='qmig_logs')
    qmigration_log = qmigrationlogfilelocation.replace("\n", "").replace(' ', '')
    qmigrator_path = path + "/" + qmigration_log
    if not os.path.exists(qmigrator_path):
        os.makedirs(qmigrator_path)
    if not os.path.exists(qmigrator_path + '/' + str(cschema_type).upper()):
        os.makedirs(qmigrator_path + '/' + str(cschema_type).upper())
    logging.getLogger().handlers.clear()
    logname = os.path.basename(__file__)
    logger = logging.getLogger(logname)
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s -' + logname + ' - %(levelname)s - %(message)s',
                        filename=qmigrator_path + '/' + str(cschema_type).upper() + '/' + 'utiltiylog.log',
                        filemode='a+')
    logging.info("Oracle schema name : %s", cschema_type)
    # print(tokendata)
    cschema_type = str(cschema_type).upper()
    check_dir = folder_path + '/' + cschema_type

    if not os.path.exists(check_dir):
        os.makedirs(check_dir)
    meta_dir = check_dir + '/' + 'MetaData'
    if os.path.isdir(meta_dir):
        shutil.rmtree(meta_dir)
    if not os.path.exists(meta_dir):
        os.makedirs(meta_dir)

    # folder structure for  Source DB, if not present craeting one as per the pattern

    # callin api
    username, password, port, hostname, sid = calling_api(tokendata, projectid, connection_type, connection_name)
    app_data = appli_db_connection_call(tokendata, projectid, connection_type, connection_name)

    app_single_connection = applica_db_connection_details(xmlpath, app_data)

    # database connection check
    oracle_try_data, orc_try_tag = xml_extractor(xmlpath, None, parent_tag='TryexceptionExtractor')
    try_oracle = orc_try_tag['TRYQUERY']
    query_oracle = str(try_oracle)

    # Commented By Sk 12/24
    # orac_df = oracle_connection(query_oracle, username, password, hostname, port, sid, cschema_type)
    # object_try = orac_df[orac_df.columns[0]].values.tolist()[0]

    # Added By SK 12/24 Maijor purpose is to connect to the database only one time
    oracle_singleconnection = single_oracle_connection(username, password, hostname, port, sid)
    orac_df = query_oracle_connection(query_oracle, oracle_singleconnection)
    object_try = orac_df[orac_df.columns[0]].values.tolist()[0]

    if int(object_try) != 1:
        raise Exception('Database connection Not Established please check the connection and try again')

    # calling iteration
    iteration_id_insert = appli_db_get_iteration(xmlpath, projectid, app_single_connection, cschema_type)
    if iteration_id_insert:
        iter = list(map(list, zip(*iteration_id_insert)))
        iter = iter[0][0]
    else:
        iter = 0
    if str(ceecount).upper() == 'D':
        if str(object_type).upper() == 'STORAGE':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'SO_Detail_Count_QMigrator_Estimate.xlsx'
        elif str(object_type).upper() == 'CODEMIGRATION':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'CO_Detail_Count_QMigrator_Estimate.xlsx'
        elif str(object_type).upper() == 'ALL':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'ALL_Detail_Count_QMigrator_Estimate.xlsx'
    elif str(ceecount).upper() == 'O':
        cdc_excel_all = check_dir + '/' + str(
            cschema_type).upper() + '_' + 'Object_Count_QMigrator_Estimate.xlsx'
        if str(object_type).upper() == 'STORAGE':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'SO_Object_Count_QMigrator_Estimate.xlsx'
        elif str(object_type).upper() == 'CODEMIGRATION':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'CO_Object_Count_QMigrator_Estimate.xlsx'
        elif str(object_type).upper() == 'ALL':
            cdc_excel_all = check_dir + '/' + str(
                cschema_type).upper() + '_' + 'ALL_Object_Count_QMigrator_Estimate.xlsx'
    if not os.path.exists(check_dir):
        os.makedirs(check_dir)

    statements_list = []
    all_codeobjects_list = []
    all_codeobjectsnew_list = []
    all_dataobjectsnew_list = []
    all_sheet_new = []

    procedure_estimation_hours = []
    function_estimation_hrs = []
    package_estimation_hrs = []
    trigger_estimation_hrs = []

    if str(object_type).upper() == 'STORAGE':
        if os.path.isdir(check_dir + '/' + 'INDEXES'):
            shutil.rmtree(check_dir + '/' + 'INDEXES')
        if not os.path.exists(check_dir + '/' + 'INDEXES'):
            os.makedirs(check_dir + '/' + 'INDEXES')
        if os.path.isdir(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS'):
            shutil.rmtree(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS')
        if not os.path.exists(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS'):
            os.makedirs(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS')

        if os.path.isdir(check_dir + '/' + 'MATERIALIZED_VIEWS'):
            shutil.rmtree(check_dir + '/' + 'MATERIALIZED_VIEWS')
        if not os.path.exists(check_dir + '/' + 'MATERIALIZED_VIEWS'):
            os.makedirs(check_dir + '/' + 'MATERIALIZED_VIEWS')

        if os.path.isdir(check_dir + '/' + 'SEQUENCES'):
            shutil.rmtree(check_dir + '/' + 'SEQUENCES')
        if not os.path.exists(check_dir + '/' + 'SEQUENCES'):
            os.makedirs(check_dir + '/' + 'SEQUENCES')

        if os.path.isdir(check_dir + '/' + 'SYNONYMS'):
            shutil.rmtree(check_dir + '/' + 'SYNONYMS')
        if not os.path.exists(check_dir + '/' + 'SYNONYMS'):
            os.makedirs(check_dir + '/' + 'SYNONYMS')

        if os.path.isdir(check_dir + '/' + 'TABLES'):
            shutil.rmtree(check_dir + '/' + 'TABLES')
        if not os.path.exists(check_dir + '/' + 'TABLES'):
            os.makedirs(check_dir + '/' + 'TABLES')

        if os.path.isdir(check_dir + '/' + 'VIEWS'):
            shutil.rmtree(check_dir + '/' + 'VIEWS')
        if not os.path.exists(check_dir + '/' + 'VIEWS'):
            os.makedirs(check_dir + '/' + 'VIEWS')

        if os.path.isdir(check_dir + '/' + 'DATATYPES'):
            shutil.rmtree(check_dir + '/' + 'DATATYPES')
        if not os.path.exists(check_dir + '/' + 'DATATYPES'):
            os.makedirs(check_dir + '/' + 'DATATYPES')

        if os.path.isdir(check_dir + '/' + 'TYPES'):
            shutil.rmtree(check_dir + '/' + 'TYPES')
        if not os.path.exists(check_dir + '/' + 'TYPES'):
            os.makedirs(check_dir + '/' + 'TYPES')

        storage_utility(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                        statements_list, ceecount, app_single_connection, iter, projectid,
                        connection_name, object_type)
        # calling meta data excel file
        metadata_excel(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                       meta_dir,
                       object_type)


    elif str(object_type).upper() == 'CODEMIGRATION':
        if os.path.isdir(check_dir + '/' + 'FUNCTIONS'):
            shutil.rmtree(check_dir + '/' + 'FUNCTIONS')
        if not os.path.exists(check_dir + '/' + 'FUNCTIONS'):
            os.makedirs(check_dir + '/' + 'FUNCTIONS')

        if os.path.isdir(check_dir + '/' + 'PACKAGES'):
            shutil.rmtree(check_dir + '/' + 'PACKAGES')
        if not os.path.exists(check_dir + '/' + 'PACKAGES'):
            os.makedirs(check_dir + '/' + 'PACKAGES')

        if os.path.isdir(check_dir + '/' + 'PROCEDURES'):
            shutil.rmtree(check_dir + '/' + 'PROCEDURES')
        if not os.path.exists(check_dir + '/' + 'PROCEDURES'):
            os.makedirs(check_dir + '/' + 'PROCEDURES')

        if os.path.isdir(check_dir + '/' + 'TRIGGERS'):
            shutil.rmtree(check_dir + '/' + 'TRIGGERS')
        if not os.path.exists(check_dir + '/' + 'TRIGGERS'):
            os.makedirs(check_dir + '/' + 'TRIGGERS')

        codeobjects_utility(
            cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
            statements_list, ceecount, all_codeobjects_list, exclusive, all_dataobjectsnew_list,
            app_single_connection,
            iter, projectid,
            connection_name,
            object_type, total_inclusive_dict, sql_exclusive, all_sheet_new, other_exclusive_data)
        # calling meta data excel file
        metadata_excel(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                       meta_dir,
                       object_type)

    elif str(object_type).upper() == 'ALL':
        if os.path.isdir(check_dir + '/' + 'INDEXES'):
            shutil.rmtree(check_dir + '/' + 'INDEXES')
        if not os.path.exists(check_dir + '/' + 'INDEXES'):
            os.makedirs(check_dir + '/' + 'INDEXES')
        if os.path.isdir(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS'):
            shutil.rmtree(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS')
        if not os.path.exists(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS'):
            os.makedirs(db_dr_path + '/' + cschema_type + '/' + 'CHECKCONSTRAINS')

        if os.path.isdir(check_dir + '/' + 'MATERIALIZED_VIEWS'):
            shutil.rmtree(check_dir + '/' + 'MATERIALIZED_VIEWS')
        if not os.path.exists(check_dir + '/' + 'MATERIALIZED_VIEWS'):
            os.makedirs(check_dir + '/' + 'MATERIALIZED_VIEWS')

        if os.path.isdir(check_dir + '/' + 'SEQUENCES'):
            shutil.rmtree(check_dir + '/' + 'SEQUENCES')
        if not os.path.exists(check_dir + '/' + 'SEQUENCES'):
            os.makedirs(check_dir + '/' + 'SEQUENCES')

        if os.path.isdir(check_dir + '/' + 'SYNONYMS'):
            shutil.rmtree(check_dir + '/' + 'SYNONYMS')
        if not os.path.exists(check_dir + '/' + 'SYNONYMS'):
            os.makedirs(check_dir + '/' + 'SYNONYMS')

        if os.path.isdir(check_dir + '/' + 'TABLES'):
            shutil.rmtree(check_dir + '/' + 'TABLES')
        if not os.path.exists(check_dir + '/' + 'TABLES'):
            os.makedirs(check_dir + '/' + 'TABLES')

        if os.path.isdir(check_dir + '/' + 'VIEWS'):
            shutil.rmtree(check_dir + '/' + 'VIEWS')
        if not os.path.exists(check_dir + '/' + 'VIEWS'):
            os.makedirs(check_dir + '/' + 'VIEWS')

        if os.path.isdir(check_dir + '/' + 'DATATYPES'):
            shutil.rmtree(check_dir + '/' + 'DATATYPES')
        if not os.path.exists(check_dir + '/' + 'DATATYPES'):
            os.makedirs(check_dir + '/' + 'DATATYPES')

        if os.path.isdir(check_dir + '/' + 'FUNCTIONS'):
            shutil.rmtree(check_dir + '/' + 'FUNCTIONS')
        if not os.path.exists(check_dir + '/' + 'FUNCTIONS'):
            os.makedirs(check_dir + '/' + 'FUNCTIONS')

        if os.path.isdir(check_dir + '/' + 'PACKAGES'):
            shutil.rmtree(check_dir + '/' + 'PACKAGES')
        if not os.path.exists(check_dir + '/' + 'PACKAGES'):
            os.makedirs(check_dir + '/' + 'PACKAGES')

        if os.path.isdir(check_dir + '/' + 'PROCEDURES'):
            shutil.rmtree(check_dir + '/' + 'PROCEDURES')
        if not os.path.exists(check_dir + '/' + 'PROCEDURES'):
            os.makedirs(check_dir + '/' + 'PROCEDURES')

        if os.path.isdir(check_dir + '/' + 'TYPES'):
            shutil.rmtree(check_dir + '/' + 'TYPES')
        if not os.path.exists(check_dir + '/' + 'TYPES'):
            os.makedirs(check_dir + '/' + 'TYPES')

        if os.path.isdir(check_dir + '/' + 'TRIGGERS'):
            shutil.rmtree(check_dir + '/' + 'TRIGGERS')
        if not os.path.exists(check_dir + '/' + 'TRIGGERS'):
            os.makedirs(check_dir + '/' + 'TRIGGERS')

        code_obj_msg = ''
        code_obj_total_count = 0
        code_obj_total_est = 0
        ty_count, ty_msg, total_estimation_types, ty_lists = storage_utility(cschema_type, check_dir,
                                                                             oracle_singleconnection, xmlpath,
                                                                             db_dr_path,
                                                                             statements_list, ceecount,
                                                                             app_single_connection,
                                                                             iter,
                                                                             projectid,
                                                                             connection_name, object_type)
        all_codeobjects_list, all_dataobjectsnew_list, all_sheet_new, whole_pack_proc_func_count, whole_pack_proc_func_est, whole_pack_proc_func_msg, whole_pack_proc_func_list_count = codeobjects_utility(
            cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
            statements_list, ceecount, all_codeobjects_list, exclusive,
            all_dataobjectsnew_list, app_single_connection, iter,
            projectid, connection_name,
            object_type, total_inclusive_dict, sql_exclusive, all_sheet_new, other_exclusive_data)

        code_obj_msg = ty_msg + '\n' + whole_pack_proc_func_msg
        code_obj_total_count = ty_count + whole_pack_proc_func_count
        code_obj_total_est = total_estimation_types + whole_pack_proc_func_est
        code_obj_total_listcount = ty_lists + code_obj_total_count
        # inserting code objects for powerbi report
        if str(object_type).upper() == 'ALL':
            if code_obj_total_listcount == code_obj_total_count:
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_single_connection, cschema_type,
                                       'Code_Objects',
                                       code_obj_total_count,
                                       code_obj_total_est, 'Success',
                                       'Code_Objects Extracted Successfully')
            else:
                appli_db_powerbireport(xmlpath, projectid, connection_name, iter, app_single_connection, cschema_type,
                                       'Code_Objects',
                                       code_obj_total_count,
                                       code_obj_total_est, 'Failed',
                                       code_obj_msg)

        # calling meta data excel file
        metadata_excel(cschema_type, check_dir, oracle_singleconnection, xmlpath, db_dr_path,
                       meta_dir,
                       object_type)

    # ADDING ONE MORE PARAMETER "TESTING" in OBJECT TYPE,
    # OBJECT COUNT FOR Testing => SUm of OBJECTCOUNT(PACKAGES,FUNCTIONS,PROCEDURES, TRIGGERS)
    # DETAIL Count for Testing => SUm of DETAILCOUNT(PACKAGES,FUNCTIONS,PROCEDURES, TRIGGERS)
    # ESTIMATION FACTOR =200 Constant
    # ESTIMATION HOUR =  (ESTIMATION FACTOR * OBJECT COUNT)/2-SK6/16

    # # total_code_pfp = 0
    for codtype in all_codeobjects_list:
        if codtype[1] == 'Procedure':
            procedure_estimation_hours.append(codtype[7])
    total_code_proc = sum(procedure_estimation_hours) / 60

    for codfun in all_codeobjects_list:
        if codfun[1] == 'Function':
            function_estimation_hrs.append(codfun[7])
    total_code_func = sum(function_estimation_hrs) / 60
    # total_code_func = round(sum(function_estimation_hrs)/60)

    for codpack in all_codeobjects_list:
        if codpack[1] == 'Package':
            package_estimation_hrs.append(codpack[7])
    total_code_pack = sum(package_estimation_hrs) / 60

    for codtrig in all_codeobjects_list:
        if codtrig[1] == 'Triggers':
            trigger_estimation_hrs.append(codtrig[7])
    total_code_trigg = sum(trigger_estimation_hrs) / 60

    DATA_COUNT = []
    # DATA_DETAIL_COUNT = []
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
        # Testing_Detail_Count = sum(DATA_DETAIL_COUNT)
        Testing_Estimation_Factor = 1
        Testing_estimate_min = Testing_Estimation_Factor * Testing_Count
        try:
            Testing_Estimation_Hours = round((Testing_Estimation_Factor * Testing_Count) / 60)
            total_testing_factor = int(Testing_Estimation_Hours) / Testing_Count
        except:
            total_testing_factor = 0

        proc_count_power_bi_est = 0
        pack_count_power_bi_est = 0
        func_count_power_bi_est = 0
        trigg_count_power_bi_est = 0

        for i in statements_list:
            sub_objects_count = []
            if ('Packages' in i) or ('Procedures' in i) or ('Functions' in i) or ('Triggers' in i):
                obj_count = i[1] * total_testing_factor
                obj_count = math.ceil(obj_count)
                obj_hrs = round(float(i[2] + obj_count), 2)
                if 'Procedures' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_proc])
                    proc_count_power_bi = i[1]
                    proc_count_power_bi_est = total_code_proc
                    powerbi_report(tokendata, projectid, connection_type, connection_name, iter, cschema_type, xmlpath,
                                   app_single_connection, total_code_proc, 'Procedures')
                elif 'Functions' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_func])
                    func_count_power_bi = i[1]
                    func_count_power_bi_est = total_code_func
                    powerbi_report(tokendata, projectid, connection_type, connection_name, iter, cschema_type, xmlpath,
                                   app_single_connection, total_code_func, 'Functions')
                elif 'Packages' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_pack])
                    pack_count_power_bi = i[1]
                    pack_count_power_bi_est = total_code_pack
                    powerbi_report(tokendata, projectid, connection_type, connection_name, iter, cschema_type, xmlpath,
                                   app_single_connection, total_code_pack, 'Packages')
                elif 'Triggers' in i:
                    sub_objects_count.extend([i[0], i[1], total_code_trigg])
                    trigg_count_power_bi = i[1]
                    trigg_count_power_bi_est = total_code_trigg
                    powerbi_report(tokendata, projectid, connection_type, connection_name, iter, cschema_type, xmlpath,
                                   app_single_connection, total_code_trigg, 'Triggers')
                total_statements_list.append(sub_objects_count)
            else:
                sub_objects_count.extend([i[0], i[1], i[2]])
                total_statements_list.append(sub_objects_count)

        total_co_est_bi = pack_count_power_bi_est + proc_count_power_bi_est + trigg_count_power_bi_est + func_count_power_bi_est

        powerbi_report(tokendata, projectid, connection_type, connection_name, iter, cschema_type, xmlpath,
                       app_single_connection, total_co_est_bi, 'Code_Objects')
    else:
        for i in statements_list:
            sub_objects_count = []
            sub_objects_count.extend([i[0], i[1], i[2]])
            total_statements_list.append(sub_objects_count)

    all_count_df = pd.DataFrame()
    all_codeobjects_df = pd.DataFrame()

    if str(ceecount).upper() == 'D':
        all_count_df = pd.DataFrame(total_statements_list,
                                    columns=["Object Type", "Detail Count",
                                             "Estimate(in Hours)"])
        all_codeobjects_df = pd.DataFrame(all_codeobjects_list,
                                          columns=["Code Object Name", "Type", "Lines", "Statements",
                                                   "Identifiers", "Total Indentifiers", "Varyng Factor", "Total Mins"])

        all_sheet_cnt = pd.DataFrame()
        data_final_sheet3 = []
        dict_measure = {}
        for data_str in all_sheet_new:
            for mes in data_str:
                if mes not in dict_measure:
                    dict_measure[mes] = [data_str[mes]]
                else:
                    dict_measure[mes].append(data_str[mes])

        for counts, items_1 in dict_measure.items():
            temp_1 = sum(items_1)
            dict_measure[counts] = temp_1

        keys_data1 = list(dict_measure.keys())
        keys_data2 = list(dict_measure.values())

        all_sheet_cnt = pd.DataFrame({'New Measures': keys_data1, 'Count': keys_data2})


    elif str(ceecount).upper() == 'O':
        all_count_df = pd.DataFrame(total_statements_list,
                                    columns=["Object Type", "Object Count",
                                             "Estimate(in Hours)"])

    est_total_counts = all_count_df["Estimate(in Hours)"].values.tolist()
    # co_hours = all_codeobjects_df["Hours"].values.tolist()
    co_lines = all_codeobjects_df["Lines"].values.tolist()
    co_ststements = all_codeobjects_df["Statements"].values.tolist()
    co_total_identifiers = all_codeobjects_df["Total Indentifiers"].values.tolist()
    co_five_fact = all_codeobjects_df["Varyng Factor"].values.tolist()
    co_ten_fact = all_codeobjects_df["Total Mins"].values.tolist()
    # sum_co_hours = sum(co_hours)
    sum_co_lines = sum(co_lines)
    sum_co_statements = sum(co_ststements)
    sum_co_total_identifiers = sum(co_total_identifiers)
    sum_co_five_fact = sum(co_five_fact)
    sum_co_ten_fact = sum(co_ten_fact)
    new_co_row1 = ['', 'Total', sum_co_lines, sum_co_statements, '', sum_co_total_identifiers,
                   sum_co_five_fact, sum_co_ten_fact]
    all_codeobjects_df.loc[len(all_codeobjects_df.index)] = new_co_row1
    cal_sum_tot_iden = round((sum_co_total_identifiers) / 60, 2)
    cal_five_fact = round((sum_co_five_fact) / 60, 2)
    cal_ten_fact = round((sum_co_ten_fact) / 60, 2)
    new_co_row2 = ['', '', '', '', 'Projected Hours', cal_sum_tot_iden, cal_five_fact, cal_ten_fact]
    all_codeobjects_df.loc[len(all_codeobjects_df.index)] = new_co_row2

    total_count = 0
    for i in est_total_counts:
        total_count = i + total_count
    total_count = round(float(total_count), 2)
    new_row1 = ["Projected Hours", '', total_count]
    # for project hours
    # hours_cal = round(math.ceil(total_count / 40), 0)
    # new_row2 = ["Project Duration in weeks", '', '', hours_cal]
    #
    # # appending two rows in the existing all_count_df
    all_count_df.loc[len(all_count_df.index)] = new_row1
    # all_count_df.loc[len(all_count_df.index)] = new_row2
    # to get schema length
    orata, orc_scehmequery_tag = xml_extractor(xmlpath, None, parent_tag='Oracleschema_size')
    oracle_schema_length = orc_scehmequery_tag['scema_length'].replace('@schemaname', cschema_type)
    # oracle_data = {'host': hostname, "port": port, "sid": sid, "name": username, "password": password}

    # length_schema = get_count(oracle_data, oracle_schema_length)
    length_schema = get_count(oracle_singleconnection, oracle_schema_length)

    sub_objects_count = []
    type_obj = 'Data (in GB)'
    if str(ceecount).upper() == 'O':
        cnt = length_schema[0][0]
    else:
        cnt = length_schema[0][0]
    length_schema_factor = 60
    esimate_h = round((cnt * length_schema_factor) / 60, 2)
    if esimate_h <= 100:
        # sub_objects_count.extend([type_obj, cnt, length_schema_factor, 100])
        sub_objects_count.extend([type_obj, cnt, ''])
    else:
        # sub_objects_count.extend([type_obj, cnt, length_schema_factor, esimate_h])
        sub_objects_count.extend([type_obj, cnt, ''])

    all_count_df.loc[len(all_count_df.index)] = sub_objects_count
    with pd.ExcelWriter(cdc_excel_all) as writer1:
        all_count_df.to_excel(writer1, sheet_name='Estimate', index=False)
        all_codeobjects_df.to_excel(writer1, sheet_name='Detailed Code Object', index=False)
        all_sheet_cnt.to_excel(writer1, sheet_name='New Identifiers', index=False)
    writer1.save()


def powerbi_report(token_data, pid, conn_type, conn_name, iter, schema, xmlpath, connection, hours, objtype):
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        power_bi_update_query = """
        update prj_assessments_progress set estimation_hours={0} where iteration={1} and project_id={2} and schemaname='{3}' and objecttype='{4}'
        """.format(hours, iter, pid, schema, objtype)

        cursor.execute(power_bi_update_query)
    except Exception as err:
        print("error", err)
    finally:
        connection.commit()
        cursor.close()
