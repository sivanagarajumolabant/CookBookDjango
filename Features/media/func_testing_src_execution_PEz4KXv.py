from datetime import datetime, date
import time
# import datetime
import re
from authentication import decrypt_application_details, decryptstr, decrypt_source_details,logging_db
from qmig_config import *
import json
import pandas as pd
import os
import logging

path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
dir = os.listdir(path)
dir = sorted(dir)
find_db = dir.index('DB')
path_DB = path + '/' + dir[find_db]
DB_dirs = os.listdir(path_DB)
find_preO2 = DB_dirs.index('Pre_O2P')
path_PreO2p = path_DB + '/' + DB_dirs[find_preO2]


# testid_list = [81]


def source_conversion_header(string):
    string = re.sub(' +', ' ', string)
    string = "".join(string.splitlines())
    return string


def appli_db_connection_call(tokendata, project_id, conn_name, conn_type):
    ''' getting the application details from application database using api'''
    app_data = decrypt_application_details(tokendata, project_id, conn_type, conn_name)
    return app_data


def source_db_connection_call(projectid, token_data, conn_name, conn_type):
    # calling get connection details for source database from api
    oracle_data_source= decrypt_source_details(token_data, projectid, conn_type, conn_name)
    return oracle_data_source


def appl_conn_select_header(id,connection):
    ''' In this function we are connecting to application database and
    trying to execute the query in a header table and storing the list of queries in data variable as tuple and returning it.
    we need to connect to oracle database and execute the list of queries'''
    # print(id)
    logging.info("connecting to oracle database and executing the list of queries")
    # app_data = appli_db_connection_call(token_data, projectid, conn_name, conn_type)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info("Executing the query in a header table and storing the list of queries in data variable")
        cursor.execute("select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
                       [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error %s",err)
        # data = None
    finally:
        cursor.close()
    return data


def header_table(id,app_connection,orcl_connection):
    '''we will get the required paramters and passing it to update function'''
    logging.info(" will get the required paramters and passing it to update function")
    header_data = appl_conn_select_header(id, app_connection)
    if header_data:
        testcasename = header_data[0][0]
        header_exe_type = header_data[0][1]
        header_data_query = header_data[0][2]
        start_time = datetime.now()
        # print(data[0][0])
        data = header_data_query
        if 'exec' in data.lower():
            data_changed = str(data).replace('exec', 'call').replace(';', '').replace('EXEC', 'call')
        else:
            data_changed = str(data)
        execution_status = source_conn_select_header(data_changed,orcl_connection)
        end_time = datetime.now()
        # total_time = total_time_query(xmlpath, str(start_time), str(end_time), projectid, token_data, conn_name,
        #                               conn_type)
        # taking the total execution time
        total_time = end_time - start_time
        # print(total_time)
        exedate = datetime.now().astimezone()
        try:
            logging.info("Updating the execution_status,total_time,executiondate into the application data base")
            appl_update_query_header(execution_status, str(total_time), exedate, id,app_connection)
            # exe_status = 'Success'
        except Exception as e:
            # exe_status = 'Failed'
            print("Error:", e)
            logging.error("Error %s",e)

        return testcasename, header_exe_type, data, execution_status
    else:
        print('Data Not Found for application db header select records with this', id)
        logging.info('Data Not Found for application db header select records with this %s', id)
        return None, None, None, None


def source_conn_select_header(exe_query,connection):
    '''we need to connect with oracle database to execute the target select query'''
    # oracle_data = source_db_connection_call(projectid, token_data, conn_name, conn_type)
    # print(exe_query)
    # connection = source_connection_details(oracle_data)
    # print("connected to the Oracle Database")
    # logging.info('connected to the Oracle Database')
    cursor = connection.cursor()
    try:
        # print(exe_query)
        exe_query = exe_query
        # exe_query = str(exe_query).replace(';','')
        # print(exe_query)
        cursor.execute(exe_query)
        print("Executed target query")
        status = 'Success'
        logging.info("Target query executed sucessfully")
    except Exception as err:
        print(err)
        status = "Failed"
        logging.error('Target query executed failed %s',err)
    finally:
        cursor.close()
    return status


def appl_update_query_header(execution_status, total_time, exedate, id,connection):
    ''' In this function we are updating the required columns'''
    # app_data = appli_db_connection_call(token_data, projectid, conn_type, conn_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # print(total_time)
    try:
        logging.info("Executing the Update query and updating the required columns ")
        cursor.execute(
            'update prj_srctestcasehdr set src_total_execution_time=%s, execution_datetime=%s,last_execution_status=%s where test_id=%s',
            (total_time, exedate, execution_status, id))
        print("Updated Record")
        logging.info("Record updated sucessfully")
    except Exception as err:
        print("Error", err)
        logging.error("Error %s",err)
    finally:
        connection.commit()
        cursor.close()


def source_conn_select_detail(select_query, connection):
    # oracle_data = source_db_connection_call(projectid, token_data, conn_name, conn_type)
    # connection = source_connection_details(oracle_data)
    # print("connected to the Oracle Database")
    # logging.info("connected to the Oracle Database")
    # print(postgres_data)
    cursor = connection.cursor()
    try:
        cursor.execute(select_query)
        # print("check")
        data = cursor.fetchall()
        print("Executed target detail pre or post query")
        logging.info("Execution of target detail pre or post query sucess")
        exe_status = 'Success'
    except Exception as err:
        exe_status = 'Failed'
        print(err)
        logging.error("Execution of target detail pre or post query fail")
        data = None
    finally:
        cursor.close()
    return data, exe_status


def detail_table(id,app_connection,orcl_connection,writer):
    # logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Source Execution Started', token_data)
    all_data = []
    pre_data = appl_conn_select_detail_pre(id,app_connection)
    # print(pre_data)

    if pre_data:
        for pre in zip(pre_data):
            sub_list = []
            prequery = pre[0][0].strip()
            prequery = prequery.replace(';', '')
            pre_sv_exp = pre[0][1]
            pre_hv_exp = pre[0][2]
            pre_select_count, exe_status = source_conn_select_detail(prequery,orcl_connection)
            logging.info(
                "Reading the pre_execution_qry ,pre_check_output_singlevalue,hashvale from oracle database ")
            try:
                exedate = datetime.now().astimezone()
                if pre_select_count:
                    pre_select_count = pre_select_count[0][0]
                else:
                    pre_select_count = 0
                if str(pre_select_count).isnumeric():
                    # print(pre_select_count)
                    pre_single_count = str(pre_select_count)
                    pre_hash_value = None
                else:
                    pre_single_count = None
                    pre_hash_value = str(pre_select_count)
                # print(pre_single_count, pre_hash_value)
                appl_conn_update_detail_pre(id, pre_single_count, pre_hash_value, exedate,app_connection)
                logging.info(
                    "Executing the pre_single_count and pre_hash_value if value before execution and and value after execution are same then"
                    "execution status is matched else not matched  and storing the output in target database ")
                pre_exe_type = 'Pre'
                exe_query = prequery
                # exe_status = 'Success'
                pre_sv_act = pre_single_count
                pre_hv_act = pre_hash_value
                if (pre_sv_act == pre_sv_exp) and (pre_hv_act == pre_hv_exp):
                    test_result = 'Matched'
                else:
                    test_result = 'Not Matched'
                sub_list.extend(
                    [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act, pre_hv_act])
                all_data.append(sub_list)
            except Exception as e:
                pre_exe_type = 'Pre'
                exe_query = prequery
                # exe_status = 'Failed'
                pre_sv_act = 0
                pre_hv_act = None
                if (pre_sv_act == pre_sv_exp) and (pre_hv_act == pre_hv_exp):
                    test_result = 'Matched'
                else:
                    test_result = 'Not Matched'
                sub_list.extend(
                    [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act,
                     pre_hv_act])
                all_data.append(sub_list)
                print('Error', e)
                logging.error("Error %s",e)
                continue
            # break
    else:
        print('Detailed Table Pre Data Not Found with this', id)
        logging.info('Detailed Table Pre Data Not Found with this %s', id)


    # # calling exec query from the target header db
    sub_list = []
    testcaename, header_exe_type, data, exe_status = header_table(id,app_connection,orcl_connection)
    if testcaename != None:
        sub_list.extend(
            [header_exe_type, data, exe_status, None, None, None, None, None])
        all_data.append(sub_list)
    # execute post check queries
    post_data = appl_conn_select_detail_post(id,app_connection)
    if post_data:
        for post in zip(post_data):
            sub_list = []
            post_query = post[0][0].strip()
            post_sv_exp = post[0][1]
            post_hv_exp = post[0][2]
            post_query_mod = post_query.replace(';', '')
            post_select_count, exe_status = source_conn_select_detail(post_query_mod,orcl_connection)
            try:
                exedate = datetime.now().astimezone()
                if post_select_count:
                    post_select_count = post_select_count[0][0]
                else:
                    post_select_count = 0
                if str(post_select_count).isnumeric():
                    post_single_count = str(post_select_count)
                    post_hash_value = None
                else:
                    post_single_count = None
                    post_hash_value = str(post_select_count)
                # print(post_single_count, post_hash_value)
                appl_conn_update_detail_post(id, post_single_count, post_hash_value,
                                             exedate,app_connection)
                logging.info("Reading the post_single_count ,post_hash_value from application database ")
                logging.info(
                    "Executing the post_single_count and post_hash_value if value before execution and and value after execution are same then"
                    "then execution is matched else not matched")
                post_exe_type = 'Post'
                exe_query = post_query
                # exe_status = 'Success'
                post_sv_act = post_single_count
                post_hv_act = post_hash_value
                if (post_sv_act == post_sv_exp) and (post_hv_act == post_hv_exp):
                    test_result = 'Matched'
                else:
                    test_result = 'Not Matched'
                sub_list.extend(
                    [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                     post_hv_act])
                all_data.append(sub_list)
            except Exception as e:
                post_exe_type = 'Post'
                exe_query = post_query
                # exe_status = 'Failed'
                post_sv_act = 0
                post_hv_act = None
                if (post_sv_act == post_sv_exp) and (post_hv_act == post_hv_exp):
                    test_result = 'Matched'
                else:
                    test_result = 'Not Matched'
                sub_list.extend(
                    [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                     post_hv_act])
                all_data.append(sub_list)
                print('Error:', e)
                logging.error("Error %s",e)
                continue
    else:
        print('Detailed Table Post Data Not Found with this', id)
        logging.info('Detailed Table Post Data Not Found with this %s', id)
    # logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Source Execution Completed',
    #                token_data)

    if testcaename != None:
        all_status_df = pd.DataFrame(all_data,
                                     columns=["Query_type", "Execution_Query", "Execution_Status",
                                              "Test_Results_status", "Exp_Exe_Output_SV",
                                              "Exp_Exe_Output_HV", "Act_Exe_Output_SV", "Act_Exe_Output_HV"])

        all_status_df.to_excel(writer,
                               sheet_name=testcaename,
                               index=False)


# def total_time_query(xmlpath, strt, end, projectid, token_data, conn_name, conn_type):
#     oracle_data = source_db_connection_call(projectid, token_data, conn_name, conn_type)
#     connection = source_connection_details(oracle_data)
#     print("connected to the Oracle Database")
#     cursor = connection.cursor()
#     try:
#         cursor.execute('select months_between(%s,%s )from dual;', (end, strt))
#         print("Executed get time query")
#         data = cursor.fetchall()
#         # print(data)
#     except Exception as err:
#         print(err)
#     finally:
#         cursor.close()
#     return data


def appl_conn_select_detail_pre(id, connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, conn_name, conn_type)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info("Executing the select equery for execution_qry,execution_output_singlevalue,execution_output_hashkey from source database ")
        cursor.execute(
            "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_srctestcasedtl where test_id=%s and qry_exec_type= 'Pre';",
            [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error %s",err)
        data = None
    finally:
        cursor.close()
    return data


## application connection for target detail post select query execution
def appl_conn_select_detail_post(id, connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, conn_type, conn_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info("Executing target detail post select query ")
        cursor.execute(
            "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_srctestcasedtl where test_id=%s and qry_exec_type= 'Post';",
            [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error %s",err)
        data = None
    finally:
        cursor.close()
    return data


## application connection for target detail pre update query execution
def appl_conn_update_detail_pre(id, pre_single_count, pre_hash_value, exedate,connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, conn_name, conn_type)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info("Executing target detail pre update query")
        cursor.execute(
            "update prj_srctestcasedtl set execution_output_singlevalue=%s, execution_output_hashkey=%s, exec_datetime=%s where test_id=%s and qry_exec_type= 'Pre';",
            (pre_single_count, pre_hash_value, exedate, id))
        print("excuted update query for pre detail table")
        logging.info("executed update query for pre detail table")
    except Exception as err:
        print(err)
        logging.error("Error %s",err)
    finally:
        connection.commit()
        cursor.close()


## application connection for target detail pre update query execution
def appl_conn_update_detail_post(id, post_single_count, post_hash_value, exedate,connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, conn_type, conn_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info(" Executing target detail pre update query")
        cursor.execute(
            "update prj_srctestcasedtl set execution_output_singlevalue=%s, execution_output_hashkey=%s, exec_datetime=%s where test_id=%s and qry_exec_type= 'Post';",
            (post_single_count, post_hash_value, exedate, id))
    except Exception as err:
        print(err)
        logging.error("Error %s",err)
    finally:
        connection.commit()
        cursor.close()


def executing_queries(testid_list, xmlpath, projectid, app_connection,orcl_connection, ftfschemaname):
    ftfschemaname = ftfschemaname.lower()
    output_excelpath = path_PreO2p + "/" + ftfschemaname.upper() + '/' + 'FTF_EXCEL'
    if not os.path.exists(output_excelpath):
        os.makedirs(output_excelpath)
    reprt_path = output_excelpath + '/' + ftfschemaname + '_functional_TestingSource.xlsx'
    logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Source Execution Started',
               app_connection)
    with pd.ExcelWriter(reprt_path) as writer:
        for id in testid_list:
            detail_table(id,app_connection,orcl_connection,writer)
    logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Source Execution Completed',
               app_connection)



def source_execution(xmlpath, projectid, token_data, conn_type, conn_name, testid_list, ftfschemaname):
    qmigrationlogfilelocation = qmigration_Xtractor(xmlpath, parent_tag='qmig_logs')
    qmigration_log = qmigrationlogfilelocation.replace("\n", "").replace(' ', '')
    qmigrator_path = path + "/" + qmigration_log
    if not os.path.exists(qmigrator_path + '/' + 'FUNCTIONAL_SRC_TGT'):
        os.makedirs(qmigrator_path + '/' + 'FUNCTIONAL_SRC_TGT')
        # Set up for the log file location
    logging.getLogger().handlers.clear()
    logname = os.path.basename(__file__)
    logger = logging.getLogger(logname)
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s -' + logname + ' - %(levelname)s - %(message)s',
                        filename=qmigrator_path + '/' + 'FUNCTIONAL_SRC_TGT' + '/' + 'functional_src.log',
                        filemode='a+')
    app_data = appli_db_connection_call(token_data, projectid, conn_name, conn_type)
    app_source_connection = applica_db_connection_details(xmlpath, app_data)
    oracle_data = source_db_connection_call(projectid, token_data, conn_name, conn_type)
    source_oracle_connection = source_connection_details(oracle_data)
    executing_queries(testid_list, xmlpath, projectid,app_source_connection, source_oracle_connection,ftfschemaname)
