from qmig_config import *
# from authentication import get_appli_connection, decryptstr
from datetime import datetime, date
import time

# import datetime
import psycopg2

import re
from authentication import decrypt_application_details, decryptstr, decrypt_target_funct_test,logging_db
import pandas as pd
import os
import logging
import sys
from cdc_pkgs_oracle_postgres import calling_cdc_packages
from cdc_proc_func_oracle_postgres import calling_proc_func_cdc_logic

path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

dir = os.listdir(path)
dir = sorted(dir)
find_db = dir.index('DB')
path_DB = path + '/' + dir[find_db]
DB_dirs = os.listdir(path_DB)
find_preO2 = DB_dirs.index('Pre_O2P')
path_PreO2p = path_DB + '/' + DB_dirs[find_preO2]

# testid_list = [108, 109, 110]

transit_path = path
transit_dirs = os.listdir(transit_path)
transit_dirs = sorted(transit_dirs)

folder_path = None

if transit_dirs[find_db].lower() == str('DB').lower():
    db_dirs = os.listdir(transit_path + '/' + transit_dirs[find_db])
    db_dirs = sorted(db_dirs)
    if 'DR' not in transit_dirs:
        db_dirs.append("DR")
    find_temp = db_dirs.index('DR')
    if 'Source' not in transit_dirs:
        db_dirs.append("Source")
    find_source = db_dirs.index('Source')
    folder_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_source]
    # print(folder_path)
    if db_dirs[find_temp].lower() == str('DR').lower():
        db_transit_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_temp]
    else:
        db_transit_path = transit_path + '/' + transit_dirs[find_db] + '/' + db_dirs[find_temp]

# calling api for application db
logging.info("calling api for application db")


def appli_db_connection_call(tokendata, project_id, connection_type, connection_name):
    app_data= decrypt_application_details(tokendata, project_id, connection_type, connection_name)
    return app_data


def target_db_connection_call(tokendata, project_id, connection_type, connection_name):
    # calling get connection details for target database from api
    postgres_data = decrypt_target_funct_test(tokendata, project_id, connection_type, connection_name)
    return postgres_data


def getting_source_details(id,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select test_case_name,test_group from prj_tgttestcasehdr where tgt_test_id=%s", [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


##Replaced
def target_appli_db_iteration_insert(xmlpath, item_name, projectid, iter, procedures_functions_names,
                                     header_exe_type,ftfschemaname,connection):
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        cursor.execute('CALL public.sp_prj_tgtobjects_insert(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s);fetch all in "dataset";',
                       (
                       projectid, iter, header_exe_type.upper(), item_name.upper(), procedures_functions_names,ftfschemaname, 'FALSE',
                       'FALSE',
                       'FALSE', None,
                       'python',
                       'dataset'))
        data = cursor.fetchall()
        print("target exc done")
    except Exception as err:
        data=None
        print("error", err)
    finally:
        connection.commit()
        cursor.close()
    return data


def getting_source_id_pre(id,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select src_dtl_id from prj_tgttestcasedtl where tgt_test_id=%s and qry_exec_type='Pre'", [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


def getting_source_id_post(id, projectid, token_data, xmlpath, connection_type, connection_name):
    app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select src_dtl_id from prj_tgttestcasedtl where tgt_test_id=%s and qry_exec_type='Post'", [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


def appli_db_target_cdc(xmlpath, item_name, header_exe_type, oracle_code_data_str_cdc, projectid, iteration_id,
                        app_data,ftfschemaname):
    # print(oracle_code_data_str_cdc)
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        cursor.execute(
            'CALL public.sp_prj_tgtobjects_insert_cdc(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
            (projectid, iteration_id, header_exe_type.upper(), item_name.upper(), oracle_code_data_str_cdc,ftfschemaname, 'FALSE',
             'FALSE', 'FALSE', None,
             'python',
             'dataset'))
        data = cursor.fetchall()
        print("target exc for cdc is done")
    except Exception as err:
        print("target cdc error", err)
        data = None
        pass
    finally:
        connection.commit()
        cursor.close()
    return data


def getting_source_details_headerquery(id,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select object_type, object_signature from prj_tgttestcasehdr where tgt_test_id = %s", [id])
        data = cursor.fetchall()
        # print(data, 'getting header details in source')
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


def source_data_headerquery(id,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select test_case_name from prj_tgttestcasehdr where tgt_test_id = %s", [id])
        data = cursor.fetchall()

        data = data[0][0]
        # print(data, 'getting header details in source')
        cursor.close()
        cursor1 = connection.cursor()
        cursor1.execute("select code_object_name from prj_srctestcasehdr where test_case_name=%s", [data])
        data1 = cursor1.fetchall()

        cursor1.close()
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data1 = None

    return data1


# cdc logic
def appli_db_src_cdc(xmlpath, item_name, item_type, item_code, projectid, iteration_id, app_data,ftfschemaname):
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        # cursor.execute('CALL public.sp_prj_srcobjects_insert(%s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
        #                (projectid, iteration_id, item_type, item_name, item_code, None, 'python', 'dataset'))
        cursor.execute('CALL public.sp_prj_srcobjects_insert_cdc(%s,%s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
                       (projectid, iteration_id, item_type.upper(), item_name.upper(), item_code,ftfschemaname, None, 'python',
                        'dataset'))
        data = cursor.fetchall()
        print("source exc for cdc is  done")
    except Exception as err:
        # print("error", err)
        data = None
        pass
    finally:
        connection.commit()
        cursor.close()
    return data


# for source data extaction
def source_extraction(xmlpath, projectid, header_query_type,item_type, item_type_1,
                                                           pack_proc_name,
                                                           ftfschemaname, app_data, iter_id):
    split_procedure = ''
    if header_query_type == 'Packaged Function' or header_query_type == 'Packaged Procedure':

        with open(folder_path + '/' + str(
                ftfschemaname).upper() + '/' + 'PACKAGES' + '/' + item_type_1.upper() + '.sql','r') as f:

            single_line_text = f.read()
            single_line_text_split = single_line_text.split('create or replace ')
            read_text_single_split = [i.strip() for i in single_line_text_split if i != '']
            empty_data = []
            for i in read_text_single_split:
                if 'package body "' in i:
                    i_split_package = i.split('package body')[1].split('.', 1)[1].split(' ')[0]
                    item_names = i_split_package.replace('"', '')
                    if item_type_1 == item_names:
                        dbms_find = re.findall(r'dbms.*?;', i, re.DOTALL)
                        for j in dbms_find:
                            dbms_replace = j.replace('procedure', 'xyzxyzxyz')
                            i = i.replace(j, dbms_replace)
                        proc_func_split = re.split(r'procedure|function', i)
                        proc_func_split = [i.strip() for i in proc_func_split if i != '']
                        empty_data.append(proc_func_split)
                        if proc_func_split:
                            for j in proc_func_split:
                                if j.strip().startswith(pack_proc_name):
                                    header_query_type = header_query_type.lower().replace('packaged', '')
                                    split_procedure = 'create or replace'+header_query_type + ' '+j
                                    if j!='' or j!=None:
                                        oracle_code_data_str_cdc = calling_proc_func_cdc_logic(split_procedure)
                                        # print(oracle_code_data_str_cdc)
                                        app_data_src_cdc = appli_db_src_cdc(xmlpath, item_type, header_query_type,
                                                                            oracle_code_data_str_cdc,
                                                                            projectid,
                                                                            iter_id, app_data, ftfschemaname)

    if header_query_type == 'Function':
        # print('entered')
        with open(folder_path + '/' + str(
                ftfschemaname).upper() + '/' + 'FUNCTIONS' + '/' + item_type.upper() + '.sql','r') as f:
            single_line_text = f.read()
            # print(single_line_text)

            single_line_text = single_line_text.replace('create or replace editionable function',
                                                        'create or replace function')
            split_with_createfunction = re.split(rf'create\s+or\s+replace', single_line_text)
            for funcs in split_with_createfunction:
                if funcs:
                    data_name = re.findall('function', funcs, re.IGNORECASE)[0]
                    data_all = re.findall(rf'{data_name}\s+\"{ftfschemaname}\"\.\"{item_type_1}', funcs,
                                          re.IGNORECASE)[0]
                    if data_all in funcs:

                    # split_with_createfunction = single_line_text.split('create or replace function ')[1:]
                    # for funcs in split_with_createfunction:
                    #     all_function_name = funcs.split('.', 1)[1:]
                    #     all_function_name_str = ''.join(all_function_name)
                    #     all_function_name_str_1 = all_function_name_str.split(' ')[0].replace('"', '')
                    # if item_type_1 == all_function_name_str_1:
                    #     header_query_type = header_query_type.lower().replace('packaged', '')
                        split_procedure = 'create or replace'+ funcs
                        if funcs!='' or funcs!=None:
                            oracle_code_data_str_cdc = calling_proc_func_cdc_logic(split_procedure)
                            # print(oracle_code_data_str_cdc)
                            app_data_src_cdc = appli_db_src_cdc(xmlpath, item_type, header_query_type, oracle_code_data_str_cdc,
                                                                projectid,
                                                                iter_id,app_data,ftfschemaname)
    if header_query_type == 'Procedure':
        with open(folder_path + '/' + str(
                ftfschemaname).upper() + '/' + 'PROCEDURES' + '/' + item_type.upper() + '.sql','r') as f:
            single_line_text = f.read()

            single_line_text = single_line_text.replace('create or replace editionable procedure',
                                                        'create or replace procedure')
            # split_with_createfunction = single_line_text.split('create or replace procedure ')[1:]
            split_with_createfunction = re.split(rf'create\s+or\s+replace', single_line_text)
            for procs in split_with_createfunction:
                if procs:
                    data_name = re.findall('procedure', procs, re.IGNORECASE)[0]
                    data_all = re.findall(rf'{data_name}\s+\"{ftfschemaname}\"\.\"{item_type_1}', procs,
                                          re.IGNORECASE)[0]
                    if data_all in procs:
                    # print(p)
                    # all_function_name = procs.split('.', 1)[1:]
                    # all_function_name_str = ''.join(all_function_name)
                    # print(all_function_name_str)
                    # all_function_name_str_1 = all_function_name_str.split('(')[0].replace('"', '')
                    # if item_type_1 == all_function_name_str_1:
                    #     header_query_type = header_query_type.lower().replace('packaged', '')
                        split_procedure = 'create or replace'+ procs
                        if procs!='' or procs!=None:
                            oracle_code_data_str_cdc = calling_proc_func_cdc_logic(split_procedure)
                            # print(oracle_code_data_str_cdc)
                            app_data_src_cdc = appli_db_src_cdc(xmlpath, item_type, header_query_type, oracle_code_data_str_cdc,
                                                                projectid,
                                                                iter_id,app_data,ftfschemaname)


    return split_procedure


def detail_table(id, xmlpath, projectid, token_data, connection_type, connection_name,app_connection,target_connection,ftfschemaname, writer):
    result = getting_source_details(id,app_connection)
    # print('result', result)
    header_query = getting_source_details_headerquery(id,app_connection)
    header_query_type = header_query[0][0]
    header_query_body = header_query[0][1]
    test_case_name = result[0][0]
    test_group = result[0][1]
    # print(test_case_name, test_group)
    src_header_query = source_data_headerquery(id,app_connection)

    all_data = []
    pre_data = appl_conn_select_detail_pre(id, app_connection)
    # print(pre_data,'pre_data')
    logging.info("Reading the pre_execution_qry ,pre_check_output_singlevalue,hashvale from application database ")
    if pre_data:
        # pre_data = [i[0] for i in pre_data]
        # print(pre_data)
        source_dtl_id = getting_source_id_pre(id,app_connection)
        # print(source_dtl_id, "source_dtl_id")
        source_dtl_id_list = []
        for i in source_dtl_id:
            source_dtl_id_list.append(i[0])
        # print(source_dtl_id_list)
        cnt = 0
        flag = 0
        flag_st = 0
        for pre in zip(pre_data):
            # print(source_dtl_id_list[cnt], "checking ids")

            # print(pre, "pre")
            sub_list = []
            prequery = pre[0][0].strip()
            # print(prequery)
            pre_sv_exp = pre[0][1]
            pre_hv_exp = pre[0][2]
            prequery = 'SET search_path = ' + ftfschemaname + ';' + '\n' + prequery
            # print(prequery)
            pre_select_count, exe_status, flag_st = target_conn_select_detail(prequery,  ftfschemaname,
                                                                              header_query_type,
                                                                              header_query_body, src_header_query,
                                                                              flag_st,target_connection)
            if exe_status == 'Failed':
                flag = 1

            # pre_select_count = [i[0] for i in pre_select_count]
            # print(pre_select_count)
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
            # print(prequery)
            # print(pre_sv_exp,pre_hv_exp)
            try:
                # print(pre_single_count, pre_hash_value)
                appl_conn_update_detail_pre(id, pre_single_count, pre_hash_value,
                                            exedate,app_connection)
                logging.info(
                    "Executing the pre_single_count and pre_hash_value if value before execution and and value after execution are same then"
                    "execution status is matched else not matched  and storing the output in target database ")
                pre_exe_type = 'Pre'
                exe_query = prequery
                # exe_status = 'Success'
                pre_sv_act = pre_single_count
                pre_hv_act = pre_hash_value
                # print("testing")
                # print(source_dtl_id_list[cnt])
                # print("testing1")
                res = matching_source_target_pre(id,
                                                 test_case_name, test_group, source_dtl_id_list[cnt],app_connection)
                # print(res, "for", source_dtl_id_list[cnt])

                # print(res[0][0],res[0][1])
                # if (pre_sv_act == pre_sv_exp) and (pre_hv_act == pre_hv_exp):
                #   test_result = 'Matched'
                # else:
                #   test_result = 'Not Matched'
                if res == 1:
                    # print('entered to if condition')
                    test_result = ''
                    sub_list.extend(
                        [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act,
                         pre_hv_act])
                    all_data.append(sub_list)
                else:
                    # print('entered in to the else condition')
                    if res[0][0] == 'Matched' and res[0][1] == 'Matched':
                        test_result = 'Matched'
                        sub_list.extend(
                            [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act,
                             pre_hv_act])
                        all_data.append(sub_list)
                        # return res
                    else:
                        test_result = 'Not Matched'
                        flag = 1
                        sub_list.extend(
                            [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act,
                             pre_hv_act])
                        # return res
                        all_data.append(sub_list)

            except Exception as e:
                pre_exe_type = 'Pre'
                exe_query = prequery
                # # exe_status = 'Failed'
                pre_sv_act = 0
                pre_hv_act = None
                # if (pre_sv_act == pre_sv_exp) and (pre_hv_act == pre_hv_exp):
                #     test_result = 'Matched'
                #
                # else:
                #     test_result = 'Not Matched'
                # test_result ='---'
                test_result = None
                flag = 1
                sub_list.extend(
                    [pre_exe_type, exe_query, exe_status, test_result, pre_sv_exp, pre_hv_exp, pre_sv_act,
                     pre_hv_act])
                all_data.append(sub_list)
                print('Pre Error', e)
                logging.error("Error %s ", e)
                continue
            cnt = cnt + 1
            # break

        all_status_df = pd.DataFrame(all_data,
                                     columns=["Query_type", "Execution_Query", "Execution_Status",
                                              "Test_Results_status", "Exp_Exe_Output_SV",
                                              "Exp_Exe_Output_HV", "Act_Exe_Output_SV",
                                              "Act_Exe_Output_HV"])
        # print(all_status_df)
        all_status_df.to_excel(writer,
                               sheet_name=test_case_name,
                               index=False)
        # print(all_status_df)

        if flag == 1:
            print('Pre query failed that why not entering into header')
            return

    else:
        print('Detailed Table Pre Data Not Found with this', id)
        logging.info('Detailed Table Pre Data Not Found with this %s', id)

    sub_list = []
    # calling exec query from the target header db
    testcaename, header_exe_type, data, exe_status = header_table(id, app_connection,target_connection, ftfschemaname, src_header_query,
                                                                  header_query_type,xmlpath, projectid)
    if testcaename != None:
        sub_list.extend(
            [header_exe_type, data, exe_status, None, None, None, None, None])
        all_data.append(sub_list)
    all_status_df = pd.DataFrame(all_data,
                                 columns=["Query_type", "Execution_Query", "Execution_Status",
                                          "Test_Results_status", "Exp_Exe_Output_SV",
                                          "Exp_Exe_Output_HV", "Act_Exe_Output_SV",
                                          "Act_Exe_Output_HV"])
    all_status_df.to_excel(writer,
                           sheet_name=test_case_name,
                           index=False)
    # print(all_status_df,'********')
    if exe_status == 'Failed':
        return
    # execute post check queries
    post_data = appl_conn_select_detail_post(id, projectid, token_data, xmlpath, connection_type, connection_name)
    if post_data:
        # post_data = [i[0] for i in post_data]
        source_dtl_id = getting_source_id_post(id, projectid, token_data, xmlpath, connection_type, connection_name)
        # print(source_dtl_id, "source_dtl_id")
        source_dtl_id_list = []
        for i in source_dtl_id:
            source_dtl_id_list.append(i[0])
        # print(source_dtl_id_list)
        cnt = 0
        flag_st = 0
        for post in zip(post_data):
            sub_list = []
            post_query = post[0][0].strip()
            post_sv_exp = post[0][1]
            post_hv_exp = post[0][2]
            # post = post.strip()
            post = 'SET search_path = ' + ftfschemaname + ';' + '\n' + post_query
            # post = 'SET search_path = ' + ftfschemaname + ';' + '\n' + post_query
            post_select_count, exe_status, flag_st = target_conn_select_detail(post_query, xmlpath, projectid,
                                                                               token_data,
                                                                               connection_type,
                                                                               connection_name, ftfschemaname,
                                                                               header_query_type,
                                                                               header_query_body, src_header_query,
                                                                               flag_st)
            # post_select_count = [i[0] for i in post_select_count]
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
            try:
                # print(post_single_count, post_hash_value)
                appl_conn_update_detail_post(id, projectid, token_data, xmlpath, post_single_count, post_hash_value,
                                             exedate, connection_type, connection_name)
                logging.info("Reading the post_single_count ,post_hash_value from application database ")
                logging.info(
                    "Executing the post_single_count and post_hash_value if value before execution and and value after execution are same then"
                    "then execution is matched else not matched")

                post_exe_type = 'Post'
                exe_query = post
                # exe_status = 'Success'
                post_sv_act = post_single_count
                post_hv_act = post_hash_value
                # print(post_hv_act)
                res = matching_source_target_post(id, projectid, token_data, xmlpath, connection_type, connection_name,
                                                  test_case_name, test_group, source_dtl_id_list[cnt],app_connection)
                # if (post_sv_act == post_sv_exp) and (post_hv_act == post_hv_exp):
                #     test_result = 'Matched'
                # else:
                #     test_result = 'Not Matched'
                # sub_list.extend(
                #     [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                #      post_hv_act])
                # all_data.append(sub_list)
                if res == 1:
                    test_result = ''
                    sub_list.extend(
                        [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                         post_hv_act])
                    all_data.append(sub_list)
                else:
                    if res[0][0] == 'Matched' and res[0][1] == 'Matched':
                        test_result = 'Matched'
                        sub_list.extend(
                            [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                             post_hv_act])
                        all_data.append(sub_list)
                        # return res
                    else:
                        test_result = 'not matched'
                        sub_list.extend(
                            [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                             post_hv_act])
                        # return res
                        all_data.append(sub_list)

                    all_status_df = pd.DataFrame(all_data,
                                                 columns=["Query_type", "Execution_Query", "Execution_Status",
                                                          "Test_Results_status", "Exp_Exe_Output_SV",
                                                          "Exp_Exe_Output_HV", "Act_Exe_Output_SV",
                                                          "Act_Exe_Output_HV"])
                    all_status_df.to_excel(writer,
                                           sheet_name=test_case_name,
                                           index=False)
                    # sys.exit()

            except Exception as e:
                post_exe_type = 'Post'
                exe_query = post
                # exe_status = 'Failed'
                post_sv_act = 0
                post_hv_act = None
                # if (post_sv_act == post_sv_exp) and (post_hv_act == post_hv_exp):
                #     test_result = 'Matched'
                # else:
                #     test_result = 'Not Matched'
                test_result = None
                sub_list.extend(
                    [post_exe_type, exe_query, exe_status, test_result, post_sv_exp, post_hv_exp, post_sv_act,
                     post_hv_act])
                all_data.append(sub_list)
                print('post Error:', e)
                logging.error("Error %s", e)
                continue
        cnt = cnt + 1
    else:
        print('Detailed Table Post Data Not Found with this', id)
        logging.info('Detailed Table Post Data Not Found with this %s', id)
    if testcaename != None:
        all_status_df = pd.DataFrame(all_data,
                                     columns=["Query_type", "Execution_Query", "Execution_Status",
                                              "Test_Results_status", "Exp_Exe_Output_SV",
                                              "Exp_Exe_Output_HV", "Act_Exe_Output_SV", "Act_Exe_Output_HV"])
        all_status_df.to_excel(writer,
                               sheet_name=testcaename,
                               index=False)
        # print(all_status_df, 'dataframe')


# calling update query for application db
def appl_update_query_header(execution_status, total_time, exedate, id, connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("Connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info(
            "updating the src_total_execution_time,execution_datetime,last_execution_status  into target database")
        cursor.execute(
            'update prj_tgttestcasehdr set src_total_execution_time=%s, execution_datetime=%s,last_execution_status=%s where tgt_test_id=%s',
            (total_time, exedate, execution_status, id))
        logging.info("Record Updated")
        print("Updated Record")
    except Exception as err:
        print("Error", err)
        logging.error("Error while updating the record  %s ", err)
    finally:
        connection.commit()
        cursor.close()


# def total_time_query(xmlpath, strt, end):
#     # call xml extractor to get db connection and queries from the xml file
#     postgres_data, postg_query_tag = xml_extractor(xmlpath, "Postgres", parent_tag='PostgresSchemaTag')  # postgres
#     connection = target_connection_details_ftf()(postgres_data)
#     print("connected to the Postgres Database")
#     cursor = connection.cursor()
#     try:
#         cursor.execute('select age(%s,%s);', (end, strt))
#         print("Executed get time query")
#         data = cursor.fetchall()
#         print(data)
#     except Exception as err:
#         print(err)
#     finally:
#         cursor.close()
#     return data

# calling stored procedure for summery data count updation
def iteration(xmlpath, projectid, app_data, mig_type,schema):
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        cursor.execute('select public.fn_prj_current_iteration1(%s,%s,%s,%s);', (projectid, mig_type, 'N',schema))
        data = cursor.fetchall()
        # print(data,"iteration selecetd")
    except Exception as err:
        print("error", err)
        data = None
        pass
    finally:
        connection.commit()
        cursor.close()
    return data


def header_table(id,app_connection,target_connection, ftfschemaname, src_header_query,
                 header_query_type,xmlpath, project_id):
    header_data = appl_conn_select_header(id, app_connection)
    if header_data:
        testcasename = header_data[0][0]
        header_exe_type = header_data[0][1]
        header_data_query = header_data[0][2]
        data = 'SET search_path = ' + ftfschemaname + ';' + '\n' + header_data_query
        # data = 'SET search_path = ' + ftfschemaname + ';' + '\n' + header_data_query
        start_time = datetime.now()
        # print(data)
        execution_status = target_conn_select_header(xmlpath, project_id,data, header_exe_type, header_data_query, ftfschemaname,
                                                     src_header_query, header_query_type,target_connection,app_connection)
        end_time = datetime.now()
        # total_time = total_time_query(xmlpath, str(start_time), str(end_time))
        # total_time = [i[0] for i in total_time]
        # total_time = total_time[0]
        logging.info("storing the execution time and execution date ")
        total_time = end_time - start_time
        exedate = datetime.now().astimezone()
        try:
            # calling the appl_update_query_header function to update the required data into target database
            appl_update_query_header(execution_status, str(total_time), exedate, id,app_connection)
            # exe_status = 'Success'
        except Exception as e:
            # exe_status = 'Failed'
            print('Error:', e)
            logging.error("Error %s ", e)
        return testcasename, header_exe_type, data, execution_status
    else:
        print('Data Not Found for application db header select records with this', id)
        logging.info('Data Not Found for application db header select records with this %s', id)
        return None, None, None, None


# postgres's connection for target select query
def target_conn_select_header(xmlpath, projectid,exe_query,header_exe_type, header_data_query, ftfschemaname, src_header_query, header_query_type,connection,app_connection):
    # call xml extractor to get db connection and queries from the xml file
    logging.info("call xml extractor to get db connection and queries from the xml file")
    # postgres_data, postg_query_tag = xml_extractor(xmlpath, None, parent_tag='PostgresSchemaTag')  # postgres
    # postgres_data = target_db_connection_call(token_data, projectid, connection_type, connection_name)
    # # postgres_data = postgres_data[0]
    # connection = target_connection_details(postgres_data)
    # print("connected to the Postgres Database")
    # logging.info("connected to the Postgres Database")
    cursor = connection.cursor()
    try:
        # print(exe_query)
        # ghjhjkjkjkj
        cursor.execute(exe_query)
        print("Executed target query")
        status = 'Success'
        logging.info("sucessfully Executed target query ")
    except Exception as err:
        print(err)
        status = "Failed"
        # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
        iter_id = iteration(xmlpath, projectid, app_connection, 'CodeObjects',ftfschemaname)
        iter_id = iter_id[0][0]
        item_name = exe_query.split('.',1)[1]
        item_name = item_name.replace('.', '_').lower()
        # item_name = function_procedure_name.split('(', 1)[0]
        procedures_functions_names = dr_extraction(xmlpath, header_exe_type, db_transit_path, item_name,
                                                   ftfschemaname, projectid,
                                                   iter_id, app_connection)
        if len(procedures_functions_names):
            # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
            iter_id = iteration(xmlpath, projectid, app_connection, 'CodeObjects',ftfschemaname)
            iter = list(map(list, zip(*iter_id)))
            iter = iter[0][0]
            target_appli_db_iteration_insert(xmlpath, item_name, projectid, iter,
                                              procedures_functions_names, header_exe_type,ftfschemaname,app_connection)
            if src_header_query:
                # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
                src_data = src_header_query[0][0]
                src_data = src_data.lower()
                item_type = src_data.split('.', 1)[1]
                item_type_1 = item_type.split('.', 1)[0]
                item_type_2 = item_type.split('.', 1)[1:]
                item_type_2_str = ''.join(item_type_2)
                pack_proc_name = item_type_2_str.split('(', 1)[0]
                iter_id = iteration(xmlpath, projectid, app_connection, 'CodeObjects',ftfschemaname)
                iter_id = iter_id[0][0]
                print("iterid",iter_id)
                calling_func_proc_pack = source_extraction(xmlpath, projectid, header_query_type,item_type, item_type_1,
                                                           pack_proc_name,
                                                           ftfschemaname, app_connection, iter_id)

                if len(calling_func_proc_pack):
                    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
                    iter_id = iteration(xmlpath, projectid, app_connection, 'CodeObjects',ftfschemaname)
                    iter = list(map(list, zip(*iter_id)))
                    iter = iter[0][0]
                    appli_db_src(xmlpath, item_type, header_query_type, calling_func_proc_pack, projectid, iter,
                                 app_connection,ftfschemaname)

        logging.error("Target query execution failed %s", err)
    finally:
        cursor.close()
    return status


def appli_db_src(xmlpath, item_name, item_type, item_code, projectid, iteration_id,connection,ftfschemaname):
    # connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        cursor.execute('CALL public.sp_prj_srcobjects_insert(%s,%s,%s,%s,%s,%s,%s,%s,%s);fetch all in "dataset";',
                       (projectid, iteration_id, item_type.upper(), item_name.upper(), item_code,ftfschemaname, None, 'python',
                        'dataset'))
        data = cursor.fetchall()
        print("source exc done")
    except Exception as err:
        # print("error", err)
        data = None
        pass
    finally:
        connection.commit()
        cursor.close()
    return data


def dr_extraction(xmlpath, header_exe_type, db_transit_path, item_name, ftfschemaname, projectid,
                  iter_id, app_data):
    procedure_function_name = ''
    file_path = db_transit_path + '/' + ftfschemaname
    collecting_fld = os.listdir(file_path)
    for all_fld_files in collecting_fld:
        object_file_name = all_fld_files
        setting_path = db_transit_path + '/' + str(ftfschemaname).upper() + '/' + object_file_name
        pg_file = os.listdir(setting_path)
        pg_files = []
        for pg in pg_file:
            path_app_src = setting_path + '/' + pg
            pg_files.append(path_app_src)
        if (object_file_name == 'PG_PACKAGES' and (
                header_exe_type == 'Packaged Function' or header_exe_type == 'Packaged Procedure')) or (
                object_file_name == 'PG_FUNCTIONS' and header_exe_type == 'Function') or (
                object_file_name == 'PG_PROCEDURES' and header_exe_type == 'Procedure'):
            # single file
            if len(pg_files) ==1:
                for one_file in pg_files:
                    with open(one_file,'r') as f:
                        all_data = f.read()
                        if object_file_name == 'PG_PACKAGES':
                            # print('entered-----------------')
                            # item_name = item_name.replace('.','_').lower()
                            data_split = re.split(rf'create\s+or\s+replace', all_data)
                            for splitdata in data_split:
                                    data_name = re.split('procedure|function', splitdata.strip())
                                    proc_func_split = [i.strip() for i in data_name if i != '']
                                    for j in proc_func_split:
                                        pkg_proc_func = j.split('(', 1)[0]
                                        if item_name.strip().lower() == pkg_proc_func.strip().lower():
                                            procedure_function_name = 'create or replace' + splitdata
                                            if splitdata != '' or splitdata != None:
                                                oracle_code_data_str_cdc = calling_proc_func_cdc_logic(
                                                    procedure_function_name)
                                                app_data_src_cdc = appli_db_target_cdc(xmlpath, item_name,
                                                                                       header_exe_type,
                                                                                       oracle_code_data_str_cdc,
                                                                                       projectid, iter_id, app_data,ftfschemaname)
                        elif (object_file_name == 'PG_FUNCTIONS') or (object_file_name == 'PG_PROCEDURES'):
                            # print('entered to functions')
                            data_split = re.split(rf'create\s+or\s+replace', all_data)
                            for splitdata in data_split:
                                    data_name = re.split('procedure|function', splitdata.strip())
                                    proc_func_split = [i.strip() for i in data_name if i != '']
                                    for k in proc_func_split:
                                        pkg_proc_func = k.split('(', 1)[0]
                                        if item_name.strip().lower() == pkg_proc_func.strip().lower():
                                            procedure_function_name = 'create or replace' + splitdata
                                            if splitdata != '' or splitdata != None:
                                                oracle_code_data_str_cdc = calling_proc_func_cdc_logic(
                                                    procedure_function_name)
                                                app_data_src_cdc = appli_db_target_cdc(xmlpath, item_name,
                                                                                       header_exe_type,
                                                                                       oracle_code_data_str_cdc,
                                                                                       projectid, iter_id, app_data,ftfschemaname)
            # multiple files
            elif len(pg_files) > 1:
                for one_file in pg_files:
                    multi_file_name = one_file.rsplit('/', 1)[1]
                    multi_file = multi_file_name.split('.')[0]
                    with open(one_file, 'r') as f:
                        all_data = f.read()
                        if object_file_name == 'PG_PACKAGES':
                            item_name = item_name.replace('.','_').lower()
                            data_split = re.split(rf'create\s+or\s+replace', all_data)
                            for splitdata in data_split:
                                data_name = re.split('procedure|function', splitdata.strip())
                                proc_func_split = [i.strip() for i in data_name if i != '']
                                for j in proc_func_split:
                                    pkg_proc_func=j.split('(',1)[0]
                                    if item_name.strip().lower() == pkg_proc_func.strip().lower():
                                        procedure_function_name = 'create or replace' + splitdata
                                        if splitdata != '' or splitdata != None:
                                            oracle_code_data_str_cdc = calling_proc_func_cdc_logic(
                                                procedure_function_name)
                                            app_data_src_cdc = appli_db_target_cdc(xmlpath, item_name,
                                                                                   header_exe_type,
                                                                                   oracle_code_data_str_cdc,
                                                                                   projectid, iter_id, app_data,ftfschemaname)
                        elif (object_file_name == 'PG_FUNCTIONS') or( object_file_name == 'PG_PROCEDURES'):
                            # print(multi_file)
                            if item_name.strip().lower() == multi_file.lower().lower():
                                procedure_function_name = all_data
                                if procedure_function_name != '' or procedure_function_name != None:
                                    oracle_code_data_str_cdc = calling_proc_func_cdc_logic(procedure_function_name)
                                    app_data_src_cdc = appli_db_target_cdc(xmlpath, item_name, header_exe_type,
                                                                           oracle_code_data_str_cdc,
                                                                           projectid, iter_id, app_data,ftfschemaname)
    return procedure_function_name



## postgres's connection for target details query
def target_conn_select_detail(select_query,ftfschemaname, header_query_type, header_query_body, src_header_query, flag_st,connection):
    # call xml extractor to get db connection and queries from the xml file
    logging.info("call xml extractor to get db connection and queries from the xml file")
    # postgres_data, postg_query_tag = xml_extractor(xmlpath, None, parent_tag='PostgresSchemaTag')  # postgres
    # postgres_data = target_db_connection_call(token_data, projectid, connection_type, connection_name)
    # # print(postgres_data)
    # connection = target_connection_details(postgres_data)
    # print("connected to the Postgres Database")
    # logging.info("connected to the Postgres Database")
    cursor = connection.cursor()
    try:
        cursor.execute(select_query)
        data = cursor.fetchall()
        # nbfjhdjfhd
        print("Executed target detail pre or post query")
        logging.info("Executed target detail pre or post query sucessfully")
        exe_status = 'Success'
    except Exception as err:
        print('entered to exception block detailed error', err)
        data = None
        exe_status = 'Failed'

        # if flag_st == 0:
        #     flag_st = flag_st + 1
        #     app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
        #     iter_id = iteration(xmlpath, projectid, app_data, 'CodeObjects')
        #     # print(iter_id,'iter_id')
        #     iter_id = iter_id[0][0]
        #     function_procedure_name = header_query_body.split('.')[1:]
        #     function_all = '\n'.join(function_procedure_name)
        #     item_name = function_all.split('(', 1)[0]
        #     item_name = item_name.replace('call ','').strip()
        #     # item_name = item_name.replace(' '+item_name,item_name)
        #
        #     procedures_functions_names = inserting_data_into_table(xmlpath,header_query_type, db_transit_path, item_name,
        #                                                            ftfschemaname,projectid,
        #                                                     iter_id, app_data)
        #     if len(procedures_functions_names):
        #         app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
        #         iter_id = iteration(xmlpath, projectid, app_data, 'CodeObjects')
        #         iter_id = iter_id[0][0]
        #         # iter = list(map(list, zip(*iter_id)))
        #         # iter = iter[0][0]
        #         header_exe_type = header_query_type
        #         target_appli_db_iteration_insert(xmlpath, item_name, projectid, iter_id,
        #                                          app_data, procedures_functions_names, header_exe_type)
        #
        #
        #     if src_header_query:
        #         app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
        #         src_data = src_header_query[0][0]
        #         src_data = src_data.lower()
        #         item_type = src_data.split('.', 1)[1]
        #         item_type_1 = item_type.split('.', 1)[0]
        #         item_type_2 = item_type.split('.', 1)[1:]
        #         item_type_2_str = ''.join(item_type_2)
        #         pack_proc_name = item_type_2_str.split('(', 1)[0]
        #         iter_id = iteration(xmlpath, projectid, app_data, 'CodeObjects')
        #         iter_id = iter_id[0][0]
        #         # print("iterid",iter_id)
        #         calling_func_proc_pack = insert_source_data(xmlpath,projectid,header_query_type, item_type_1, pack_proc_name,
        #                                                     ftfschemaname,app_data,iter_id)
        #
        #         if len(calling_func_proc_pack):
        #             app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
        #             iter_id = iteration(xmlpath, projectid, app_data, 'CodeObjects')
        #             iter = list(map(list, zip(*iter_id)))
        #             iter = iter[0][0]
        #             appli_db_src(xmlpath, pack_proc_name, header_query_type, calling_func_proc_pack, projectid, iter, app_data)
        #
        #     else:
        #         print("TestId not present in source table")
        #         logging.error("TestId not present in source table")

        logging.error("Execution of target detail pre or post query failed")
    finally:
        cursor.close()
    return data, exe_status, flag_st


# def inserting_data_into_table(db_transit_path, item_name, ftfschemaname):
#     procedure_function_name = ''
#     with open(db_transit_path + '/' + str(ftfschemaname).upper() + '/' + 'PG_PACKAGES' + '/' + 'packages.sql') as f:
#         read_text = f.read()
#         if 'create or replace procedure' in read_text:
#             split_with_procedure = read_text.split('create or replace procedure')[1:]
#             for i in split_with_procedure:
#                 all_procedure_name = i.strip().split(' ')[0]
#                 if item_name == all_procedure_name:
#                     procedure_function_name = 'create or replace procedure ' + i
#         elif 'create or replace function ' in read_text:
#             split_with_createfunction = read_text.split('create or replace function')[1:]
#             for funcs in split_with_createfunction:
#                 all_function_name = funcs.strip().split(' ')[0]
#                 if item_name == all_function_name:
#                     procedure_function_name = 'create or replace function ' + funcs
#     return procedure_function_name


## application connection for target select query execution
def appl_conn_select_header(id, connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute(
            "select test_case_name,object_type,code_object_name from prj_tgttestcasehdr where tgt_test_id=%s", [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


## application connection for target detail pre select query execution
def appl_conn_select_detail_pre(id,connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_srctestcasedtl where test_id=%s and qry_exec_type= 'Pre';",

    try:
        logging.info(
            "executing target detail pre select query for execution_qry,execution_output_singlevalue,execution_output_hashkey ")
        cursor.execute(
            "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_tgttestcasedtl where tgt_test_id=%s and qry_exec_type= 'Pre';",
            [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target detail pre select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


## application connection for target detail post select query execution
def appl_conn_select_detail_post(id, projectid, token_data, xmlpath, connection_type, connection_name):
    app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_srctestcasedtl where test_id=%s and qry_exec_type= 'Post';",

    try:
        logging.info(
            "executing target detail post select query for execution_qry,execution_output_singlevalue,execution_output_hashkey into target database ")
        cursor.execute(
            "select execution_qry,execution_output_singlevalue,execution_output_hashkey from prj_tgttestcasedtl where tgt_test_id=%s and qry_exec_type= 'Post';",
            [id])
        data = cursor.fetchall()
    except Exception as err:
        print(err)
        logging.error("Error while target detail post select query execution %s", err)
        data = None
    finally:
        cursor.close()
    return data


## application connection for target detail pre update query execution
def appl_conn_update_detail_pre(id, pre_single_count, pre_hash_value, exedate,connection):
    # print(id)
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info(
            "executing target detail pre update query for execution_output_singlevalue, execution_output_hashkey, exec_datetime  into target database")
        cursor.execute(
            "update prj_tgttestcasedtl set execution_output_singlevalue=%s, execution_output_hashkey=%s, exec_datetime=%s where tgt_test_id=%s and qry_exec_type= 'Pre';",
            (pre_single_count, pre_hash_value, exedate, id))
        print("excuted update query for pre detail table")
        logging.info("excuted update query for pre detail table")
    except Exception as err:
        print(err)
        logging.error("Error while target detail pre update query execution %s", err)
    finally:
        connection.commit()
        cursor.close()


## application connection for target detail pre update query execution
def appl_conn_update_detail_post(id, projectid, token_data, xmlpath, post_single_count, post_hash_value, exedate,
                                 connection_type, connection_name):
    # print(id)
    app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    logging.info("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info(
            " Executing target detail pre update query for execution_output_singlevalue, execution_output_hashkey, exec_datetime into target database")
        cursor.execute(
            "update prj_tgttestcasedtl set execution_output_singlevalue=%s, execution_output_hashkey=%s, exec_datetime=%s where tgt_test_id=%s and qry_exec_type= 'Post';",
            (post_single_count, post_hash_value, exedate, id))
    except Exception as err:
        print(err)
        logging.error("Error while target detail pre update query execution %s", err)
    finally:
        connection.commit()
        cursor.close()


# to check whether data is present in database
def check_id_in_target_table(projectid, token_data, xmlpath, connection_type, connection_name):
    # print(id)
    app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    connection = applica_db_connection_details(xmlpath, app_data)
    print("connected to the Application Database")
    cursor = connection.cursor()
    try:
        logging.info(
            "executing target detail pre update query for execution_output_singlevalue, execution_output_hashkey, exec_datetime  into target database")
        cursor.execute("select tgt_test_id from prj_tgttestcasehdr")
        data = cursor.fetchall()
        data_list = []
        for i in data:
            data_list.append(i[0])
        print("gathered ids from target header table")
        logging.info("excuted update query for pre detail table")
    except Exception as err:
        data_list=None
        print(err)
        logging.error("Error while getting target ids %s", err)
    finally:
        connection.commit()
        cursor.close()
    return data_list


def executing_queries(testid_list, xmlpath, projectid, token_data, connection_type, connection_name,app_connection,target_connection, ftfschemaname):
    ftfschemaname = ftfschemaname.lower()
    output_excelpath = path_PreO2p + "/" + ftfschemaname.upper() + '/' + 'FTF_EXCEL'
    if not os.path.exists(output_excelpath):
        os.makedirs(output_excelpath)
    reprt_path = output_excelpath + '/' + ftfschemaname + '_functional_TestingTarget.xlsx'
    res_ids = check_id_in_target_table(projectid, token_data, xmlpath, connection_type, connection_name)
    # print(res_ids)
    logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Target Execution Started',
               app_connection)
    with pd.ExcelWriter(reprt_path) as writer:
        for id in testid_list:
            res = detail_table(id, xmlpath, projectid, token_data, connection_type, connection_name, app_connection,target_connection,ftfschemaname,
                               writer)
    logging_db(projectid, xmlpath, 'Functional Testing Execution', 'Functional Testing Target Execution Completed',app_connection)

            # print(id)
            # if id in res_ids:
            #     res = detail_table(id, xmlpath, projectid, token_data, connection_type, connection_name, ftfschemaname,writer)
            # else:
            #     print('given id is not present in database')
            #     continue


def target_execution(xmlpath, projectid, token_data, connection_type, connection_name, testid_list, ftfschemaname):
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
                        filename=qmigrator_path + '/' + 'FUNCTIONAL_SRC_TGT' + '/' + 'functional_target.log',
                        filemode='a+')

    app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    app_connection = applica_db_connection_details(xmlpath, app_data)
    postgres_data = target_db_connection_call(token_data, projectid, connection_type, connection_name)
    target_connection = target_connection_details(postgres_data)
    # iter_id = iteration(xmlpath, projectid, app_connection, 'CodeObjects', ftfschemaname)
    # iter = list(map(list, zip(*iter_id)))
    # iter = iter[0][0]
    executing_queries(testid_list, xmlpath, projectid, token_data, connection_type, connection_name,app_connection,target_connection, ftfschemaname)


def matching_source_target_pre(id, test_case_name,test_group, source_dtl_id_list,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    print("testcasename", test_case_name)
    print("testgroup", test_group)
    print("sourceid", source_dtl_id_list)
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        if source_dtl_id_list == None:
            print(source_dtl_id_list, 'cahbcn asvh')
            return 1
        print("pre entrting match")
        print("===============================end====")
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute("""
        select
        case when
        (case when ps.execution_output_singlevalue in ('None', '0.0','0') then '0'
        else ps.execution_output_singlevalue
        end =
        coalesce (pt.execution_output_singlevalue, '0'))
        then 'Matched' else 'Not Matched'
        end as single_val_status,
        case when (case when ps.execution_output_hashkey in ('None', '0.0') then '0'
        when ps.execution_output_hashkey is null then '0'
        end)= coalesce (pt.execution_output_hashkey, '0') then 'Matched' else 'Not Matched' end
        as multi_val_status
        from public.prj_srctestcasedtl ps, public.prj_srctestcasehdr ps2,
        public.prj_tgttestcasedtl pt, public.prj_tgttestcasehdr pt2
        where ps.test_id = ps2.test_id
        and ps2.test_group = pt2.test_group
        and ps2.test_case_name = pt2.test_case_name
        and ps.id = pt.src_dtl_id
        and ps.qry_exec_type = pt.qry_exec_type
        and pt.tgt_test_id = pt2.tgt_test_id
        and pt.qry_exec_type = 'Pre'
        and pt2.test_group = %s
        and pt2.test_case_name =%s
        and pt.src_dtl_id = %s;
        """, [test_group, test_case_name, source_dtl_id_list])
        print(test_group, test_case_name, source_dtl_id_list, '-===================')
        data = cursor.fetchall()
        print(data)
    except Exception as err:
        print(err, "error matched")
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        connection.commit()
        cursor.close()
    return data


def matching_source_target_post(id, projectid, token_data, xmlpath, connection_type, connection_name, test_case_name,
                                test_group, source_dtl_id_list,connection):
    # app_data = appli_db_connection_call(token_data, projectid, connection_type, connection_name)
    # connection = applica_db_connection_details(xmlpath, app_data)
    # print("connected to the Application Database")
    # logging.info("connected to the Application Database")
    cursor = connection.cursor()
    # print("testcasename", test_case_name)
    # print("testgroup", test_group)
    # print("sourceid", source_dtl_id_list)
    # "select test_case_name,object_type,object_signature from prj_srctestcasehdr where test_id=%s;",
    try:
        if source_dtl_id_list == None:
            print(source_dtl_id_list, 'cahbcn asvh')
            return 1
        logging.info("executing target select query for test_case_name,object_type,object_signature")
        cursor.execute("""
        select
        case when
        (case when ps.execution_output_singlevalue in ('None', '0.0','0') then '0'
        else ps.execution_output_singlevalue
        end =
        coalesce (pt.execution_output_singlevalue, '0'))
        then 'Matched' else 'Not Matched'
        end as single_val_status,
        case when (case when ps.execution_output_hashkey in ('None', '0.0') then '0'
        when ps.execution_output_hashkey is null then '0'
        end)= coalesce (pt.execution_output_hashkey, '0') then 'Matched' else 'Not Matched' end
        as multi_val_status
        from public.prj_srctestcasedtl ps, public.prj_srctestcasehdr ps2,
        public.prj_tgttestcasedtl pt, public.prj_tgttestcasehdr pt2
        where ps.test_id = ps2.test_id
        and ps2.test_group = pt2.test_group
        and ps2.test_case_name = pt2.test_case_name
        and ps.id = pt.src_dtl_id
        and ps.qry_exec_type = pt.qry_exec_type
        and pt.tgt_test_id = pt2.tgt_test_id
        and pt.qry_exec_type = 'Post'
        and pt2.test_group = %s
        and pt2.test_case_name =%s
        and pt.src_dtl_id = %s;""", [test_group, test_case_name, source_dtl_id_list])
        print(test_group, test_case_name, source_dtl_id_list, '===========================')
        data = cursor.fetchall()
        # print(data)
        # data = "not matched"
    except Exception as err:
        print(err)
        logging.error("Error while target select query execution %s", err)
        data = None
    finally:
        connection.commit()
        cursor.close()
    return data
