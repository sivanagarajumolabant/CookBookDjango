import re


# Connect by prior assaigned by nagaraju 16_8_2021
def connect_prior(connect_by_prior,schema):
    connect_by_prior = re.sub(r' +', ' ', connect_by_prior)
    connect_by_prior = connect_by_prior.replace(':=', ' := ')
    if 'connect by prior' in connect_by_prior:
        connect_by_prior = re.sub(r'\s+\=\s+', '=', connect_by_prior)
        connect_by_prior = re.sub(r'connect\s+by\s+prior', 'connect by prior', connect_by_prior)
        connect_by_prior = re.sub(r'\,\s+level', ',level', connect_by_prior)
        connect_by_prior = re.sub(r'is\s+null', 'is null', connect_by_prior)
        connect_by_prior = re.sub(r' +', ' ', connect_by_prior)
        select_semi_data = re.findall(r'select.*?;', connect_by_prior, re.DOTALL)
        for select_data in select_semi_data:
            count_select = select_data.count('select')
            if count_select < 2:
                if 'from' in select_data:
                    # Check the "CONNECT BY PRIOR "  keyword between "FROM" and ";". If it is there then only do other steps
                    from_semi = re.findall(r'\bfrom\b(.*?);', select_data, re.DOTALL)[0]
                    if 'connect by prior' in from_semi:
                        # Take the data between select and from except ,level and stored in a variable
                        select_semi = re.findall(r'\bselect\b(.*?)from', select_data, re.DOTALL)[0]
                        select_from_store = select_semi.replace(',level', '')
                        # Take the word after the "FROM"  and stored in a variable
                        from_data = re.findall(r'\bfrom\s+\S+', select_data)[0]
                        from_data_store = from_data.split()[1]
                        # Step4
                        # Check the "START WITH" keyword ,if it is there following below steps
                        # 1. If it is there adding single quotation and store it into variable
                        # Three Condition are present
                        # 1. p_first_Condition  =  'ENAME =''KING'''
                        # 2. p_first_Condition =  'EMPNO = 101'
                        # 3. p_first_Condition = 'parent_id IS NULL'
                        # If  "START WITH" keyword not present then add null
                        start_with = re.findall(r'start\s+with\s+\S+\s+\S+\s+\S+', select_data)
                        if len(start_with):
                            if 'is null' in start_with[0]:
                                start_with_store = "'" + start_with[0].replace('start with', '') + "'"
                            else:
                                start_with_store = start_with[0].split()[2]
                                if "'" in start_with_store:
                                    start_with_store = start_with_store.replace("'", "''")
                                    start_with_store = "'" + start_with_store + "'"
                                else:
                                    start_with_store = "'" + start_with_store + "'"
                        else:
                            start_with_store = 'null'
                        # Take the condition after the 'CONNECT BY PRIOR' and store into p_second_condition variables.
                        connect_by = re.findall(r'connect by prior\s+\S+', select_data)[0]
                        connect_by_prior_store = connect_by.split()[-1]
                        # if start_with_store =='null':
                        select_public_statement = 'select public.CONNECT_BY_PRIOR(' + "'" + select_from_store.strip() + "'," + "'" + from_data_store.strip() + "'," + start_with_store.strip() + "," + \
                                                  "'" + connect_by_prior_store.strip() + "'" + ');'
                        # else:
                        #     select_public_statement = 'select public.CONNECT_BY_PRIOR(' + "'" + select_from_store.strip() + "'," + "'" + from_data_store.strip() + "'," + "'" + start_with_store.strip() + "'," + \
                        #                               "'" + connect_by_prior_store.strip() + "'" + ');fetch all from dataset;'
                        connect_by_prior = connect_by_prior.replace(select_data, select_public_statement)
    # print(connect_by_prior)
    return connect_by_prior

