import re

def to_number(data,sch):
    data = data.lower()
    data_between_sel_semi = re.findall(r'\bselect.*?;', data, flags=re.IGNORECASE | re.DOTALL)  # select to ;
    data_between_sel_insert = re.findall(r'\binsert.*?;', data, flags=re.IGNORECASE | re.DOTALL)  # insert to ;
    data_between_sel_delete = re.findall(r'\bdelete.*?;', data, flags=re.IGNORECASE | re.DOTALL)  # delete to;
    data_between_sel_update = re.findall(r'\bupdate.*?;', data, flags=re.IGNORECASE | re.DOTALL)  # update to;
    total_dml_commands = data_between_sel_semi + data_between_sel_insert + data_between_sel_delete + data_between_sel_update
    for i in total_dml_commands:
        if'to_number'in i and 'dual' in i:
            select_data_original = i
            select_data_replace = i
            i_replace = re.sub(r'\bto_number', '', i, flags=re.DOTALL | re.I)
            i_replace = re.sub(r'\bfrom.*?dual', ' ::numeric ', i_replace, flags=re.IGNORECASE).replace('(', '').replace( ')', '')
            select_data_replace = select_data_replace.replace(i, i_replace)
            data = data.replace(select_data_original, select_data_replace)
    return data.upper()