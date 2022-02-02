def row_id(row_id_str):
    if 'rowid' in row_id_str:
        split_begin_end = row_id_str.split('begin', 1)[1].rsplit('end', 1)
        for data in split_begin_end:
            if 'rowid' in data:
                split_begin_end = data.replace('rowid', 'ctid')
                row_id_str = row_id_str.replace(data, split_begin_end)
        # print(row_id_str)
    return row_id_str