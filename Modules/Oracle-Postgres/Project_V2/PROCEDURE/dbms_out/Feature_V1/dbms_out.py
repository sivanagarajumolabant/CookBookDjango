import re
def dbms_out(data,sch):
    data=data.lower()
    data=re.sub(r' +',' ',data,flags=re.DOTALL|re.I)
    data=re.sub(r'end\s+;','end;',data,flags=re.DOTALL|re.I)
    create=re.findall(r'\bcreate\b.*?\bend;',data,flags=re.DOTALL|re.I)
    for i in create:
        db=re.findall(r'\bdbms.*?;',i,flags=re.DOTALL|re.I)
        for d in db:
            if ' dbms_output_line'or'dbms_output.put_line'or'dbms_output.put_line' in d:
                i1 = re.sub(r'\bdbms_output_line', 'dbms_output_line ', d, flags=re.DOTALL | re.I)
                i1 = re.sub(r'\bdbms_output_line','raise notice ', i1, flags=re.DOTALL | re.I)
                i1=re.sub(r'\bdbms_output.put_line','raise notice ',i1,flags=re.DOTALL|re.I)
                i1=re.sub(r'\bdbms_output.put_line|dbms_output.get_line','raise notice ',i1,flags=re.DOTALL|re.I)
                i1=re.sub(r'\s+\(',' ',i1,flags=re.DOTALL)
                i1=re.sub(r'\);|\)\s+;',';',i1,flags=re.DOTALL)
            data=data.replace(d,i1)
    return data.upper()