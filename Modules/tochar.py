import re

def tochar(data):
    if 'to_char' in data:
        data = re.sub(r' +', ' ', data)
        data = re.sub(r'to_char\s+\(', "to_char(", data)
        data = re.sub(r'to_char\(\s+select', "to_char(select", data)
        data = re.sub(r'to_char\(select\s+1', "to_char(select1", data)
        data = re.sub(r'to_char\(select1\s+\)', "to_char(select1)", data)
        data = re.sub(r'to_char\(select1\)', "'1'", data)
    return data