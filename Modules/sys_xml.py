import re
def sys_xml(sys_data):
    if 'sys.xmltype' in sys_data:
        sys_data = re.sub('sys.xmltype', 'xml', sys_data)
    return sys_data
