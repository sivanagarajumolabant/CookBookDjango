import re

def default_null(data):
    data = data.lower()
    data = re.sub(r'\)\s+is', ')is', data, flags=re.DOTALL | re.I)
    data = re.sub(r'\)\s+as', ')as', data, flags=re.DOTALL | re.I)
    data = re.sub(r'\s+,', ',', data, flags=re.DOTALL | re.I)
    dt = ''
    lst = re.findall(r'\((.*?)\)as|\((.*?)\)is', data, flags=re.DOTALL | re.I)
    if lst:
        lst = lst[0]
    for x in lst:
        y = re.sub(r'\s+', ' ', x, flags=re.I | re.DOTALL)
        ab = 0
        yy = y.split(',')
        for l, a in enumerate(yy):
            if l + 1 != len(yy):
                a = a + ','
            a1 = a.replace(':=', 'default').replace('out', 'inout')
            if ' default ' in a1:
                ab = ab + 1
            if ab > 0:
                if ' default ' not in a1:
                    a1 = a1.replace(',', ' default null,')
                    if l + 1 == len(yy):
                        a1 = a1 + ' default null '
            dt = dt + a1 + '\n'
    data = re.sub(r'\((.*?)\)as', '(' + dt + ')as', data, flags=re.DOTALL | re.I)
    data = re.sub(r'\((.*?)\)is', '(' + dt + ')is', data, flags=re.DOTALL | re.I)
    return data