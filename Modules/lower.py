import re
def lower(data):
    singlequoye = re.findall(r"\'.*?\'", data)
    extractpartsdictformat ={}
    if len(singlequoye):
        arya = 0
        for arc1 in singlequoye:
            data = data.replace(arc1, 'sngqx' + str(arya)+'sngqx', 1)
            extractpartsdictformat['sngqx' + str(arya)+'sngqx'] = arc1
            arya = arya + 1
    data = data.lower()

    if len(extractpartsdictformat):
        for barc in extractpartsdictformat:
            data = data.replace(barc, extractpartsdictformat[barc], 1)
    return data