import re
def NVL3(Data):
 Data = re.sub(r' +', ' ', Data)
 Data = Data.replace('nvl (', 'nvl(')
 Data = Data.replace('nvl(', 'coalesce(')
 return Data