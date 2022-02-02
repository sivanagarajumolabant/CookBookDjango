import re
def NVL(Data):
 Data = re.sub(r' +', ' ', Data)
 Data = Data.replace('nvl (', 'nvl(')
 Data = Data.replace('nvl(', 'coalesce(')
 return Data