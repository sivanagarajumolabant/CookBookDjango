import re
def Proc_NVL2(Data):
  Data = re.sub(r' +', ' ', Data)
  Data = Data.replace('nvl (', 'nvl(')
  Data = Data.replace('nvl(', 'coalesce(')
  return Data