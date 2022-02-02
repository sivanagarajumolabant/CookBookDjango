def nvarchar(nvarchar_str):
    # nvarchar_str = nvarchar_str.lower()
    '''
    Replacing the nvarchar2(),nvarchar(),nvarchar2,nvarchar with 'TEXT'
    '''
    nvarchar_str = re.sub(' +', ' ', nvarchar_str)
    # print(nvarchar_str)
    nvarchar_str = re.sub(r'nvarchar2\s+\(', 'nvarchar2(', nvarchar_str)
    nvarchar_str = re.sub(r'nvarchar\s+\(', 'nvarchar(', nvarchar_str)
    find_nvarchar2 = re.findall(r'nvarchar2\(.*?\)', nvarchar_str, re.DOTALL)
    find_nvarchar3 = re.findall(r'nvarchar\(.*?\)', nvarchar_str, re.DOTALL)
    for i in find_nvarchar2:
        nvarchar_str = nvarchar_str.replace(i, 'text')
    for j in find_nvarchar3:
        nvarchar_str = nvarchar_str.replace(j, 'text')
    nvarchar_str = nvarchar_str.replace('nvarchar2', 'text').replace('nvarchar', 'text')
    return nvarchar_str