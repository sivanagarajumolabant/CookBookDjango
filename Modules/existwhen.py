import re
def existwhen(data):
    """
    Taking the whole data from exit when to not found. and replacing the data between them as empty
    Example :: exit when the varchar has features not found ==> exit when not found     --Lakshmi 7/14
    """
    data = data
    find_data = re.findall(r'\bexit when\b(.*?)\bnotfound', data, re.DOTALL)
    if len(find_data):
        for i in find_data:
            data = data.replace(i, ' ')
    else:
        data = data

    return data
