import re 

def spacehandling(space_handling):
    ''' Handling multiple spaces '''
    space_handling= re.sub(' +', ' ', space_handling)
    space_handling= re.sub(r'\s+\.', '.', space_handling)
    space_handling= re.sub(r'\.\s+', '.', space_handling)
    space_handling= re.sub(r'\s+;', ';', space_handling)
    space_handling= space_handling.replace('.', ' .')
    space_handling= re.sub(r'\bis\s+table\s+of\s+', 'is table of ', space_handling)
    space_handling= re.sub(r'\b%rowtype\s+index\s+by\s+', '%rowtype index by ', space_handling)
    return space_handling