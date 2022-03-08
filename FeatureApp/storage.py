import re
from django.utils.encoding import force_str
from django.utils.functional import keep_lazy_text
from django.core.files.storage import FileSystemStorage

@keep_lazy_text
def get_valid_filename(s):
    s = force_str(s).strip()
    return re.sub(r'(?u)[^-\w. ]', '', s)

class CleanFileNameStorage(FileSystemStorage):

    def get_valid_name(self, name):
        print(name)
        print(get_valid_filename(name))
        return get_valid_filename(name)
