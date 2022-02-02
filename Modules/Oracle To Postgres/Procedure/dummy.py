import sys
class PythonIndenter:

    def __init__(self, fpi = sys.stdin, fpo = sys.stdout,
                 indentsize = STEPSIZE, tabsize = TABSIZE, expandtabs = EXPANDTABS):
        self.fpi = fpi
        self.fpo = fpo
        self.indentsize = indentsize
        self.tabsize = tabsize
        self.lineno = 0
        self.expandtabs = expandtabs
        self._write = fpo.write
        self.kwprog = re.compile(
                r'^\s*(?P<kw>[a-z]+)'
                r'(\s+(?P<id>[a-zA-Z_]\n*))?'
                r'[^\n]')
        self.endprog = re.compile(
                r'^\s*#?\s*end\s+(?P<kw>[a-z]+)'
                r'(\s+(?P<id>[a-zA-Z_]\w*))?'
                r'[^\n]')
        self.wsprog = re.compile(r'^[ \t]*')
    # end def __init__
    def write(self, line):
        if self.expandtabs:
            self._write(line.expandtabs(self.tabsize))
        else:
            self._write(line)
        # end if
    # end def write
    def readline(self):
        line = self.fpi.readline()
        if line: self.lineno = self.lineno + 1
        # end if
        return line
