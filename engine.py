import csv
import sqlparse
import sys

class Meta(dict):
    def __init__(self):
        dict.__init__(self)
        self.fname = 'metadata.txt'
        self.read()

    def read(self):
        f = open(self.fname, 'r')
        isNextNewTable = False
        for line in f:
            l = line.strip()
            if l == "<begin_table>":
                isNextNewTable = True
            elif isNextNewTable:
                tableName = l
                self[tableName] = []
                isNextNewTable = False
            elif l != '<end_table>':
                self[tableName].append(l)

class Table(dict):
    def __init__(self, name):
        dict.__init__(self)
        self.name = name
        for col in meta[self.name]:
            self[col] = []
        self.read()

    def read(self):
        fname = self.name + '.csv'
        with open(fname, 'rb') as csvfile:
            csvr = csv.reader(csvfile)
            for row in csvr:
                for i in range(len(row)):
                    col_name = meta[self.name][i]
                    self[col_name].append(row[i])

class Query:
    def __init__(self, command):
        query = sqlparse.parse(command.strip())[0]
        if not str(query.tokens[0]).lower() == "select":
            raise NotImplementedError('Only select query type is supported')

        i = 2
        self.distinct = False
        if str(query.tokens[i]).lower() == 'distinct':
            self.distinct = True
            i += 2
        self.cols = list(query.tokens[i].get_identifiers())
        self.cols = [str(x) for x in self.cols]
        self.tables = list(query.tokens[i+4].get_identifiers())
        self.tables = [str(x) for x in self.tables]
        self.proper_meta()

    def proper_col(self, col):
        if '.' in col:
            colx = col.split('.')
            if not colx[0] in self.tables:
                raise Exception('Invalid column ' + col)
            if not colx[1] in meta[colx[0]]:
                raise Exception('Invalid column ' + col)
            return col
        for t in self.tables:
            if col in meta[t]:
                return t + '.' + col
        raise Exception('Invalid column ' + col)

    def proper_meta(self):
        for t in self.tables:
            if not t in meta:
                raise Exception('Invalid table ' + t)
        for i in range(len(self.cols)):
            self.cols[i] = self.proper_col(self.cols[i])

meta = Meta()
tables = {}
for table in meta:
    tables[table] = Table(table)
    # print(tables[table])
q = Query(sys.argv[1])
print(q.cols, q.tables, q.distinct)
