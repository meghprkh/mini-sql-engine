import copy
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

class Table(list):
    def __init__(self, name):
        list.__init__(self)
        self.name = name
        self.nrows = 0
        self.read()

    def read(self):
        fname = self.name + '.csv'
        with open(fname, 'rb') as csvfile:
            csvr = csv.reader(csvfile)
            for row in csvr:
                r = {}
                for i in range(len(row)):
                    col_name = meta[self.name][i]
                    r[col_name] = row[i]
                self.append(r)
            self.nrows += 1

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
        self.join_tables()

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

    def recurse_join(self, ttj):
        nts = []
        table = ttj[0]
        if len(ttj) == 1:
            for row in tables[table]:
                rts = {}
                for col in row:
                    rts[table + '.' + col] = row[col]
                nts.append(rts)
            return nts
        ots = self.recurse_join(ttj[1:])
        for row in tables[table]:
            for row2 in ots:
                rts = copy.deepcopy(row2)
                for col in row:
                    rts[table + '.' + col] = row[col]
                nts.append(rts)
        return nts

    def join_tables(self):
        self.nt = self.recurse_join(self.tables)
        # print(self.nt, len(self.nt))

meta = Meta()
tables = {}
for table in meta:
    tables[table] = Table(table)
    # print(tables[table])
q = Query(sys.argv[1])
print(q.cols, q.tables, q.distinct)
