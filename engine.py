#!/usr/bin/env python

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
                    r[col_name] = int(row[i])
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
        if type(query.tokens[i+4]).__name__ == 'Identifier':
            self.tables = [str(query.tokens[i+4])]
        else:
            self.tables = list(query.tokens[i+4].get_identifiers())
            self.tables = [str(x) for x in self.tables]
        self.validate_tables()
        self.colsaggcol, self.colsaggfn = [], []
        if str(query.tokens[i]) == '*':
            self.cols = [table + '.' + col for table in self.tables for col in meta[table]]
        else:
            if type(query.tokens[i]).__name__ != 'IdentifierList':
                self.cols = [query.tokens[i]]
            else:
                self.cols = list(query.tokens[i].get_identifiers())
            colsn = []
            for col in self.cols:
                if type(col).__name__ == 'Function':
                    fn_done = False
                    for token in col.tokens:
                        if type(token).__name__ == 'Identifier':
                            if not fn_done:
                                self.colsaggfn.append(str(token).lower())
                                fn_done = True
                            else:
                                self.colsaggcol.append(self.proper_col(str(token)))
                                break
                        elif type(token).__name__ == 'Parenthesis':
                            col.tokens += token.tokens[1:-1]
                    colsn.append(self.colsaggfn[-1] + '(' + self.colsaggcol[-1] + ')')
                else:
                    colsn.append(str(col))
            self.cols = colsn
        self.where = []
        if len(query.tokens) > i+6:
            self.where = query.tokens[i+6].tokens
            if not str(self.where[0]).lower() == "where":
                raise NotImplementedError('Only where is supported' + str(self.where))
            self.where = self.where[2:]
        self.validate_cols()
        self.join_tables()
        self.resolve_where()
        self.resolve_distinct()
        self.resolve_aggregate()

    def test_row(self, row, c):
        prev = True
        prevand = True
        for i, condition in enumerate(c):
            ns = None # if set, means new status is there
            if str(condition.ttype) == 'Token.Text.Whitespace':
                continue
            elif str(condition.ttype) == 'Token.Keyword':
                if str(condition).lower() == 'or':
                    prevand = False
                elif str(condition).lower() == 'and':
                    prevand = True
            elif type(condition).__name__ == 'Parenthesis':
                ns = self.test_row(row, condition.tokens[1:-1])
            elif type(condition).__name__ == 'Comparison':
                tokens = condition.tokens
                iden1 = None
                iden2 = None
                op = None
                value = None
                for token in tokens:
                    if token.ttype is not None: # is token
                        if str(token.ttype) == 'Token.Operator.Comparison':
                            op = str(token)
                        elif str(token.ttype).startswith('Token.Literal'):
                            value = int(str(token))
                    elif type(token).__name__ == 'Identifier':
                        if iden1 is not None:
                            iden2 = self.proper_col(str(token))
                        else:
                            if value is not None:
                                op = self.reverseop(op)
                            iden1 = self.proper_col(str(token))
                if iden1 is None: # Nothing to compare anything to
                    ns = True
                elif iden2 is not None: # Join
                    if iden2 in self.cols: # Remove dup col
                        self.cols.remove(iden2)
                    ns = self.applyop(row[iden1], op, row[iden2])
                else:
                    ns = self.applyop(row[iden1], op, value)

            if not ns is None:
                if prevand:
                    prev = prev and ns
                else:
                    prev = prev or ns
        return prev

    def reverseop(self, op):
        if op == '<':
            return '>'
        elif op == '>':
            return '<'
        elif op == '<=':
            return '>='
        elif op == '>=':
            return '<='
        return op

    def applyop(self, v1, op, v2):
        v1, v2 = int(v1), int(v2)
        if op == '=':
            return v1 == v2
        elif op == '<':
            return v1 < v2
        elif op == '>':
            return v1 > v2
        elif op == '<=':
            return v1 <= v2
        elif op == '>=':
            return v1 >= v2
        else:
            raise NotImplementedError(op + ' operator not recognized')

    def resolve_where(self):
        if len(self.where) == 0:
            return
        nnt = []
        for i, row in enumerate(self.nt):
            status = self.test_row(row, self.where)
            if status:
                nnt.append(row)
        self.nt = nnt

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

    def validate_tables(self):
        for t in self.tables:
            if not t in meta:
                raise Exception('Invalid table ' + t)

    def validate_cols(self):
        for i in range(len(self.cols)):
            if '(' in self.cols[i]:
                continue
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

    def resolve_distinct(self):
        if not self.distinct:
            return
        nnt = []
        s = set()
        for row in self.nt:
            tp = str([row[col] for col in self.cols])
            if not tp in s:
                s.add(tp)
                nnt.append(row)
        self.nt = nnt

    def resolve_aggregate(self):
        if len(self.nt) == 0 or len(self.colsaggfn) == 0:
            return
        for i, fn in enumerate(self.colsaggfn):
            col = self.colsaggcol[i]
            fullname = fn + '(' + col + ')'
            v = self.nt[0][col]
            for row in self.nt:
                if fn == 'sum' or fn == 'average' or fn == 'avg':
                    v += row[col]
                elif fn == 'min':
                    v = min(v, row[col])
                elif fn == 'max':
                    v = max(v, row[col])
                else:
                    raise NotImplementedError('Function %s not implemented' % fn)
            if fn == 'avg' or fn == 'average':
                v /= len(self.nt)
            for row in self.nt:
                row[fullname] = v
        if len(self.nt) > 1:
            self.nt = [self.nt[0]]

    def print_result(self):
        writer = csv.DictWriter(sys.stdout, self.cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(self.nt)

meta = Meta()
tables = {}
for table in meta:
    tables[table] = Table(table)
    # print(tables[table])
q = Query(sys.argv[1])
# print(q.cols, q.tables, q.distinct)
q.print_result()
