import csv

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

meta = Meta()
t1 = Table('table1')
print(t1)
