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

meta = Meta()
print(meta)
