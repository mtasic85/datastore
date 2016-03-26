import os
import warnings


class TableMeta(object):
    def __init__(self, ds):
        self.ds = ds


class MemTableData(object):
    def __init__(self, table, cap=1000):
        self.table = table
        self.cap = cap
        self.items = []
        self.event_full_func = None

    def __repr__(self):
        return '<{} table:{} cap:{}>'.format(self.__class__.__name__, self.table, self.cap)

    def set(self, key, value):
        self.items[key] = value

        if len(self.items) == self.cap:
            if self.event_full_func:
                self.event_full_func(self)
            else:
                warnings.warn('max capacity reached for memtable: {}'.format(self))

    def get(self, key):
        value = self.items[key]
        return value

    def delete(self, key):
        del self.items[key]

    def on_full(self, func):
        self.event_full_func = func


class TableData(object):
    def __init__(self, db, path):
        self.db = db
        self.path = path

    def __repr__(self):
        return '<{} db:{} path:{}>'.format(self.__class__.__name__, self.db, self.path)

    @classmethod
    def from_memtable(cls, db, memtable):
        path = None
        sstable = SSTable(db, path)
        return sstable

    def set(self, key, value):
        pass

    def get(self, key):
        pass

    def delete(self, key):
        pass


class Table(object):
    def __init__(self, ds, name):
        self.ds = ds
        self.name = name

        self.table_meta = TableMeta(self) # look for .meta file
        self.mem_table_data = MemTableData(self)
        self.table_data_items = [] # scan for .data files
        self.table_index_items = [] # scan for .index files

    def set(self, key, value):
        self.mem_table_data.set(key, value)

    def get(self, key):
        value = self.mem_table_data.get(key)
        return value

    def delete(self, key):
        self.mem_table_data.delete(key)


class DataStore(object):
    def __init__(self, dirpath, max_memtable_cap=1000):
        self.db = os.path.split(dirpath)[-1]
        self.dirpath = dirpath

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        # database tables
        self.tables = {}

        # # memtable
        # self.max_memtable_cap = max_memtable_cap
        # self.memtable = MemTable(self.db, max_memtable_cap)
        # self.memtable.on_full(self._memtable_full)

        # # sstable
        # self.sstable = []

        # for entry in os.scandir(dirpath):
        #     if entry.is_file() and entry.name.endswith('.sstable'):
        #         path = os.path.join(self.dirpath, entry.name)
        #         sstable = SSTable(self.db, path)
        #         self.sstables.append(sstable)
        #         print(path, sstable)

    def __repr__(self):
        return '<{} db:{}>'.format(self.__class__.__name__, self.db)

    def table(self, name, **fields):
        t = Table(self, name, **fields)
        self.tables[name] = t
        return t

    # def _memtable_full(self, memtable):
    #     print('_memtable_full', self, memtable)

    #     # sstable
    #     sstable = SSTable.from_memtable(self.db, memtable)
    #     self.sstable.append(sstable)
        
    #     # memtable
    #     self.memtable = MemTable(self.db, self.max_memtable_cap)
    #     self.memtable.on_full(self._memtable_full)


class Table(object):
    def __init__(self, name, **fields):
        self.name = name
        self.fields = fields

    def execute(self, q):
        pass

if __name__ == '__main__':
    d = DataStore('tmp/demo0', 10)

    User = d.table(
        'user',
        username=TextField(primary_key=True),
        password=TextField(primary_key=True),
        created=DatetimeField(primary_key=True),
        first_name=TextField(),
        last_name=TextField(),
        email=TextField(),
        dob=DateField(),
        username_dob_index=Index('username', 'dob'),
        dob_email_index=Index('dob', 'email'),
    )

    results = User.execute(
        Or(
            Term('username', 'mtasic'),
            And(
                Le(Term('dob', '19850623')),
                Ge(Term('dob', '19890625')),
            )
        )
    )

    results = User.execute('username == "mtasic" OR (dob < "19850623" AND dob > "19890625")')

    for i in range(d.max_memtable_cap * 2 - 1):
        d.set(i, i)

    for i in range(d.max_memtable_cap * 2 - 1):
        try:
            v = d.get(i)
        except KeyError as e:
            print(KeyError, e)
            continue

        print('{}: {}'.format(i, v))
