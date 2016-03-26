import os
import sys
import json
import warnings

__all__ = ['DataStore']


class Field(object):
    def __init__(self, name, primary_key=False):
        self.name = name


class BoolField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class IntField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class FloatField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class TextField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class DateField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class TimeField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class DateTimeField(Field):
    def __init__(self, name, primary_key=False):
        Field.__init__(self, name=name, primary_key=primary_key)


class Index(object):
    def __init__(self, columns):
        self.columns = columns


class TableMeta(object):
    def __init__(self, table, fields=None):
        self.table = table
        self.fields = fields

        self.path = os.path.join(
            self.table.ds.dirpath,
            '{}.meta'.format(self.table.name),
        )

        if self.fields:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    meta = json.load(f)
                    fields = meta.get('fields', None)
                    # compare fields
            else:
                with open(path, 'w') as f:
                    meta = {
                        'name': self.table.name,
                        'fields': self.fields,
                    }

                    json.dump(meta, f)
        else:
            # look for .meta file (JSON)
            if os.path.exists(path):
                with open(path, 'r') as f:
                    meta = json.load(f)
                    self.fields = meta.get('fields', None)

class MemIndex(object):
    def __init__(self, mem_table, columns):
        self.mem_table = mem_table
        self.columns = columns
        self.items = []

    def add(self, row):
        # columns: pos_in_mem_table
        pass

    def execute(self, q):
        pass


class MemTable(object):
    def __init__(self, table, cap=1000, on_full=None):
        self.table = table
        self.cap = cap
        self.items = []
        self.on_full_callback = on_full

    def __repr__(self):
        return '<{} table:{} len:{} cap:{}>'.format(
            self.__class__.__name__,
            self.table,
            len(self.cap),
            self.cap,
        )

    def set(self, key, value):
        # FIXME:
        self.items[key] = value

        if len(self.items) == self.cap:
            if self.on_full_callback:
                self.on_full_callback(table=self.table, mem_table=self)
            else:
                warnings.warn('max capacity reached for memtable: {}'.format(self))

    def get(self, key):
        # FIXME:
        value = self.items[key]
        return value

    def delete(self, key):
        # FIXME:
        del self.items[key]

    def on_full(self, func):
        self.on_full_callback = func


class FileIndex(object):
    def __init__(self, file_table, columns, path):
        self.file_table = file_table
        self.path = path

        # FIXME: mmap file

    def __repr__(self):
        return '<{} table:{} path:{}>'.format(
            self.__class__.__name__,
            self.table,
            self.path,
        )

    def execute(self, q):
        pass


class FileTable(object):
    def __init__(self, table, path):
        self.table = table
        self.path = path

        # FIXME: mmap file

    def __repr__(self):
        return '<{} table:{} path:{}>'.format(
            self.__class__.__name__,
            self.table,
            self.path,
        )

    @classmethod
    def from_memtable(cls, table, mem_table):
        file_table = FileTable(table, mem_table)
        return file_table

    def get(self, key):
        pass


class Table(object):
    def __init__(self, ds, name, fields=None, mem_table_cap=1000):
        self.ds = ds
        self.name = name

        self.meta = TableMeta(table=self, fields=fields)
        self.mem_table = MemTable(table=self, mem_table_cap=mem_table_cap, on_full=self._mem_full)
        self.mem_index = MemIndex(table=self)
        self.file_tables = [] # scan for .data files
        self.file_indexes = [] # scan for .index files

        # scan datastore dir for data files
        for entry in os.scandir(self.ds.dirpath):
            if entry.is_file() and entry.name.endswith('.data'):
                path = os.path.join(self.dirpath, entry.name)
                file_table = FileTable(table=self, path=path)
                self.file_tables.append(file_table)
                print(path, file_table)

    def fields(self, fields):
        self.meta = TableMeta(table=self, fields=fields)
        return self

    def set(self, key, value):
        self.mem_table.set(key, value)

    def get(self, key):
        value = self.mem_table.get(key)
        return value

    def delete(self, key):
        self.mem_table.delete(key)

    def _mem_full(self, table, mem_table):
        print('_mem_full', self, memtable)

        # table
        table_data = TableData.from_mem_table(self.db, memtable)
        self.sstable.append(sstable)

        # memtable
        self.memtable = MemTable(self.db, self.max_memtable_cap)
        self.memtable.on_full(self._memtable_full)

    def execute(self, q):
        pass


class DataStore(object):
    def __init__(self, dirpath):
        self.dirpath = os.path.abspath(dirpath)

        if not os.path.exists(self.dirpath):
            msg = 'datastore does not exist at {}'.format(repr(self.dirpath))
            msg += ', so it will be created.'
            warnings.warn(msg)
            os.makedirs(dirpath)

        # database tables
        self.tables = {}

    def __repr__(self):
        return '<{} dirpath:{}>'.format(
            self.__class__.__name__,
            repr(self.dirpath),
        )

    def table(self, name, fields=None, mem_table_cap=1000):
        t = Table(self, name, fields, mem_table_cap=mem_table_cap)
        self.tables[name] = t
        return t


if __name__ == '__main__':
    d = DataStore('tmp/demo0')

    User = d.table('User', mem_table_cap=10).fields(
        username=TextField(primary_key=True),
        password=TextField(primary_key=True),
        created=DateTimeField(primary_key=True),
        first_name=TextField(),
        last_name=TextField(),
        email=TextField(),
        dob=DateField(),
        username_dob_index=Index('username', 'dob'),
        dob_email_index=Index('dob', 'email'),
    )

    # results = User.execute(
    #     Or(
    #         Term('username', 'mtasic'),
    #         And(
    #             Le(Term('dob', '19850623')),
    #             Ge(Term('dob', '19890625')),
    #         )
    #     )
    # )

    # results = User.execute('username == "mtasic" OR (dob < "19850623" AND dob > "19890625")')

    for i in range(User.mem_table.mem_table_cap * 2 - 1):
        d.set(i, i)

    for i in range(User.mem_table.mem_table_cap * 2 - 1):
        try:
            v = d.get(i)
        except KeyError as e:
            print(KeyError, e)
            continue

        print('{}: {}'.format(i, v))
