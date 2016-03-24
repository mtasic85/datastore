import os
import warnings


class MemTable(object):
    def __init__(self, db, cap=1000):
        self.db = db
        self.cap = cap
        self.items = {}
        self.event_full_func = None

    def __repr__(self):
        return '<{} db:{} cap:{}>'.format(self.__class__.__name__, self.db, self.cap)

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


class SSTable(object):
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


class DataStore(object):
    def __init__(self, dirpath, max_memtable_cap=1000):
        self.db = os.path.split(dirpath)[-1]
        self.dirpath = dirpath

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        # memtable
        self.max_memtable_cap = max_memtable_cap
        self.memtable = MemTable(self.db, max_memtable_cap)
        self.memtable.on_full(self._memtable_full)

        # sstable
        self.sstable = []

        for entry in os.scandir(dirpath):
            if entry.is_file() and entry.name.endswith('.sstable'):
                path = os.path.join(self.dirpath, entry.name)
                sstable = SSTable(self.db, path)
                self.sstables.append(sstable)
                print(path, sstable)

    def __repr__(self):
        return '<{} db:{}>'.format(self.__class__.__name__, self.db)

    def _memtable_full(self, memtable):
        print('_memtable_full', self, memtable)

        # sstable
        sstable = SSTable.from_memtable(self.db, memtable)
        self.sstable.append(sstable)
        
        # memtable
        self.memtable = MemTable(self.db, self.max_memtable_cap)
        self.memtable.on_full(self._memtable_full)

    def set(self, key, value):
        self.memtable.set(key, value)

    def get(self, key):
        value = self.memtable.get(key)
        return value

    def delete(self, key):
        self.memtable.delete(key)


class Model(object):
    def 


if __name__ == '__main__':
    d = DataStore('tmp/demo0', 10)

    UserModel = d.model(
        'user',
        username=TextField(),
        password=TextField(),
        first_name=TextField(),
        last_name=TextField(),
        email=TextField(),
        dob=DateField(),
        username_index=Index('username'),
        dob_index=Index('dob'),
        dob_email_index=Index('dob', 'email'),
    )

    results = UserModel.query(
        Or(
            Term('username', 'mtasic'),
            And(
                Le(Term('dob', '19850623')),
                Ge(Term('dob', '19890625')),
            )
        )
    )

    results = UserModel.query('username == "mtasic" OR (dob < "19850623" AND dob > "19890625")')

    for i in range(d.max_memtable_cap * 2 - 1):
        d.set(i, i)

    for i in range(d.max_memtable_cap * 2 - 1):
        try:
            v = d.get(i)
        except KeyError as e:
            print(KeyError, e)
            continue

        print('{}: {}'.format(i, v))
