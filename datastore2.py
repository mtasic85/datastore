__all__ = ['DataStore']
from bisect import bisect_left

DATASTORE_FLAG_SET = 1
DATASTORE_FLAG_DELETE = 2

class DataStore(object):
    def __init__(self, pk):
        self.pk = tuple(pk) # primary key
        
        # memtable
        self.MEM_TABLE_MAX_ITEMS = 4
        self.memtable = MemTable(datastore=self)

        # memtables
        self.memtables = []

    def push_memtable_if_necessary(self):
        if self.memtable.get_n_items() >= self.MEM_TABLE_MAX_ITEMS:
            # append current memtable to memtables
            # and make it immutable
            memtable = self.memtable
            self.memtables.insert(0, memtable)

            # create new memtable which is mutable
            self.memtable = MemTable(datastore=self)

    def get(self, key):
        try:
            doc = self.memtable.get(key)
        except KeyError as e:
            for memtable in self.memtables:
                try:
                    doc = memtable.get(key)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError('Key not found: {}'.format(key))

        return doc

    def set(self, key, doc):
        self.push_memtable_if_necessary()
        self.memtable.set(key, doc)

    def delete(self, key):
        self.push_memtable_if_necessary()
        self.memtable.delete(key)

    def add(self, doc):
        key = tuple(doc[k] for k in self.pk)
        self.set(key, doc)

    def remove(self, doc):
        key = tuple(doc[k] for k in self.pk)
        self.delete(key)

    def filter(self, *terms):
        for doc in self.memtable.filter(*terms):
            yield doc

        for memtable in self.memtables:
            for doc in memtable.filter(*terms):
                yield doc

        raise StopIteration

class MemTable(object):
    def __init__(self, datastore):
        self.datastore = datastore
        self.flags = []
        self.keys = []
        self.values = []

        self.indexes = {ppk: MemIndex(self.datastore, ppk) for ppk in self.datastore.pk}
        self.indexes[self.datastore.pk] = MemIndex(self.datastore, self.datastore.pk)

    def get_n_items(self):
        return len(self.values)

    def get(self, key):
        i = bisect_left(self.keys, key)

        if i == len(self.keys):
            raise KeyError('Key not found: {}'.format(key))

        # flag
        flag = self.flags[i]

        if flag == DATASTORE_FLAG_DELETE:
            raise KeyError('Key not found: {}'.format(key))

        # check key of document
        doc = self.values[i]
        doc_key = tuple(doc[k] for k in self.datastore.pk)
        
        if key != doc_key:
            raise KeyError('Key not found: {}'.format(key))

        return doc

    def set(self, key, doc):
        i = bisect_left(self.keys, key)

        if i == len(self.keys):
            self.flags.insert(i, DATASTORE_FLAG_SET)
            self.keys.insert(i, key)
            self.values.insert(i, doc)
        else:
            # check key
            old_key = self.keys[i]

            if old_key == key:
                self.flags[i] = DATASTORE_FLAG_SET
                self.values[i] = doc
            else:
                self.flags.insert(i, DATASTORE_FLAG_SET)
                self.keys.insert(i, key)
                self.values.insert(i, doc)

    def delete(self, key):
        i = bisect_left(self.keys, key)

        if len(self.keys) > 0 and self.keys[i] == key:
            self.flags[i] = DATASTORE_FLAG_DELETE
        else:
            # default empty doc
            doc = {}

            self.flags.insert(i, DATASTORE_FLAG_DELETE)
            self.keys.insert(i, key)
            self.values.insert(i, doc)

    def filter(self, *terms):
        raise StopIteration

class MemIndex(object):
    def __init__(self, datastore, ppk):
        self.datastore = datastore
        self.ppk = ppk
        self.flags = []
        self.keys = []

    def get(self, key):
        i = bisect_left(self.keys, key)

        if i == len(self.keys):
            raise KeyError('Key not found: {}'.format(key))

        # flag
        flag = self.flags[i]

        if flag == DATASTORE_FLAG_DELETE:
            raise KeyError('Key not found: {}'.format(key))

        return i

    def set(self, key, doc):
        i = bisect_left(self.keys, key)

        if i == len(self.keys):
            self.flags.insert(i, DATASTORE_FLAG_SET)
            self.keys.insert(i, key)
        else:
            # check key
            old_key = self.keys[i]

            if old_key == key:
                self.flags[i] = DATASTORE_FLAG_SET
            else:
                self.flags.insert(i, DATASTORE_FLAG_SET)
                self.keys.insert(i, key)

        return i

    def delete(self, key):
        i = bisect_left(self.keys, key)

        if len(self.keys) > 0 and self.keys[i] == key:
            self.flags[i] = DATASTORE_FLAG_DELETE
        else:
            # default empty doc
            doc = {}

            self.flags.insert(i, DATASTORE_FLAG_DELETE)
            self.keys.insert(i, key)

        return i

class SSTable(object):
    def __init__(self, datastore):
        self.datastore = datastore

    def get(self, key):
        pass

    def set(self, key, doc):
        pass

    def delete(self, key):
        pass

if __name__ == '__main__':
    ds = DataStore(pk=['a', 'b', 'c'])
    
    for i in range(0, 3):
        for j in range(0, 3):
            for k in range(0, 3):
                doc = {'a': i, 'b': float(j), 'c': str(k), 'd': '{}-{}-{}'.format(i, j, k)}
                ds.add(doc)

    key = (1, 1.1, '1')
    # print(ds.get(key))

    try:
        print(ds.get(key))
    except KeyError as e:
        print(e)

    key = (1, 1.0, '1')
    print(ds.get(key))

    key = (2, 1.0, '2')
    ds.delete(key)

    key = (2, 2.0, '2')
    ds.delete(key)

    key = (1, 1.0, '1')
    ds.delete(key)

    try:
        print(ds.get(key))
    except KeyError as e:
        print(e)

    key = (1, 1.0, '1')
    doc = {'a': 1, 'b': 1.0, 'c': '1', 'd': 'SOME NEW VALUE'}
    ds.set(key, doc)

    key = (1, 1.0, '1')
    print(ds.get(key))

    """
    """
    # debug
    print('-' * 24)
    
    for n in zip(ds.memtable.flags, ds.memtable.keys, ds.memtable.values):
        print(n)
    
    for memtable in ds.memtables:
        print('-' * 24)

        for n in zip(memtable.flags, memtable.keys, memtable.values):
            print(n)
    """
    """

    docs = ds.filter(('a', '>=', 1), ('a', '<=', 2))
    print(docs)

    for doc in docs:
        print(doc)
