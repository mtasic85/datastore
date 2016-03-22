__all__ = ['DataStore']
import os
import re

class DataStore(object):
    def __init__(self, path, name, primary_key_columns, columns):
        self.path = path
        self.name = name
        self.primary_key_columns = primary_key_columns
        self.columns = columns
        self.memtable = MemTable(self)

        # create dir for datastore
        self.dir_path = os.path.join(self.path, self.name)
        os.makedirs(self.dir_path)
        self.revisons = []

        for entry in os.scandir(self.dir_path):
            if not entry.is_file():
                continue

            if not entry.name.endswith('.sstable'):
                continue

            revison = re.findall(r'\b\d+\b', entry.name)[0]
            self.revisons.append(revison)

        self.revisons.sort()

    def close(self):
        self.memtable.close()

    def add(self, doc):
        self.memtable.add(doc)

    def get(self, *key):
        doc = self.memtable.get(key)
        return doc

    def has(self, *key):
        has = self.memtable.has(key)
        return has

    def remove(self, *key):
        self.memtable.remove(key)

class MemTable(object):
    MAX_DOCS = 100

    def __init__(self, data_store):
        self.data_store = data_store
        self.primary_key_columns = self.data_store.primary_key_columns
        self.docs = {}

    def close(self):
        pass

    def add(self, doc):
        key = tuple([doc[n] for n in self.primary_key_columns])
        self.docs[key] = doc

    def get(self, key):
        doc = self.docs[key]
        return doc

    def has(self, key):
        has = key in self.docs
        return has

    def remove(self, key):
        del self.docs[key]

class SSTable(object):
    def __init__(self, data_store, revision):
        self.data_store = data_store
        self.revision = revision

    @classmethod
    def create_sstable(cls, data_store, revision):
        pass

class PrimaryKey(object):
    def __init__(self, data_store, revision):
        self.data_store = data_store
        self.revision = revision

class Index(object):
    def __init__(self, data_store, revision):
        self.data_store = data_store
        self.revision = revision

class BloomFilter(object):
    def __init__(self, data_store, revision):
        self.data_store = data_store
        self.revision = revision

if __name__ == '__main__':
    ds = DataStore(os.path.join('tmp', 'store0'), ['id0', 'id1'], ['first_name', 'last_name'])
    
    ds.add({'id0': 1, 'id1': 2, 'first_name': 'Marko', 'last_name': 'Tasic'})
    ds.add({'id0': 2, 'id1': 3, 'first_name': 'Milica', 'last_name': 'Tasic'})
    ds.add({'id0': 3, 'id1': 4, 'first_name': 'Milos', 'last_name': 'Milosevic'})
    ds.add({'id0': 4, 'id1': 5, 'first_name': 'Milan', 'last_name': 'Zdravkovic'})
    
    print(ds.get(1, 2))
    print(ds.get(4, 5))
    print(ds.has(2, 3))
    ds.remove(2, 3)
    print(ds.has(2, 3))
    
    ds.close()
