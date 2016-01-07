import os
import time
import unittest

from datastore import DataStore

class TestDataStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ds = DataStore(os.path.join('tmp', 'store0'), ['id0', 'id1'], ['first_name', 'last_name'])

        # prepare docs
        cls.docs = []
        first_names = ['Marko', 'Milica', 'Dunja', 'Marta']
        last_names = ['Tasic', 'Milosevic', 'Colic']

        for i in range(1000000):
            doc = {
                'id0': i,
                'id1': i + 1,
                'first_name': first_names[i % len(first_names)],
                'last_name': last_names[i % len(last_names)],
            }

            cls.docs.append(doc)

    @classmethod
    def tearDownClass(cls):
        cls.ds.close()
        cls.ds = None

    def test_one_million_writes(self):
        ds = self.ds
        t = time.time()

        for doc in self.docs:
            ds.add(doc)

        print('took {} seconds'.format(time.time() - t))

    def test_one_million_writes_and_reads(self):
        ds = self.ds
        t = time.time()

        for i in range(1000000):
            doc = ds.get(i, i + 1)

        print('took {} seconds'.format(time.time() - t))

if __name__ == '__main__':
    unittest.main()
