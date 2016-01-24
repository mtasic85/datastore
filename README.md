# datastore
Python implementation of data-store based on SStables

# MemTable

Python dict as MemTable. Key in MemTable is tuple of primary keys for a given document. All primary keys are unique. MemTable is unordered.

'''py
{
    (): {},
}
'''

# PrimaryKey

Small JSON file containing primary key definitions.

# SSTable

JSON file containing array which items are dicts which represent documents. This array is sorted.

```js
[{}]
```

# Index

JSON file containing array of dicts which represent documents locations in sstable.