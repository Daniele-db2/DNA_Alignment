def display_hash(hashTable):
    for key, value in hashTable.items():
        print("{}: {}".format(key, value))

        print()

def Hashing(keyvalue):
    return keyvalue

def insert(hashTable, keyvalue, value):
    hash_key = Hashing(keyvalue)
    if hash_key in hashTable:
        hashTable[hash_key].append(value)
    else:
        hashTable[hash_key] = list()
        hashTable[hash_key].append(value)

def hash_djb2(s):
    hash = 5381
    for x in s:
        hash = (( hash << 5) + hash) + ord(x)
    return hash & 0xFFFFFFFF
