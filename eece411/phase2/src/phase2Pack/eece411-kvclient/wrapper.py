from kvclient import KeyValueClient, InvalidKeyError

SERVER = 'planetlab1.cs.unc.edu:5000'
KEY = '11'
VALUE = '1'

client = KeyValueClient(SERVER)
client.put(KEY, VALUE)
print(client.get(KEY))
client.delete(KEY)
try:
    print(client.get(KEY))
except InvalidKeyError, error:
    print(str(error))
