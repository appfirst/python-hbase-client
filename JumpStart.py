#
# Initialize the Hbase jars for Apache 
# before it starts to handle requests
#
import sys
import hbase


client = hbase.Connect('zookeeper0:2181', 'hbmaster0:60000',
                       None, False, '512M')
hbase.Put(client, b'summary', b'test', b'summary', 
          b'\x00\x00A', b'Apache Test String')
