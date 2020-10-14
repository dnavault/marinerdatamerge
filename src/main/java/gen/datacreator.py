import uuid
import random
import datetime
import time

services = []
clients = []
for i in range(0,20):
    services.append(uuid.uuid4())

for i in range(0,100):
    clients.append('.'.join('%s'%random.randint(0, 255) for i in range(4)) + "," + str(uuid.uuid4()))

print "client-address,client-guid,request-time,service-guid,retries-request,packets-requested,packets-serviced,max-hole-size"
for i in range(0,300):
    #print str(random.choice(clients)) + "," + (datetime.datetime.now() + datetime.timedelta(seconds=random.randint(0, 86400))).strftime("%Y-%m-%d %H:%M:%S ADT") + "," + str(random.choice(services)) \
    print str(random.choice(clients)) + "," + str(int(time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=random.randint(0, 86400))).timetuple()))*1000 + random.randint(0,999)) + "," + str(random.choice(services)) \
    + "," + str(int(random.triangular(0, 10, 7))) + "," + str(int(random.triangular(0, 20, 15))) + "," + str(int(random.triangular(0, 20, 15))) + "," + str(int(random.triangular(0, 20, 15)))
