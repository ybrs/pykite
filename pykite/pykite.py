import os
import time
import json
import pika
import redis
import signal
import urllib
import socket
import requests
import threading

def get_publickey(home="/root"):
    with open(os.path.join(home, ".kd", "koding.key.pub"), "r") as f:
        return f.read()

def get_manifest(home="/root"):
    with open(os.path.join(home, ".kd", "kd-manifest.json"), "r") as f:
        return json.loads(f.read())

def get_kdconfig(home="/root"):
    with open(os.path.join(home, ".kd", "kdconfig"), "r") as f:
        return json.loads(f.read())

def get_config(manifest, kdconfig, publickey):
    print manifest
    url = "%s/-/kite/login?" % manifest['apiAddress']
    params = {
        'key': publickey,
        'name': manifest['name'],
        'username': kdconfig['username'],
        'type': 'openservice'
    }
    r = requests.get(url, params=params)
    return json.loads(r.content)

class Kite(threading.Thread):

    def channel_name_for_all(self):
        return "%s-kite-all" % self.publickey

    def channel_name_for_group(self, groupName=None):
        if not groupName:
            groupName = self.groupName
        return "%s-kite-kite-%s" % (self.publickey, groupName)

    def channel_name_for_self(self, groupName=None, kiteName=None):
        if not groupName:
            groupName = self.groupName
        if not kiteName:
            kiteName = "%s-%s-%s|%s" % (groupName, self.publickey, os.getpid(), socket.gethostname())
        "%s-kite-kite-%s-kite-%s" % (
            self.publickey,
            groupName,
            kiteName
        )

    def __init__(self, publickey, manifest):
        threading.Thread.__init__(self)

        self.publickey = publickey
        self.groupName = manifest['name']
        self.redisconn = redis.StrictRedis(host='local.koding.com', port=6380, db=0)
        self.pubsub = self.redisconn.pubsub()
        self.pubsub.subscribe([self.channel_name_for_all(), self.channel_name_for_group(), self.channel_name_for_self()])

    def exit(self):
        self.pubsub.unsubscribe()

    def run(self):
        for item in self.pubsub.listen():
            print ">>>", item


def signal_int_handler(c, k):
    import sys
    print "hallo"
    kite.exit()
    sys.exit(0)

if __name__=="__main__":
    publickey = get_publickey(os.environ['HOME'])
    manifest = get_manifest(os.environ['HOME'])
    kdconfig = get_kdconfig(os.environ['HOME'])
    config = get_config(manifest, kdconfig, publickey)
    signal.signal( signal.SIGINT, signal_int_handler )

    # {"protocol":"amqp","host":"local.koding.com",
    # "username":"kite-api-kd-gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv",
    #"password":"96fc11888509bc110c44e471","vhost":"/"}
    print ">>", config
    vhost = urllib.quote_plus(config['vhost'])
    amqpurl = '%s://%s:%s@%s:%s/%s' % (config['protocol'], 
                        config['username'], 
                        config['password'], 
                        config['host'],
                        5672,
                        vhost    
                        )
    parameters = pika.URLParameters(str(amqpurl))
    kite = Kite(publickey, manifest)
    kite.start()
    while True:
        time.sleep(0.100)
