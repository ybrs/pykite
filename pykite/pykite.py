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

class Kite(threading.Thread):

    def get_publickey(self):
        with open(os.path.join(self.kdroot, "koding.key.pub"), "r") as f:
            return f.read()

    def get_manifest(self):
        with open(os.path.join(self.kitepath, self.filename), "r") as f:
            return json.loads(f.read())

    def get_kdconfig(self):
        with open(os.path.join(self.kdroot, "kdconfig"), "r") as f:
            return json.loads(f.read())

    def get_config(self):
        url = "%s/-/kite/login?" % self.manifest['apiAddress']
        params = {
            'key': self.publickey,
            'name': self.manifest['name'],
            'username': self.kdconfig['username'],
            'type': 'openservice'
        }
        r = requests.get(url, params=params)
        return json.loads(r.content)

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

    def __init__(self, home="/root", filename="kd-manifest.json", kitepath=None):
        threading.Thread.__init__(self)
        self.home = home
        self.filename = filename

        if not kitepath:
            kitepath = home
        self.kitepath = kitepath

        self.kdroot = os.path.join(self.home, ".kd")

        self.manifest  = self.get_manifest()
        self.publickey = self.get_publickey()
        self.kdconfig  = self.get_kdconfig()

        self.groupName = self.manifest['name']
        self.redisconn = redis.StrictRedis(host='local.koding.com', port=6380, db=0)
        self.pubsub = self.redisconn.pubsub()

        self.pubsub.subscribe([self.channel_name_for_all(), 
                                self.channel_name_for_group(), 
                                self.channel_name_for_self()])

        self.internalfnmap = {
            'kite.consume': self.kite_consume
        }

    def exit(self):
        self.pubsub.unsubscribe()

    def execfn(self, args):
        fnname = args['cmd']
        fn = getattr(self, fnname, None)
        if fn:
            print ">>> running fn", fn 
            return fn(args, None)
        else:
            raise Exception("%s - not found" % fnname)        

    def send_command(self, channame, cmd, args):
        data = {
            'from': None,
            'cmd': cmd,
            'args': args
        }
        data = json.dumps(data)
        self.redisconn.publish(channame, data)

    def kite_consume(self):
        popped = self.redisconn.lpop(self.channel_name_for_group())
        if not popped:
            return
        print ">>>>", popped
        data = json.loads(popped)
        err, res = self.execfn(data)
        print ">>>> pushing to >>>>", data['from']
        self.send_command(data['from'], "reply-%s" % data['id'], [err, res])

    def run(self):
        # {'pattern': None, 'type': 'message', 
        #  'channel': 'gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv-kite-kite-consumer', 
        # 'data': 
        #     '{"cmd":"kite.consume","id":24,"args":"{\\"cmd\\":\\"consumeque\\",\\"id\\":14,\\"args\\":[\\"19951009\\\\t19951009\\\\t Iowa City, IA\\\\t\\\\t\\\\tMan repts. witnessing &quot;flash, followed by a classic UFO, w/ a tailfin at back.&quot; Red color on top half of tailfin. Became triangular.\\"],\\"from\\":\\"gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv-kite-kite-producer-kite-producer-gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv-39951|aybarss-MacBook-Air_local\\"}","from":"gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv-kite-kite-producer-kite-producer-gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv-39951|aybarss-MacBook-Air_local"}'}
        for item in self.pubsub.listen():
            try:
                print ">>>", item
                data = json.loads(item['data'])
            except Exception as e:
                print ">>> Error", e
            else:
                fn = self.internalfnmap[data['cmd']]
                if fn:
                    fn(data['args'])


# def signal_int_handler(c, k):
#     import sys
#     print "hallo"
#     kite.exit()
#     sys.exit(0)

# signal.signal( signal.SIGINT, signal_int_handler )


if __name__=="__main__":

    # # {"protocol":"amqp","host":"local.koding.com",
    # # "username":"kite-api-kd-gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv",
    # #"password":"96fc11888509bc110c44e471","vhost":"/"}
    # print ">>", config
    # vhost = urllib.quote_plus(config['vhost'])
    # amqpurl = '%s://%s:%s@%s:%s/%s' % (config['protocol'], 
    #                     config['username'], 
    #                     config['password'], 
    #                     config['host'],
    #                     5672,
    #                     vhost    
    #                     )
    # parameters = pika.URLParameters(str(amqpurl))
    kite = Kite(home=os.environ['HOME'], filename="kd-manifest.json")
    kite.start()
    while True:
        try:
            print "calling "
            kite.kite_consume()
        except Exception as e:
            print "ERror", e
        time.sleep(0.10)
