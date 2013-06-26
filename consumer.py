import os
import time
import signal
import traceback
from pykite.pykite import Kite


class Consumer(Kite):

    def consumeque(self, data, callback):
        # 
        #callback(False, True)
        print data
        return (False, True)

kite = Consumer(home=os.environ['HOME'], kitepath=".", filename="manifest.json")
kite.start()

def signal_int_handler(c, k):
    import sys
    print "hallo"
    kite.exit()
    sys.exit(0)

signal.signal( signal.SIGINT, signal_int_handler )

while True:
    try:
        #print "calling "
        kite.kite_consume()
    except Exception as e:
        print "Error", e
        traceback.print_exc()
    #time.sleep(0.1)

