import threading
import time
import util
import cv2
from Queue import Queue, PriorityQueue

class Thread(threading.Thread):
    output = None
    newborn = True

    def __init__(self, args):
        super(Thread, self).__init__()
        self.args = args

    def run(self):
        self.newborn = False
        idx, data = self.args
        cv2.imread(data)
        opt = 1
        self.output = (idx, opt)

class ThreadRecycler:
    args = None
    thread = None
    state = 'stop'  # busy/stop
    name = ''

    def __init__(self, name=''):
        self.name = name

    def feed(self, args):
        self.args = args

    def run(self):
        self.thread = Thread(self.args)
        self.thread.start()
        self.state = 'busy'

    def join(self):
        if self.thread.isAlive():
            self.thread.join()
        self.state = 'stop'

    def isAvailable(self):
        return not (self.thread.newborn or self.thread.isAlive())

    def get(self, block=True, timeout=None):
        if block:
            self.thread.join(timeout)
            opt = self.thread.output
        else:
            opt = self.thread.output
        self.state = 'stop'
        return opt

class Pool:
    input_queue = Queue()
    output_queue = PriorityQueue()
    thread_pool = list()

    def __init__(self, num_worker):
        for i in range(num_worker):
            self.thread_pool.append(ThreadRecycler(name=str(i)))

    def run(self, interval = 1):
        tsk = self.input_queue.qsize()
        hash_table = set()
        while len(hash_table) != tsk:
            for t in self.thread_pool:
                if self.input_queue.empty():
                    if t.state == 'stop':
                        t.state = 'busy'
                if t.state == 'stop':
                    data = self.input_queue.get()
                    #print '[feed] %2d -> %2s'%(data[1], t.name)
                    t.feed(data)
                    t.run()
                if t.isAvailable():
                    idx, data = t.get()
                    if idx not in hash_table:
                        hash_table.add(idx)
                        #print '[get ]       %2s -> %2d'%(t.name, data)
                        self.output_queue.put(data)

    def feed(self, data):
        for n, elem in enumerate(data):
            self.input_queue.put((n, elem))

    def get(self):
        return self.output_queue.get()

lst = util.search()[:1000]

pool = Pool(5)
pool.feed(lst)
#t = threading.Thread(target=pool.run)
#t.start()
ts = time.time()
pool.run()
tf = time.time()
print tf-ts
ts = time.time()
for i in lst:
    cv2.imread(i)
tf = time.time()
print tf-ts
