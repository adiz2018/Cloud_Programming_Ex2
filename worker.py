import queue
import time
import datetime
import hashlib
import requests
import json

from tasks import Task

MAX_IDLE_TIME = 600

class Worker:

    def __init__(self, worker_id, my_addr, sibling_addr):
        self.worker_id = worker_id
        self.my_addr = my_addr
        self.sibling_addr = sibling_addr

        # start the worker
        self.loop()

    # ec2 - terminate on shutdown
    def do_work(self, iterations, buffer):
        output = hashlib.sha512(buffer).digest()
        for i in range(iterations - 1):
            output = hashlib.sha512(output).digest()
        return output

    def get_nodes(self):
        if self.sibling_addr:
            return [self.my_addr, self.sibling_addr]
        return [self.my_addr]

    def loop(self):
        nodes = self.get_nodes()
        last_time = datetime.datetime.now()
        while (datetime.datetime.now() - last_time).seconds <= MAX_IDLE_TIME:
            for node in nodes:
                # try to get the next work
                req = requests.get(f'http://{node}:5000/get_work', timeout=10)
                if req.status_code == 200:
                    # perform the work
                    if req.json()['work']:
                        work = req.json()['work']
                        result = self.do_work(int(work['iterations']), bytes.fromhex(work['buffer']))
                        # return the result to the endpoint
                        j = {'work_id': work['task_id'], 'result': result.hex()}
                        print(result.hex())
                        req = requests.post(f'http://{node}:5000/done_work', json=j, timeout=10)
                        if req.status_code != 200:
                            print(f"An error occurred while trying to save the result for {work['task_id']}")
                        # update the last time a work was completed
                        last_time = datetime.datetime.now()
                        # work was done so we continue to the sleep
                        continue
            # sleep between work batches
            time.sleep(10)

        # we can terminate because no work is needed
        self.terminate()

    # def loop(self):
    #     last_time = datetime.datetime.now()
    #     while (datetime.datetime.now() - last_time).seconds <= MAX_IDLE_TIME:
    #         try:
    #             work = self.work_queue.get(block=False)
    #             if work:
    #                 result = self.do_work(work)
    #                 # push to the results queue
    #                 self.done_queue.put((work.task_id, result))
    #         except queue.Empty:
    #             time.sleep(2)
    #     self.terminate()

    def terminate(self):
        requests.post(f"http://{self.my_addr}:5000/killWorker?work_id={self.worker_id}")



