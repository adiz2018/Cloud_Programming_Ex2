from flask import Flask, request, jsonify, make_response
import datetime
import requests
import time
import queue
import sys
from apscheduler.schedulers.background import BackgroundScheduler

from worker import Worker
from tasks import Task
from aws_utils import AWSUtils

# app = Flask(__name__)
# display response as pretty json
# app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

MAX_TASK_TIME_SEC = 60

class Endpoint:

    def __init__(self, max_num_of_workers, my_ip, sibling_ip):
        self.my_ip = my_ip
        self.sibling_ip = sibling_ip
        # define workers properties
        self.max_num_of_workers = int(max_num_of_workers)
        self.current_num_of_workers = 0
        # define queues
        self.workQueue = queue.Queue()
        self.DoneQueue = queue.Queue()
        self.workers = {}
        self.task_id = 1
        self.worker_id = 0
        #define the flask app
        self.app = Flask(__name__)
        # display response as pretty json
        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
        # configure API
        self.add_all_functions()

        self.aws = AWSUtils()
        # define scheduled function that will check if worker should be started
        scheduler = BackgroundScheduler()
        scheduler.add_job(id='Scheduled task', func=self.timer_new_worker, trigger='interval', seconds=600)
        scheduler.start()

    def add_all_functions(self):
        # Add endpoint for the action function
        self.add_endpoint('/add_sibling', 'add_sibling', self.add_sibling, methods=['POST'])
        self.add_endpoint('/get_is_available_workers_num', 'get_workers_num', self.check_num_of_workers, methods=['GET'])
        self.add_endpoint('/killWorker', 'killWorker', self.kill_worker, methods=['POST'])
        self.add_endpoint('/enqueue', 'enqueue', self.enqueueWork, methods=['PUT'])
        self.add_endpoint('/get_work', 'get_work', self.give_work, methods=['GET'])
        self.add_endpoint('/done_work', 'done_work', self.done_work, methods=['POST'])
        self.add_endpoint('/pullCompleted', 'pullCompleted', self.pullComplete, methods=['POST'])
        self.add_endpoint('/pullCompletedSibling', 'pullCompletedSibling', self.pullComplete_sibling, methods=['POST'])
        self.add_endpoint('/', 'up', self.is_up, methods=['GET'])
        self.add_endpoint('/updateMaxWorkers', 'updateMaxWorkers', self.update_max_num_of_workers, methods=['POST'])


    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=None, *args, **kwargs):
        self.app.add_url_rule(endpoint, endpoint_name, handler, methods=methods, *args, **kwargs)

    def run(self, **kwargs):
        self.app.run(**kwargs)

    def get_attribute(self, request, name):
        attr = request.args.get(name)
        if attr is None:
            content = request.get_json()
            if content:
                attr = content.get(name)
        return attr

    def add_sibling(self):
        sibling_ip = self.get_attribute(request, 'sibling_ip')
        if sibling_ip:
            print(f'setting sibling to {sibling_ip}')
            self.sibling_ip = sibling_ip
            return make_response(jsonify(res='success', error=None), 200)
        return make_response(jsonify(res='fail', error='no IP was sent'), 400)

    def is_up(self):
        return jsonify(message='server is alive')

    def check_is_sibling_up(self):
        if self.sibling_ip:
            try:
                req = requests.get(f"http://{self.sibling_ip}:5000/", timeout=5)
                return True
            except requests.exceptions.ConnectTimeout:
                pass
        return False

    def check_sibling_workers(self):
        if self.check_is_sibling_up():
            try:
                req = requests.get(f"http://{self.sibling_ip}:5000/get_is_available_workers_num")
                j = req.json()
                return j['res'], j['num_of_workers']
            except requests.exceptions.ConnectTimeout:
                print(f"Error: can't connect to sibling ip: {self.sibling_ip}")
                return False, 0
        return False, 0

    def update_max_num_of_workers(self):
        self.max_num_of_workers -= 1
        return make_response(jsonify(success=True), 200)

    def create_worker(self):
        worker, worker_ip = self.aws.create_worker_instance(self.worker_id,
                                        self.my_ip,
                                        sibling_ip=self.sibling_ip)
        self.workers[self.worker_id] = worker
        self.worker_id += 1
        self.current_num_of_workers += 1
        return self.worker_id - 1

    def spawn_worker_inner(self, from_sibling=False):
        # check if we can create a worker for the current endpoint
        if self.current_num_of_workers < self.max_num_of_workers:
            new_worker_id = self.create_worker()
            print(f'new worker was created on endpoint with ID: {new_worker_id}')
            return new_worker_id, from_sibling

        # otherwise, check if we can take a worker from the sibling endpoint
        is_available, num_of_abailable_workers = self.check_sibling_workers()
        if is_available and num_of_abailable_workers > 0:
            # update num of workers on sibling
            req = requests.post(f"http://{self.sibling_ip}:5000/updateMaxWorkers", timeout=10)
            if req.status_code == 200:
                self.max_num_of_workers += 1
                new_worker_id = self.create_worker()
                print(f'new worker was created on (from sibling) endpoint with ID: {new_worker_id}')
                return new_worker_id, True

        return -1, from_sibling

    def check_num_of_workers(self):
        if self.current_num_of_workers < self.max_num_of_workers:
            available_workers = self.max_num_of_workers - self.current_num_of_workers
            return make_response(jsonify(res=True, num_of_workers=available_workers), 200)
        return make_response(jsonify(res=False, num_of_workers=0), 400)

    def check_if_worker_alive(self, id):
        return self.workers[id].is_alive()

    def kill_worker(self):
        worker_id = int(self.get_attribute(request, 'work_id'))
        if worker_id in list(self.workers.keys()):
            # update number of workers
            self.current_num_of_workers -= 1
            success = self.aws.terminate_instance(self.workers[worker_id].id)
            if success:
                print(f'worker {worker_id} was successfully terminated')
                return make_response(jsonify(killed=True), 200)
        return make_response(jsonify(killed=False), 400)

    def timer_new_worker(self):
        print('checking if new worker should be created')
        if self.workQueue.qsize() > 0:
            last_task_time = list(self.workQueue.queue)[0].receive_time
            if (datetime.datetime.now() - last_task_time).seconds > MAX_TASK_TIME_SEC:
                spawn_id, from_sibling = self.spawn_worker_inner()
                if spawn_id != -1:
                    if from_sibling:
                        print(f'new worker was created on sibling with ID: {spawn_id}')
                    else:
                        print(f'new worker was created on endpoint with ID: {spawn_id}')
                else:
                    print("worker couldn't be created")

    def enqueueWork(self):
        iterations = self.get_attribute(request, 'iterations')
        body = request.get_data().hex()
        # check if more workers are needed
        if self.current_num_of_workers == 0:
            spawn_id, from_sibling = self.spawn_worker_inner()

        task = Task(self.task_id, body, iterations)
        self.workQueue.put(task)
        self.task_id += 1
        return make_response(jsonify(work_id=self.task_id - 1), 200)

    def give_work(self):
        if not self.workQueue.empty():
            work = self.workQueue.get()
            return make_response(jsonify(work=work.__dict__), 200)
        return make_response(jsonify(work={}), 400)

    def done_work(self):
        work_id = self.get_attribute(request, 'work_id')
        result = self.get_attribute(request, 'result')
        if result:
            self.DoneQueue.put((work_id, result))
            return make_response(jsonify(success=True), 200)
        return make_response(jsonify(success=False), 400)

    def get_complete_from_sibling(self, top):
        try:
            req = requests.post(f"http://{self.sibling_ip}:5000/pullCompletedSibling?top={top}")
            j = req.json()
            return j['results']
        except requests.exceptions.ReadTimeout:
            return []

    def pull_results(self, top, from_sibling=False):
        results = []
        keys = ['work_id', 'result']

        if self.DoneQueue.qsize() > 0:
            while not self.DoneQueue.empty() and len(results) < top:
                result = self.DoneQueue.get()
                results.append(dict([x for x in zip(keys, result)]))

        if not from_sibling and len(results) < top and self.check_is_sibling_up():
            results.extend(self.get_complete_from_sibling(top - len(results)))

        return results

    def pullComplete_sibling(self):
        # only called when results are pulled by the sibling
        top = int(self.get_attribute(request, 'top'))
        results = self.pull_results(top, from_sibling=True)
        return make_response(jsonify(results=results), 200)

    def pullComplete(self):
        top = int(self.get_attribute(request, 'top'))
        results = self.pull_results(top)

        return make_response(jsonify(results=results), 200)

if __name__ == "__main__":
    args = sys.argv
    my_ip = requests.get('https://checkip.amazonaws.com').text.strip()
    num_workers = args[1]
    sibling_ip = None
    if len(args) > 2:
        sibling_ip = args[2]
    endpoint = Endpoint(num_workers, my_ip, sibling_ip)
    endpoint.run(host='0.0.0.0')
