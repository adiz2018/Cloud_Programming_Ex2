from flask import Flask, request, jsonify, make_response
from multiprocessing import Process
import datetime
import requests
import time
import queue
import sys

from worker import Worker
from tasks import Task

app = Flask(__name__)
# display response as pretty json
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

MAX_TASK_TIME_SEC = 15

class Endpoint:

    def __init__(self, max_num_of_workers, my_ip, sibling_ip):
        self.my_ip = my_ip
        self.sibling_ip = sibling_ip
        # define workers properties
        self.max_num_of_workers = max_num_of_workers
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


    def add_all_functions(self):
        # Add endpoint for the action function
        self.add_endpoint('/add_sibling', 'add_sibling', self.add_sibling, methods=['POST'])
        self.add_endpoint('/spawn_worker', 'spawn_worker', self.spawn_worker, methods=['POST'])
        self.add_endpoint('/get_is_available_workers_num', 'get_workers_num', self.check_num_of_workers, methods=['GET'])
        self.add_endpoint('/killWorker', 'killWorker', self.kill_worker, methods=['POST'])
        self.add_endpoint('/enqueue', 'enqueue', self.enqueueWork, methods=['PUT'])
        self.add_endpoint('/get_work', 'get_work', self.give_work, methods=['GET'])
        self.add_endpoint('/done_work', 'done_work', self.done_work, methods=['POST'])
        self.add_endpoint('/pullCompleted', 'pullCompleted', self.pullComplete, methods=['POST'])
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
            attr = content.get(name)
        return attr

    # @app.route('/add_sibling', methods=['POST'])
    def add_sibling(self):
        sibling_ip = self.get_attribute(request, 'sibling_ip')
        # sibling_ip = request.args.get('sibling_ip')
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

    # @app.route('/spawn_worker', methods=['POST'])
    def spawn_worker(self):
        val = self.get_attribute(request, 'from_sibling')
        # val = request.get_json().get('from_sibling')
        from_sibling = True if val == 'true' else False
        spawn_id, from_sibling = self.spawn_worker_inner(from_sibling=from_sibling)
        if spawn_id != -1:
            return make_response(jsonify(spawn_id=spawn_id, from_sibling=from_sibling, error=None), 200)
        return make_response(jsonify(spawn_id=spawn_id, from_sibling=from_sibling, error="couldn't spawn process"), 400)

    def create_worker(self):
        # create worker process
        worker = Process(target=Worker, args=((self.worker_id,
                                               self.my_ip,
                                               self.sibling_ip)))
        worker.daemon = True
        # start worker process
        worker.start()
        # wait for worker to start
        time.sleep(1)
        self.workers[self.worker_id] = worker
        # check worker is alive
        if self.check_if_worker_alive(self.worker_id):
            # increase number of workers
            self.worker_id += 1
            self.current_num_of_workers += 1
            return self.worker_id - 1
        return -1


    def spawn_worker_inner(self, from_sibling=False):
        # TODO: Should I check if worker creation was successful? Multiprocessing?
        # check if we can create a worker for the current endpoint
        if self.current_num_of_workers < self.max_num_of_workers:
            new_worker_id = self.create_worker()
            return new_worker_id, from_sibling
            # # create worker process
            # worker = Process(target=Worker, args=((self.worker_id,
            #                                        self.my_ip,
            #                                        self.sibling_ip)))
            # worker.daemon = True
            # # start worker process
            # worker.start()
            # # wait for worker to start
            # time.sleep(1)
            # self.workers[self.worker_id] = worker
            # # check worker is alive
            # if self.check_if_worker_alive(self.worker_id):
            #     # increase number of workers
            #     self.worker_id += 1
            #     self.current_num_of_workers += 1
            #     return self.worker_id-1, from_sibling
        # otherwise, check if we can take a worker from the sibling endpoint
        is_available, num_of_abailable_workers = self.check_sibling_workers()
        if is_available and num_of_abailable_workers > 0:
            # update num of workers on sibling
            req = requests.post(f"http://{self.sibling_ip}:5000/updateMaxWorkers", timeout=10)
            if req.status_code == 200:
                self.max_num_of_workers += 1
                new_worker_id = self.create_worker()
                return new_worker_id, True

        # elif not from_sibling and self.check_sibling_workers():
        #     # try to spawn worker on sibling endpoint
        #     try:
        #         req = requests.post(f"http://{self.sibling_ip}:5000/spawn_worker", json={'from_sibling': True}, timeout=10)
        #         j = req.json()
        #         return j.spawn_id, j.from_sibling
        #     except requests.exceptions.ConnectTimeout:
        #         print(f"Error: can't connect to sibling ip: {self.sibling_ip}")
        #         return -1, from_sibling
        # can't spawn worker
        return -1, from_sibling

    def check_num_of_workers(self):
        if self.current_num_of_workers < self.max_num_of_workers:
            available_workers = self.max_num_of_workers - self.current_num_of_workers
            return make_response(jsonify(res=True, num_of_workers=available_workers), 200)
        return make_response(jsonify(res=False, num_of_workers=0), 400)

    def check_if_worker_alive(self, id):
        return self.workers[id].is_alive()

    # @app.route('/killWorker', methods=['POST'])
    def kill_worker(self):
        worker_id = int(self.get_attribute(request, 'work_id'))
        # kill process
        if worker_id in list(self.workers.keys()) and self.check_if_worker_alive(worker_id):
            # update number of workers
            self.current_num_of_workers -= 1
            self.workers[worker_id].terminate()
            # give it time to die
            time.sleep(0.5)
            if self.check_if_worker_alive(worker_id):
                print(f"couldn't terminate process {worker_id}")
                return make_response(jsonify(killed=False), 400)
            self.workers[worker_id].join(timeout=1.0)
            return make_response(jsonify(killed=True), 200)
        return make_response(jsonify(killed=False), 400)

    def timer_new_worker(self):
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

    # @app.route('/enqueue', methods=['PUT'])
    def enqueueWork(self):
        iterations = self.get_attribute(request, 'iterations')
        # print(request.args)
        # iterations = request.args.get('iterations')
        body = request.get_data().hex()
        # check if more workers are needed
        if self.current_num_of_workers == 0:
            spawn_id, from_sibling = self.spawn_worker_inner()
        else:
            self.timer_new_worker()
        # inset new work
        task = Task(self.task_id, body, iterations)
        self.workQueue.put(task)
        self.task_id += 1
        return make_response(jsonify(work_id=self.task_id - 1), 200)

    # @app.route('/get_work', methods=['GET'])
    def give_work(self):
        if not self.workQueue.empty():
            work = self.workQueue.get()
            return make_response(jsonify(work=work.__dict__), 200)
        return make_response(jsonify(work={}), 400)

    # @app.route('/done_work', methods=['POST'])
    def done_work(self):
        work_id = self.get_attribute(request, 'work_id')
        result = self.get_attribute(request, 'result')
        # content = request.get_json()
        # work_id = content.get('work_id')
        # result = content.get('result')
        if result:
            self.DoneQueue.put((work_id, result))
            return make_response(jsonify(success=True), 200)
        return make_response(jsonify(success=False), 400)

    def get_complete_from_sibling(self, top):
        req = requests.post(f"http://{self.sibling_ip}:5000/pullCompleted?top={top}")
        j = req.json()
        return j.results

    # @app.route('/pullCompleted', methods=['POST'])
    def pullComplete(self):
        top = int(self.get_attribute(request, 'top'))
        # content = request.get_json()
        # top = int(content.get('top'))
        # top = request.args.get('top')
        results = []
        keys = ['work_id', 'result']

        if self.DoneQueue.qsize() > 0:
            while not self.DoneQueue.empty() and len(results) < top:
                result = self.DoneQueue.get()
                results.append(dict([x for x in zip(keys, result)]))

        if len(results) < top and self.check_is_sibling_up():
            results.extend(self.get_complete_from_sibling(top-len(results)))

        return make_response(jsonify(results=results), 200)

if __name__ == "__main__":
    args = sys.argv
    my_ip = args[1]
    num_workers = args[2]
    sibling_ip = None
    if len(args) > 3:
        sibling_ip = args[3]
    endpoint = Endpoint(num_workers, my_ip, sibling_ip)
    endpoint.run()
    # connect sibling
    if sibling_ip:
        req = requests.post(f"http://{sibling_ip}:5000/add_sibling?sibling_ip={my_ip}")
        j = req.json()
