import datetime

class Task:
    def __init__(self, task_id, buffer, iterations):
        self.task_id = task_id
        self.buffer = buffer
        self.iterations = iterations
        self.receive_time = datetime.datetime.now()
