#!/usr/bin/env python

import logging
import os
import sys
import threading
import time

from grpc.beta import implementations
import typhoon_pb2


class Task(object):
    def __init__(self, description):
        self._description = description
        self._worker = None

        logging.info('Created task: %s', self._description.id)

    @property
    def name(self):
        return self._description.id

    def status(self):
        if self._worker:
            return typhoon_pb2.ACTIVE

        return typhoon_pb2.PENDING

    def can_schedule(self, stores):
        logging.info('CanSchedule(%s, %s)', self.name, self._worker)
        if self._worker:
            return False

        for source in self._description.source:
            if not stores[source.name].is_scheduled():
                return False

        for sink in self._description.sink:
            if not stores[sink.name].is_scheduled():
                return False

        return True

    def __repr__(self, *args, **kwargs):
        return 'Task(%s, %s)' % (self.name, id(self))

class Store(object):
    def __init__(self, desc):
        self._description = desc
        self._worker = None

    def worker(self):
        return self._worker

    def can_schedule(self):
        return self._worker is None

    def is_scheduled(self):
        return self._worker is not None

    @property
    def name(self):
        return self._description.name

    def __repr__(self, *args, **kwargs):
        return 'Store(%s)' % self.name

class Worker(object):
    def __init__(self, host, port, slots):
        logging.info('Registering worker: %s:%s', host, port)
        self.host = host
        self.port = port
        self.slots = slots

        channel = implementations.insecure_channel(host, port)
        self._stub = typhoon_pb2.beta_create_TyphoonWorker_stub(channel)
        self._ping_resp = None

        self._tasks = set()
        self._stores = set()

    def assign_task(self, task):
        logging.info('Assigning task %s to %s:%d', task, self.host, self.port)
        self._tasks.add(task)
        task._worker = task
        assert not task.can_schedule({})

        self._stub.start(task._description, 10)
        logging.info('Done.')

    def assign_store(self, store):
        logging.info('Assigning store %s to %s:%d', store, self.host, self.port)
        self._stores.add(store)
        store._worker = self
        assert not store.can_schedule()
        self._stub.assign(store._description, 10)
        logging.info('Done.')

    def ping(self, req):
        try:
            self._ping_resp = self._stub.ping(req, 10)
        except:
            logging.warn('Worker ping failed.', exc_info=1)

    def shutdown(self):
        empty = typhoon_pb2.EmptyMessage()
        self._stub.shutdown(empty, 10)

    def slots_available(self):
        return self.slots - len(self._tasks)

    def __repr__(self):
        return 'Worker(slots=%d, used=%d)' % (self.slots, len(self._tasks))


class TestWorker(Worker):
    def ping(self, req):
        return

class MasterService(typhoon_pb2.BetaTyphoonMasterServicer):
    def __init__(self):
        self._workers = {}
        self._stores = {}
        self._tasks = {}

#         self._workers['testworker'] = TestWorker('localhost', 19999, 10)
        # self._workers['testworker'] = Worker('[::]', 19999, 10)

        self._scheduler = threading.Thread(target=self._schedule)
        self._scheduler.daemon = True
        self._scheduler.start()

    def _schedule(self):
        while True:
            logging.info('Updating workers...')
            self.update_workers()
            logging.info('Scheduling tasks...')
            self.schedule()
            time.sleep(1)

    def hello(self, request, context):
        logging.info('Register request: %s', request)
        host = request.host
        port = request.port
        slots = request.slots

        self._workers['%s:%d'] = Worker(host, port, slots)
        return typhoon_pb2.RegisterResponse(id=2)

    def execute(self, request, context):
        logging.info('Received run request: %s', request)
        for task in request.task:
            for source in task.source:
                if not source.name in self._stores:
                    self._stores[source.name] = Store(source)

            for sink in task.sink:
                if not sink.name in self._stores:
                    self._stores[sink.name] = Store(sink)

            logging.info('Stores: %s', self._stores.keys())
            self._tasks[task.id] = Task(task)

        return typhoon_pb2.RunGraphResponse()

    def status(self, request, context):
        pass

    def available_tasks(self):
        for t in self._tasks.values():
            if t.can_schedule(self._stores):
                logging.info('Task %s can be scheduled.', t.name)
                yield t

    def available_stores(self):
        for s in self._stores.values():
            if s.can_schedule():
                yield s

    def schedule(self):
        logging.info('Scheduling...')
        stores = self.available_stores()
        workers = self._workers.itervalues()
        for worker, store in zip(workers, stores):
            worker.assign_store(store)

        tasks = self.available_tasks()
        workers = [w for w in self._workers.itervalues() if w.slots_available()]
        for worker, task in zip(workers, tasks):
            worker.assign_task(task)

    def update_workers(self):
        ping_req = typhoon_pb2.PingRequest()
        for hostport, worker in self._workers.items():
            worker.ping(ping_req)

    def shutdown(self):
        for hostport, worker in self._workers.items():
            worker.shutdown()


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('Starting master.')
    service = MasterService()
    server = typhoon_pb2.beta_create_TyphoonMaster_server(service)
    server.add_insecure_port('0.0.0.0:29999')
    server.start()
    logging.info('Master listening on 29999')
    while True:
        try:
            time.sleep(1)
        except:
            logging.info('Shutting down.')
            os._exit(0)

if __name__ == '__main__':
    main()
