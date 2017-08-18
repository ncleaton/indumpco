import sys, os, time, threading
from nose.tools import assert_equals, raises
from indumpco.pll_pipe import parallel_pipe

class FaultyWorkerError(StandardError):
    pass

def worker_function(q, job):
    if job == "die_worker_die":
        raise FaultyWorkerError("worker died")
    elif job == 0:
        raise ValueError("job 0 made it to a worker")
    time.sleep(job / 100)
    q.put(job)

def faulty_job_generator():
    yield 1
    yield 2
    yield 3
    i = 17
    i += "foo"
    yield 4

############################################################################

def test_job_order():
    jobs = [3,15,1,9,2,8,5,3,4,7]
    for thread_count in 1, 2, 4, 8:
        results = list(parallel_pipe(jobs, worker_function, thread_count))
        assert_equals(results, jobs)

@raises(FaultyWorkerError)
def test_exception_in_worker_1thread():
    return list(parallel_pipe([1,"die_worker_die",3], worker_function, 1))

@raises(FaultyWorkerError)
def test_exception_in_worker_2threads():
    return list(parallel_pipe([1,"die_worker_die",3], worker_function, 2))

@raises(FaultyWorkerError)
def test_exception_in_worker_4threads():
    return list(parallel_pipe([1,2,3,4,5,6,7,8,"die_worker_die",3,4,5,6,7,0,0,0,0,0,0,0,0,0,0,0,0,0,0], worker_function, 4))

@raises(TypeError)
def test_exception_in_source_iterator_1thread():
    return list(parallel_pipe(faulty_job_generator(), worker_function, 1))

@raises(TypeError)
def test_exception_in_source_iterator_2threads():
    return list(parallel_pipe(faulty_job_generator(), worker_function, 2))

def test_job_delegation():
    # Allow only one thread at a time to do actual work, other worker
    # threads delegate their tasks to this active thread.

    class DelegateState(object):
        def __init__(self):
            self.lock = threading.RLock()
            self.running = False
            self.jobs = []
    dg_state = DelegateState()

    def dg_worker(q, job):
        with dg_state.lock:
            dg_state.jobs.append((q, job))
            if dg_state.running:
                # another thread is active, let it do my work
                return
            else:
                # I am the active thread
                dg_state.running = True
        while True:
            with dg_state.lock:
                if len(dg_state.jobs) == 0:
                    dg_state.running = False
                    return
                batch = dg_state.jobs
                dg_state.jobs = []
            for q, job in batch:
                time.sleep(job / 100)
                q.put(job)

    jobs = [3,15,1,9,2,8,5,3,4,7]
    for thread_count in 1, 2, 4, 8:
        results = list(parallel_pipe(jobs, dg_worker, thread_count))
        assert_equals(results, jobs)
