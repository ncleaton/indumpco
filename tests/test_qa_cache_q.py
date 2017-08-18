import sys, os, time, threading, Queue
from nose.tools import assert_equals
from functools import wraps
from indumpco.qa_caching_q import QACacheQueue, NOT_IN_CACHE
from indumpco.pll_pipe import parallel_pipe

class TestHarness(object):
    def __init__(self):
        self.bg_threads = []

    def setup(self, testcase):
        self.q_list = []
        for t in testcase:
            if not isinstance(t, list):
                t = [t]
            main_q = t[0]
            byproduct_q = t[1:]
            self.q_list.append(main_q)
        self.have_computed = set()
        self.qacq = QACacheQueue(iter(self.q_list))

    def teardown(self):
        self.qacq.finished()

        for t in self.bg_threads:
            t.join()
        self.bg_threads = []

    def pretend_to_compute(self, q):
        if q in self.have_computed:
            raise RuntimeError('cache failure: answer recompued', q)
        self.have_computed.add(q)
        return 2*q

    def spawnthread(self, target):
        t = threading.Thread(target=target)
        t.deamon = True
        self.bg_threads.append(t)
        t.start()

    def wrap_iter_in_queue(self, it, maxsize):
        END_OF_QUEUE = object()
        queue = Queue.Queue(maxsize=maxsize)

        def push_to_queue():
            for x in it:
                queue.put(x)
            queue.put(END_OF_QUEUE)
        self.spawnthread(push_to_queue)

        def pull_from_queue():
            while True:
                x = queue.get()
                if x is END_OF_QUEUE:
                    return
                else:
                    yield x
        return pull_from_queue()

th = TestHarness()

############################################################################

def trun(testcase, byproduct_map, qlen=0):
    th.setup(testcase)

    my_iter = th.qacq
    if qlen > 0:
        # Put a Queue.Queue on the output of the QACacheQueue, delaying
        # the processing of the elements coming off it.
        my_iter = th.wrap_iter_in_queue(my_iter, qlen)

    answers = []
    for q in my_iter:
        a = th.qacq.consume_cached_answer(q)
        if a is NOT_IN_CACHE:
            byproduct_q = byproduct_map.get(q, [])
            if th.qacq.i_should_compute(q, byproduct_q):
                a = th.pretend_to_compute(q)
                th.qacq.i_have_computed(q, a, [(x,2*x) for x in byproduct_q])
            else:
                a = th.qacq.wait_for_answer(q)
        answers.append(a)

    assert_equals(answers, [2*x for x in testcase])
    th.teardown()

def trun_pll_pipe(testcase, byproduct_map, popsleep):
    th.setup(testcase)

    def _pll_pipe_worker(outq, q):
        a = th.qacq.consume_cached_answer(q)
        time.sleep(popsleep)
        if a is NOT_IN_CACHE:
            byproduct_q = byproduct_map.get(q, [])
            if th.qacq.i_should_compute(q, byproduct_q):
                time.sleep(q / 100)
                a = th.pretend_to_compute(q)
                th.qacq.i_have_computed(q, a, [(x,2*x) for x in byproduct_q])
                outq.put(a)
            else:
                th.qacq.put_answer_when_ready(q, outq)
        else:
            outq.put(a)

    answers = list(parallel_pipe(th.qacq, _pll_pipe_worker, 10, 20))
    assert_equals(answers, [2*x for x in testcase])
    th.teardown()

def test_all():
    for testcase in ([], [1], [1,1], [10,1], [1,3,4,99,100,3,4,5,5], [1,2,7,14,2,4,3,6,2,4]):
        for byproduct_map in ({}, {1:[10]}, {10:[1]}, {4:[3,5]}):
            for qlen in (0, 100):
                trun(testcase, byproduct_map, qlen)
            for popsleep in 0, 0.1:
                trun_pll_pipe(testcase, byproduct_map, popsleep)
