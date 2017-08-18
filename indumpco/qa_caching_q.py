# -*- coding: utf-8 -*-

import Queue
import threading
from collections import defaultdict, deque
from itertools import islice
import sys

class WorkflowError(Exception):
    pass

NOT_IN_CACHE = object()

class QACacheQueue(object):
    """
    Designed for a situation in which you have an iterator of questions to work through, and questions may be repeated and you want to cache the answers to questions that are coming up again soon. The answers may be large, so you can't just blindly cache everything.

    Also, maybe computing the answer to a question can give rise to other question and answer pairs as a byproduct, and those other questions might also be coming up soon. There may be several threads consuming questions in parallel, and you want to avoid any thread working on a question the answer to which will be produced by another thread that's already started work.

    Wraps the iterator of questions, and provides some extra methods. The workflow should look like this:

        qacq = QACachingQueue(original_questions_iterable, lookahead=1000)
        for question in qacq:
            answer = qacq.consume_cached_answer(question)
            if answer is NOT_IN_CACHE:
                byproduct_questions = determine_byproducts(question)
                if qacq.i_should_compute(question, byproduct_questions)
                    # No other thread is in the process of answering my
                    # main question, and other threads are now blocked from
                    # working on any of the questions that I'm about answer.
                    answer, byproduct_qa = do_compute_answer(question)
                    qacq.i_have_computed(question, answer, byproduct_qa)
                else:
                    # Another thread will generate my answer
                    answer = qacq.wait_for_answer(question)

            do_something_with(answer)

    As an alternaive to calling the blocking wait_for_answer() method, you can arrange for a callback when the answer is ready via the put_answer_when_ready() method and move on the next question.

    Note that QACachingQueue's internal reference counting for cached questions and answers depends on you sticking to this workflow with respect to the QACachingQueue methods calls. The methods have side effects, don't call them more than once per question or miss any out.

    for q in qacq:
        a = qacq.consume_cached_answer(q)
        if a is NOT_IN_CACHE:
            if qacq.i_should_compute(...):
                ...
                qacq.i_have_computed(...)
            else:
                qacq.wait_for_answer(q) # or call put_answer_when_ready()
    """
    def __init__(self, src_iterable, lookahead=1000):
        self.lookahead = lookahead
        self.laq = deque()
        self.refcnt = defaultdict(int)
        self.answers = {}
        self.src_iterable = src_iterable
        self.lock = threading.RLock()
        self.in_progress_callback_queues = {}

    def _dec_refcnt(self, item):
        self.refcnt[item] -= 1
        if self.refcnt[item] < 1:
            if self.refcnt[item] < 0:
                raise WorkflowError("reference count went negative", item)
            self.refcnt.pop(item, None)
            self.answers.pop(item, None)

    def __iter__(self):
        if not hasattr(self, "src_iterable"):
            raise WorkflowError("attempt to iterate a QACachingQueue twice")
        it = iter(self.src_iterable)
        del self.src_iterable

        if self.lookahead > 0:
            for item in islice(it, self.lookahead):
                self.refcnt[item] += 1
                self.laq.append(item)

        for item in it:
            self.refcnt[item] += 1
            self.laq.append(item)
            yield self.laq.popleft()

        while len(self.laq):
            yield self.laq.popleft()

    def consume_cached_answer(self, q):
        with self.lock:
            a = self.answers.get(q, NOT_IN_CACHE)
            if a is not NOT_IN_CACHE:
                self._dec_refcnt(q)
        return a

    def i_should_compute(self, main_question, byproduct_questions=[]):
        with self.lock:
            if main_question in self.answers or main_question in self.in_progress_callback_queues:
                return False
            else:
                for q in [main_question] + byproduct_questions:
                    if q in self.refcnt and q not in self.answers and q not in self.in_progress_callback_queues:
                        self.in_progress_callback_queues[q] = []
                return True

    def i_have_computed(self, main_q, main_a, byproduct_qa_list=[]):
        with self.lock:
            self._dec_refcnt(main_q)
            for question, answer in [(main_q, main_a)] + byproduct_qa_list:
                if question in self.refcnt:
                    self.answers[question] = answer
                for queue in self.in_progress_callback_queues.pop(question, []):
                    queue.put(answer)
                    self._dec_refcnt(question)

    def put_answer_when_ready(self, question, queue):
        with self.lock:
            if question in self.answers:
                queue.put(self.answers[question])
                self._dec_refcnt(question)
            else:
                self.in_progress_callback_queues[question].append(queue)

    def wait_for_answer(self, question):
        queue = Queue.Queue()
        self.put_answer_when_ready(question, queue)
        return queue.get()

    def finished(self):
        if len(self.laq):
            raise WorkflowError("finished without flushing lookahead queue")
        elif self.refcnt != {}:
            raise WorkflowError("leaked references")
        elif self.answers != {}:
            raise WorkflowError("leaked cached answers")
        elif self.in_progress_callback_queues != {}:
            raise WorkflowError("leaked callback queues")

