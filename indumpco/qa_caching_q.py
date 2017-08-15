# -*- coding: utf-8 -*-

import Queue
from collections import defaultdict, deque
from itertools import islice
import sys

class QACacheQueue(object):
    """
    Designed for a situation in which you have an iterable of questions to
    work through, and questions may be repeated and you want to cache the
    answers to questions that are coming up again soon. The answers may be
    large, so you can't just blindly cache everything.

    Also, maybe computing the answer to a question can give rise to other
    question and answer pairs as a byproduct, and those other questions
    might also be coming up soon.

    Converts an iterator of questions into an iterator of
    (question, cached_answer) tuples. The cached answers are installed
    with calls to the offer_answer(question, answer) method, which will
    install the cached answer only if the specified question is coming
    up soon.

        qacq = QACachingQueue(questions_iterable, lookahead=1000)
        for question, answer in qacq:
            if answer is None:
                # The answer to this question wasn't cached, we'll have to work it out
                answer, byproduct_answers = do_compute_answer(question)

                # Offer QA pairs to the cache
                qacq.offer_answer(question, answer)
                for a, q in byproduct_answers:
                    qacq.offer_answer(q, a)

            do_something_with(answer)
    """
    def __init__(self, src_iterable, lookahead=1000, no_answer_cached=None):
        self.lookahead = lookahead
        self.no_answer_cached = no_answer_cached
        self.laq = deque()
        self.refcnt = defaultdict(int)
        self.answers = dict()
        self.src_iterator = iter(src_iterable)

    def _dec_refcnt(self, item):
        self.refcnt[item] -= 1
        if self.refcnt[item] < 1:
            self.refcnt.pop(item, None)
            self.answers.pop(item, None)

    def _pop_q_and_a(self):
        q = self.laq.popleft()
        a = self.answers.get(q, self.no_answer_cached)
        self._dec_refcnt(q)
        return (q, a)

    def __iter__(self):
        prepopulate_len = self.lookahead - len(self.laq)
        if prepopulate_len > 0:
            for item in islice(self.src_iterator, prepopulate_len):
                self.refcnt[item] += 1
                self.laq.append(item)

        for item in self.src_iterator:
            self.refcnt[item] += 1
            self.laq.append(item)
            yield self._pop_q_and_a()

        while len(self.laq):
            yield self._pop_q_and_a()

    def offer_answer(self, item, answer):
        if item in self.refcnt:
            self.answers[item] = answer
