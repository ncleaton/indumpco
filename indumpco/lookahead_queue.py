
import Queue
from collections import defaultdict, deque
from itertools import islice
import sys

class LookaheadQueue(object):
    def __init__(self, src_iterable, lookahead=1000):
        self.lookahead = lookahead
        self.laq = deque()
        self.refcnt = defaultdict(int)
        self.answers = dict()
        self.src_iterator = iter(src_iterable)

    def do_iterate(self):
        prepopulate_len = self.lookahead - len(self.laq)
        if prepopulate_len > 0:
            for item in islice(self.src_iterator, prepopulate_len):
                self.refcnt[item] += 1
                self.laq.append(item)

        for item in self.src_iterator:
            self.refcnt[item] += 1
            self.laq.append(item)
            yield self.laq.popleft()

        while len(self.laq):
            yield self.laq.popleft()

    def __iter__(self):
        return self.do_iterate()

    def dec_refcnt(self, item):
        self.refcnt[item] -= 1
        if self.refcnt[item] < 1:
            self.refcnt.pop(item, None)
            self.answers.pop(item, None)
        
    def have_answer(self, item):
        return item in self.answers

    def get_answer(self, item):
        return self.answers.get(item)
        
    def put_answer(self, item, answer):
        if item in self.refcnt:
            self.answers[item] = answer
