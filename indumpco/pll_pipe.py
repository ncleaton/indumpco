
import threading, Queue, sys
import weakref

_NO_MORE_INPUT = object()

class PipeState(object):
    def __init__(self, queue_maxsize):
        self.dest_queue = Queue.Queue(maxsize=queue_maxsize)

        self.source_queue = Queue.Queue(maxsize=queue_maxsize)
        self.source_queue_finished = False
        self.source_queue_lock = threading.RLock()

        self.transient_queues = weakref.WeakSet()
        self.exception = None
        self.state_lock = threading.RLock()

    def record_exception(self, exc):
        with self.state_lock:
            if self.exception is not None:
                return
            self.exception = exc

            # Help any blocked threads to exit
            for q in list(self.transient_queues):
                try:
                    q.put(_NO_MORE_INPUT, block=False)
                except Queue.Full:
                    pass
            q = Queue.Queue()
            q.put(_NO_MORE_INPUT)
            try:
                self.dest_queue.put(q, block=False)
            except Queue.Full:
                pass
            while True:
                try:
                    self.source_queue.get(block=False)
                except Queue.Empty:
                    break
            self.source_queue.put(_NO_MORE_INPUT)

def _source_reader_thread(pipe_state, source_iterable):
    # A thread to read from the input iterable into a queue.
    try:
        for job in source_iterable:
            with pipe_state.state_lock:
                if pipe_state.exception is not None:
                    break

            pipe_state.source_queue.put(job)

            with pipe_state.state_lock:
                if pipe_state.exception is not None:
                    break
    except Exception:
        pipe_state.record_exception(sys.exc_info())

    pipe_state.source_queue.put(_NO_MORE_INPUT)


def _worker_thread(pipe_state, worker_func):
    # A thread to process jobs from input queue to output queue.
    try:
        while True:
            # There's a lock around the process of obtaining a job from the input queue
            # and pushing a placeholder for the result onto the output queue. This ensures
            # that job order is preserved when there are many worker threads.
            # The placeholder takes the form of a reference to another queue, to which the
            # result will be pushed when it's ready.
            my_q = Queue.Queue()
            pipe_state.transient_queues.add(my_q)
            with pipe_state.source_queue_lock:
                if pipe_state.source_queue_finished:
                    return

                job = pipe_state.source_queue.get()

                if job is _NO_MORE_INPUT:
                    pipe_state.source_queue_finished = True
                    my_q.put(_NO_MORE_INPUT)
                    pipe_state.dest_queue.put(my_q)

                if pipe_state.source_queue_finished:
                    return

                pipe_state.dest_queue.put(my_q)

            worker_func(my_q, job)

    except Exception:
        pipe_state.record_exception(sys.exc_info())

def _start_daemon_threads(target, args, count):
    tlist = []
    for _ in range(count):
        t = threading.Thread(target=target, args=args)
        t.daemon = True
        t.start()
        tlist.append(t)
    return tlist

def parallel_pipe(source_iterable, worker_func, thread_count, queue_size=10):
    state = PipeState(queue_maxsize = thread_count * queue_size)

    child_threads = _start_daemon_threads(_source_reader_thread, (state, source_iterable), 1) + \
            _start_daemon_threads(_worker_thread, (state, worker_func), thread_count)

    while True:
        result = state.dest_queue.get().get()
        if result is _NO_MORE_INPUT:
            break
        else:
            yield result

    # Drain the dest queue, in case we're exiting on an exception and
    # something is blocked on put.
    while True:
        try:
            state.dest_queue.get(block=False)
        except Queue.Empty:
            break

    for t in child_threads:
        t.join()

    e = state.exception
    if e is not None:
        raise e[0], e[1], e[2]
