
import threading, Queue, sys


_NO_MORE_INPUT = object()


class PipeState(object):
	def __init__(self, dest_queue):
		self.dest_queue = dest_queue

		self.source_queue = Queue.Queue()
		self.source_queue_lock = threading.RLock()
		self.source_queue_finished = False

		self.exception = None
		self.exception_lock = threading.RLock()

	def get_exception(self, exc):
		with self.exception_lock:
			return self.exception

	def record_exception(self, exc):
		with self.exception_lock:
			if self.exception is None:
				self.exception = exc


def _source_reader_thread(pipe_state, source_iterable):
	# A thread to read from the input iterable into a queue.
	try:
		for job in source_iterable:
			with pipe_state.exception_lock:
				if pipe_state.exception is not None:
					break

			pipe_state.source_queue.put(job)

			with pipe_state.exception_lock:
				if pipe_state.exception is not None:
					break
	except Exception:
		pipe_state.record_exception(sys.exc_info())

	pipe_state.source_queue.put(_NO_MORE_INPUT)


def _worker_thread(pipe_state, worker_func):
	# A thread to process jobs from input queue to output queue.
	my_q = Queue.Queue()
	try:
		while True:
			# There's a lock around the process of obtaining a job from the input queue
			# and pushing a placeholder for the result onto the output queue. This ensures
			# that job order is preserved when there are many worker threads.
			# The placeholder takes the form of a reference to another queue, to which the
			# result will be pushed when it's ready.
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

			my_q.put(worker_func(job))

	except Exception:
		pipe_state.record_exception(sys.exc_info())

		# Unblock the main thread
		my_q.put(_NO_MORE_INPUT)
		pipe_state.dest_queue.put(my_q) # This may or may not be needed, depending on where in the loop above the exception struck.

		# Help the source reader thread to exit, it may be blocked on queue put.
		with pipe_state.source_queue_lock:
			if not pipe_state.source_queue_finished:
				job = None
				while job is not _NO_MORE_INPUT:
					job = pipe_state.source_queue.get()
				pipe_state.source_queue_finished = True


def _start_daemon_threads(target, args, count):
	tlist = []
	for _ in range(count):
		t = threading.Thread(target=target, args=args)
		t.daemon = True
		t.start()
		tlist.append(t)
	return tlist


def parallel_pipe(source_iterable, worker_func, thread_count, queue_size=10):
	q = Queue.Queue(maxsize = thread_count * queue_size)
	state = PipeState(q)

	child_threads = _start_daemon_threads(_source_reader_thread, (state, source_iterable), 1) + \
			_start_daemon_threads(_worker_thread, (state, worker_func), thread_count)

	while True:
		result = q.get().get()
		if result is _NO_MORE_INPUT:
			break
		else:
			yield result

	for t in child_threads:
		t.join()

	e = state.exception
	if e is not None:
		raise e[0], e[1], e[2]

