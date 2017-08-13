import sys, os, time
from nose.tools import assert_equals, raises

for libdir in os.listdir('build'):
	if libdir.startswith('lib.'):
		sys.path.insert(0, os.path.join('build', libdir))

from indumpco.pll_pipe import parallel_pipe

class FaultyWorkerError(StandardError):
	pass

def worker_function(job):
	print >>sys.stderr, "worker got job", job
	if job == "die_worker_die":
		raise FaultyWorkerError("worker died")
	elif job == 0:
		raise ValueError("job 0 made it to a worker")
	time.sleep(job / 100)
	return job

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

