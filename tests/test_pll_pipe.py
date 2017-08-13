import sys, os, time
from nose.tools import assert_equals, raises

for libdir in os.listdir('build'):
	if libdir.startswith('lib.'):
		sys.path.insert(0, os.path.join('build', libdir))

from indumpco.pll_pipe import parallel_pipe

def worker_function(job):
	if job == "die_worker_die":
		raise StandardError("worker died")
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

@raises(StandardError)
def test_exception_in_worker_1thread():
	return list(parallel_pipe([1,"die_worker_die",3], worker_function, 1))

@raises(StandardError)
def test_exception_in_worker_2threads():
	return list(parallel_pipe([1,"die_worker_die",3], worker_function, 2))

@raises(TypeError)
def test_exception_in_source_iterator_1thread():
	return list(parallel_pipe(faulty_job_generator(), worker_function, 1))

@raises(TypeError)
def test_exception_in_source_iterator_2threads():
	return list(parallel_pipe(faulty_job_generator(), worker_function, 2))

