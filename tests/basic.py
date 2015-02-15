import tempfile, shutil, os, sys
from nose.tools import assert_equals, assert_true

for libdir in os.listdir('build'):
	if libdir.startswith('lib.'):
		sys.path.insert(0, os.path.join('build', libdir))
import indumpco

tmpdir = tempfile.mkdtemp()

def teardown_module(module):
	if tmpdir is not None and os.path.exists(tmpdir):
		shutil.rmtree(tmpdir)

def string_to_indumpco(strval, dumpdir_basename, reusedir_basenames=[]):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	reuse = [os.path.join(tmpdir, d) for d in reusedir_basenames]
	tmp = os.path.join(tmpdir, 'tmp-input')
	f = open(tmp, 'w')
	f.write(strval)
	f.close()
	f = open(tmp)
	indumpco.create_dump(f, dumpdir, reuse)
	os.unlink(tmp)

def assert_indumpco_matches(dumpdir_basename, expect_content):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	content = ''.join((seg for seg in indumpco.extract_dump(dumpdir)))
	assert_equals(content, expect_content)

def check_indumpco_restores_input(input_str):
	dumpdir = 'eqcheck'
	string_to_indumpco(input_str, dumpdir)
	assert_indumpco_matches(dumpdir, input_str)
	shutil.rmtree(os.path.join(tmpdir, dumpdir))
	
def segments_new_reused(dumpdir_basename):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	blkdir = os.path.join(dumpdir, 'blocks')
	new_segs, reused_segs = 0, 0
	for f in os.listdir(blkdir):
		path = os.path.join(blkdir, f)
		nlink = os.lstat(path).st_nlink
		if nlink == 1:
			new_segs += 1
		else:
			reused_segs += 1
	return new_segs, reused_segs
	
############################################################################

def test_short_strings():
	for s in ('\r', '\n', '', 'x', '\0', '\\', 'foo', '0'):
		check_indumpco_restores_input(s)
	
def test_long_string():
	input_str = ''.join(('%d bottles of beer on the wall, %d bottles of beer.\nIf one of those bottles should happen to fall, ' % (b,b) for b in xrange(500000, 1, -1)))
	string_to_indumpco(input_str, 'orig')
	assert_indumpco_matches('orig', input_str)

	input_str = input_str[:4321] + input_str[4325:]
	string_to_indumpco(input_str, 'delbytes', ['orig'])
	assert_indumpco_matches('delbytes', input_str)
	new_segs, reused_segs = segments_new_reused('delbytes')
	assert_true(new_segs <= 2, msg='small change creates no more than 2 new segments')
	assert_true(new_segs < reused_segs, msg='most segments reused after small change')

	for d in 'orig', 'delbytes':
		shutil.rmtree(os.path.join(tmpdir, d))
