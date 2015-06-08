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

def string_to_indumpco(strval, dumpdir_basename, reusedir_basenames=[], remote_segs=None):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	reuse = [os.path.join(tmpdir, d) for d in reusedir_basenames]

	remseg_file = None
	if remote_segs is not None:
		remseg_file = os.path.join(tmpdir, 'tmp-remotesegs')
		rs = open(remseg_file, 'w')
		for remseg in remote_segs:
			rs.write(remseg + "\n")
		rs.close()

	tmp = os.path.join(tmpdir, 'tmp-input')
	f = open(tmp, 'w')
	f.write(strval)
	f.close()
	f = open(tmp)

	indumpco.create_dump(src_fh=f, outdir=dumpdir, dumpdirs_for_reuse=reuse, remote_seg_list_file=remseg_file)

	os.unlink(tmp)
	if remseg_file is not None:
		os.unlink(remseg_file)

def assert_indumpco_matches(dumpdir_basename, expect_content, extra_blkdirs=[]):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	content = ''.join((seg for seg in indumpco.extract_dump(dumpdir, extra_blkdirs)))
	assert_equals(content, expect_content)

def check_indumpco_restores_input(input_str):
	dumpdir = 'eqcheck'
	string_to_indumpco(input_str, dumpdir)
	assert_indumpco_matches(dumpdir, input_str)
	shutil.rmtree(os.path.join(tmpdir, dumpdir))

def delete_indumpco(dumpdir_basename):
	shutil.rmtree(os.path.join(tmpdir, dumpdir_basename))
	
def segments_new_reused_absent(dumpdir_basename):
	dumpdir = os.path.join(tmpdir, dumpdir_basename)
	blkdir = os.path.join(dumpdir, 'blocks')
	idxpath = os.path.join(dumpdir, 'index')
	new_segs, reused_segs, absent_segs = 0, 0, 0
	for line in open(idxpath):
		segsum = line.strip().split()[1]
		path = os.path.join(blkdir, segsum)
		if os.path.exists(path):
			nlink = os.lstat(path).st_nlink
			if nlink == 1:
				new_segs += 1
			else:
				reused_segs += 1
		else:
			absent_segs += 1
	return new_segs, reused_segs, absent_segs

############################################################################

def test_short_strings():
	for s in ('\r', '\n', '', 'x', '\0', '\\', 'foo', '0'):
		check_indumpco_restores_input(s)
	
def test_long_string():
	input_str = ''.join(('%d bottles of beer on the wall, %d bottles of beer.\nIf one of those bottles should happen to fall, ' % (b,b) for b in xrange(500000, 1, -1)))
	string_to_indumpco(input_str, 'orig')
	assert_indumpco_matches('orig', input_str)

	origdir = os.path.join(tmpdir, 'orig')
	origdir_blocks = os.path.join(origdir, 'blocks')
	orig_seg_digests = os.listdir(origdir_blocks)

	# delete a few bytes from the middle, then incremental redump
	input_str = input_str[:4321] + input_str[4325:]
	string_to_indumpco(input_str, 'delbytes', ['orig'])
	assert_indumpco_matches('delbytes', input_str)
	new_segs, reused_segs, abs_segs = segments_new_reused_absent('delbytes')
	assert_true(new_segs <= 2, msg='small change creates no more than 2 new segments')
	assert_true(new_segs < reused_segs, msg='most segments reused after small change')
	assert_true(abs_segs == 0, msg='no absent segments')
	delete_indumpco('delbytes')

	# check that it works if the segments of the previous dump are considered 'remote' rather than being in a blockdir
	string_to_indumpco(input_str, 'delbytes2', [], set(orig_seg_digests))
	assert_indumpco_matches('delbytes2', input_str, [origdir_blocks])
	new_segs2, reused_segs2, abs_segs2 = segments_new_reused_absent('delbytes2')
	assert_true(new_segs2 == new_segs, msg='same number of new segments')
	assert_true(reused_segs2 == 0, msg='no segs reused from existing blockdirs')
	assert_equals(abs_segs2, reused_segs, msg='%d reused segs become %s absent segs' % (reused_segs, abs_segs2))
	delete_indumpco('delbytes2')

	delete_indumpco('orig')
