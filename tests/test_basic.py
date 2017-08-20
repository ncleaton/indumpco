import tempfile, shutil, os, sys
from nose.tools import assert_equals, assert_true
import indumpco

from tutil import IndumpcoUnderTest, check_indumpco_restores_input

def test_short_strings():
    for s in ('\r', '\n', '', 'x', '\0', '\\', 'foo', '0'):
        check_indumpco_restores_input(s)
    
def test_stepped_blockdir():
    def mangler(idc):
        for hexdigit in [hex(x)[-1].lower() for x in range(16)]:
            os.mkdir(os.path.join(idc.blockdir, hexdigit))
        for seg in os.listdir(idc.blockdir):
            if len(seg) > 2:
                block_subdir = os.path.join(idc.blockdir, seg[0])
                os.rename(os.path.join(idc.blockdir, seg), os.path.join(block_subdir, seg))

    check_indumpco_restores_input('''afs lasfjlasf laskjdf lasfj asf
asldf aslfjas lfdslad lkjsadflkasf lsaflasdfjsldfj sladfjlaldsfjlsajfsadf
asdlf lasdfsad flsadladsdfj2 fsfsljflsfjs lasdfj    234028340f sadfjasflsl''', mangler)

def test_long_string():
    input_str = ''.join(('%d bottles of beer on the wall, %d bottles of beer.\nIf one of those bottles should happen to fall, ' % (b,b) for b in xrange(500000, 1, -1)))
    orig = IndumpcoUnderTest(input_str)
    assert_true(orig.restore_to_string() == input_str)

    # delete a few bytes from the middle, then incremental redump
    input_str = input_str[:4321] + input_str[4325:]
    delbytes = IndumpcoUnderTest(input_str, reuse_dumpdirs=[orig.dumpdir])
    assert_true(delbytes.restore_to_string() == input_str)
    assert_true(delbytes.new_segs <= 2, msg='small change creates no more than 2 new segments')
    assert_true(delbytes.new_segs < delbytes.reused_segs, msg='most segments reused after small change')
    assert_true(delbytes.absent_segs == 0, msg='no absent segments')
    delbytes.delete_data() # save a bit of space

    # check that it works if the segments of the previous dump are considered 'remote' rather than being in a blockdir
    delbytes2 = IndumpcoUnderTest(input_str, [], orig.set_of_digests)
    assert_true(delbytes2.restore_to_string(extra_blkdirs=[orig.blockdir]) == input_str)
    assert_true(delbytes2.new_segs == delbytes.new_segs, msg='same number of new segments')
    assert_true(delbytes2.reused_segs == 0, msg='no segs reused from existing blockdirs')
    assert_equals(delbytes2.absent_segs, delbytes.reused_segs, msg='%d reused segs become absent segs' % delbytes.reused_segs)
