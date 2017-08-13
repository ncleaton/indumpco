from nose.tools import assert_true, assert_equals
from tutil import IndumpcoUnderTest
import random
import indumpco


def do_repack_t(input_str):
	idc = IndumpcoUnderTest(input_str)
	digests = list(idc.set_of_digests)
	random.shuffle(digests)
	indumpco.repack_blocks(idc.blockdir, digests)
	assert_true(idc.restore_to_string() == input_str, msg="repack failed to preserve data")

def test_repack_short():
	do_repack_t('asd-0f98a-sdf9a-sf9a-sfd9as-df9a-sdf9-as9f-asdf9as-df')

def test_repack_long():
	for _ in range(5):
		do_repack_t(''.join(('%d bottles of beer on the wall, %d bottles of beer.\nIf one of those bottles should happen to fall, ' % (b,b) for b in xrange(500000, 1, -1))))
