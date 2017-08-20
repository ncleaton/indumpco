import tempfile, shutil, os, sys
from nose.tools import assert_equals

import indumpco

class IndumpcoUnderTest(object):
    def __init__(self, input_data, reuse_dumpdirs=[], remote_segs=None):
        self.basedir = tempfile.mkdtemp()
        self.tmpdir = os.path.join(self.basedir, 'tmp')
        os.mkdir(self.tmpdir)
        self.reuse_dumpdirs = reuse_dumpdirs
        self.remote_segs = remote_segs
        self.dumpdir = os.path.join(self.basedir, 'd')
        self.blockdir = os.path.join(self.dumpdir, 'blocks')

        remseg_file = None
        if remote_segs is not None:
            remseg_file = os.path.join(self.tmpdir, 'remotesegs')
            rs = open(remseg_file, 'w')
            for remseg in remote_segs:
                rs.write(remseg + "\n")
            rs.close()

        tmp = os.path.join(self.tmpdir, 'input')
        f = open(tmp, 'w')
        f.write(input_data)
        f.close()
        f = open(tmp)

        indumpco.create_dump(src_fh=f, outdir=self.dumpdir, dumpdirs_for_reuse=reuse_dumpdirs, remote_seg_list_file=remseg_file)
        os.unlink(tmp)
        if remseg_file is not None:
            os.unlink(remseg_file)

        # Gather stats on segment reuse. This works by counting hardlinks on
        # segment files, so unless we do it now the deletion of the dump(s)
        # from which we've reused segments might confuse the count.
        self._compute_segments_new_reused_absent()

    def _compute_segments_new_reused_absent(self):
        idxpath = os.path.join(self.dumpdir, 'index')
        self.new_segs, self.reused_segs, self.absent_segs = 0, 0, 0
        self.set_of_digests = set()
        for line in open(idxpath):
            segsum = line.strip().split()[1]
            self.set_of_digests.add(segsum)
            path = os.path.join(self.blockdir, segsum)
            if os.path.exists(path):
                nlink = os.lstat(path).st_nlink
                if nlink == 1:
                    # a segment unique to this dump
                    self.new_segs += 1
                else:
                    # a reused segment hardlinked from another dump
                    self.reused_segs += 1
            else:
                self.absent_segs += 1
    
    def delete_data(self):
        if os.path.exists(self.basedir) and 'INDUMPCO_TEST_NODEL' not in os.environ:
            shutil.rmtree(self.basedir)

    def __del__(self):
        self.delete_data()

    def restore_to_string(self, extra_blkdirs=[]):
        if not os.path.exists(self.basedir):
            raise RuntimeError('restore attempt after data deleted', (self, self.basedir))
        return ''.join((seg for seg in indumpco.extract_dump(self.dumpdir, extra_blkdirs)))


def check_indumpco_restores_input(input_str, mangler_callback=None):
    idc = IndumpcoUnderTest(input_str)
    if mangler_callback is not None:
        mangler_callback(idc)
    assert_equals(idc.restore_to_string(), input_str)
