
import os, hashlib, sys, zlib, lzma, re
import fletcher_sum_split
from pll_pipe import parallel_pipe
from qa_caching_q import QACacheQueue, NOT_IN_CACHE

class Error(Exception):
    pass

class FormatError(Error):
    pass

def split_filehandle_into_segments(src_file):
    """ Read an open file to EOF, split it repeatably into segments

        A generator function to read src_file to EOF, and return the data
        as a sequence of variable length segments.  The segment boundaries
        are placed in a way that depends on the local contents of the file,
        so that splitting the same file twice will yield the same sequence
        of segments.

        Since the segment boundaries depend only on *local* file content,
        making a change near the start of the file doesn't move segment
        boundaries later in the file, even if the change involves
        inserting or deleting some bytes.

        This is useful for splitting something like a textual dump of a
        database into segments for individual compression and storage,
        since it allows compressed segments from a previous day's dump to
        be reused for the parts of the dump that have not changed.

        The minimum segment length is 1M, and the mean segment length is
        about 4M.
    """
    fss = fletcher_sum_split.new(src_file.fileno())
    while True:
        seg = fletcher_sum_split.readsegment(fss)
        if seg is None:
            return
        yield seg

def pack_idxline(seg_len, seg_sum):
    return "%d %s\n" % (int(seg_len), seg_sum)

def unpack_idxline(idxline):
    hit = re.match(r'^([0-9]+) (\w+)\s*$', idxline)
    if not hit:
        raise FormatError("malformed line in index file", (idxline,))
    seg_len = int(hit.group(1))
    seg_sum = hit.group(2)
    return seg_len, seg_sum

def _compress_string_to_file(src_str, dest_file):
    f = open(dest_file, 'w')
    f.write('z')
    f.write(zlib.compress(src_str, 9))
    f.close()

class BlockFileRead(object):
    def __init__(self, idxline, seg_search_path):
        self.idxline = idxline
        self.seg_len, self.seg_sum = unpack_idxline(idxline)
        self.blk_file = _path_search(self.seg_sum, seg_search_path)
        if self.blk_file is None:
            raise FormatError("index references a missing block file", (self.seg_sum, seg_search_path))
        self.blk_fh = open(self.blk_file)
        self.format_byte = self.blk_fh.read(1)
        if self.format_byte == 'z':
            self.byproduct_idxlines = []
        elif self.format_byte == 'x':
            self.overall_sum = self.blk_fh.readline().strip()
            embedded_idxline_count = int(self.blk_fh.readline().strip())
            self.embedded_idxlines = []
            for _ in range(embedded_idxline_count):
                self.embedded_idxlines.append(self.blk_fh.readline())
            self.overall_len = sum([unpack_idxline(i)[0] for i in self.embedded_idxlines])
            self.overall_idxline = pack_idxline(self.overall_len, self.overall_sum)
            self.all_idxlines = self.embedded_idxlines + [self.overall_idxline]
            self.byproduct_idxlines = [x for x in self.all_idxlines if x != idxline]
            if idxline not in self.embedded_idxlines:
                raise ValueError("desired sum not found in compound block", (idxline, self.blk_file))
        else:
            raise FormatError("invalid first byte of compressed block", (self.blk_file, format_byte))

    def read_and_uncompress(self):
        if self.format_byte == 'z':
            return zlib.decompress(self.blk_fh.read()), []
        elif self.format_byte == 'x':
            xz_data = self.blk_fh.read()
            unpacked_data = lzma.decompress(xz_data)
            idxline_to_seg = {}
            idxline_to_seg[self.overall_idxline] = unpacked_data
            offset = 0
            for idxline in self.embedded_idxlines:
                seg_len, seg_sum = unpack_idxline(idxline)
                idxline_to_seg[idxline] = unpacked_data[offset:offset+seg_len]
                offset += seg_len
            if offset != len(unpacked_data):
                raise ValueError("lzma data len not consistent with seg lens in header", self.blk_file)
            byproduct_qa = [(idxline, idxline_to_seg[idxline]) for idxline in self.all_idxlines if idxline != self.idxline]
            return idxline_to_seg[self.idxline], byproduct_qa
        else:
            raise FormatError("invalid first byte of compressed block", (self.blk_file, format_byte))

def repack_blocks(blockdir, repack_sums):
    idxlines = []
    compound_data = ''
    for s in repack_sums:
        f = open(os.path.join(blockdir, s))
        if f.read(1) != 'z':
            raise ValueError('attempt to repack non-zlib block', s)
        seg_data = zlib.decompress(f.read())
        idxlines.append(pack_idxline(len(seg_data), s))
        compound_data += seg_data

    compound_sum = hashlib.md5(compound_data).hexdigest()
    compressed_data = lzma.compress(compound_data)
    del compound_data
        
    compound_file = os.path.join(blockdir, compound_sum)
    f = open(compound_file, 'w')
    f.write('x%s\n%d\n%s' % (compound_sum, len(idxlines), ''.join(idxlines)))
    f.write(compressed_data)
    f.close()

    for s in repack_sums:
        f =  os.path.join(blockdir, s)
        os.link(compound_file, f+'.tmp')
        os.rename(f+'.tmp', f)

def _blockdir(dump_rootdir):
    return os.path.join(dump_rootdir, 'blocks')

def _path_search(seg_sum, path):
    for d in path:
        f = os.path.join(d, seg_sum)
        if os.path.exists(f):
            return f
    return None
 
def extract_dump(dumpdir, extra_block_dirs=[], thread_count=4):
    """ Generator function for restoring a compressed dump

        Concatenate the values yielded by this generator to get the
        uncompressed dump.
    """
    seg_search_path = [_blockdir(dumpdir)] + extra_block_dirs
    idxline_qa_iter = QACacheQueue(src_iterable = open(os.path.join(dumpdir, 'index')))

    def _idxline_processor(outq, idxline):
        seg = idxline_qa_iter.consume_cached_answer(idxline)
        if seg is NOT_IN_CACHE:
            blk_file_reader = BlockFileRead(idxline, seg_search_path)
            if idxline_qa_iter.i_should_compute(idxline, blk_file_reader.byproduct_idxlines):
                seg, byproduct_idxline_seg = blk_file_reader.read_and_uncompress()
                idxline_qa_iter.i_have_computed(idxline, seg, byproduct_idxline_seg)
            else:
                idxline_qa_iter.put_answer_when_ready(idxline, outq)
                return
        outq.put(seg)

    return parallel_pipe(idxline_qa_iter, _idxline_processor, thread_count)

def create_dump(src_fh, outdir, dumpdirs_for_reuse=[], thread_count=8, remote_seg_list_file=None):
    os.mkdir(outdir)
    blkdir = _blockdir(outdir)
    os.mkdir(blkdir)
    idx_file = os.path.join(outdir, 'index')
    idx_fh = open(idx_file, 'w')
    blk_reuse_dirs = [_blockdir(d) for d in dumpdirs_for_reuse]

    remote_segs = set()
    if remote_seg_list_file is not None:
        for line in open(remote_seg_list_file):
            remote_segs.add(line.strip())

    def _seg_processor(q, segment):
        segsum = hashlib.md5(segment).hexdigest()
        dest_path = os.path.join(blkdir, segsum)
        if segsum not in remote_segs and not os.path.exists(dest_path):
            reuse_path = _block_for_reuse(blk_reuse_dirs, segsum)
            if reuse_path is None:
                _compress_string_to_file(segment, dest_path)
            else:
                os.link(reuse_path, dest_path)
        q.put((segsum, len(segment)))
        
    src_iterator = split_filehandle_into_segments(src_fh)
    pipe = parallel_pipe(src_iterator, _seg_processor, thread_count)
    for segsum, seglen in pipe:
        idx_fh.write(pack_idxline(seglen, segsum))

def _block_for_reuse(blk_reuse_dirs, segsum):
    for blkdir in blk_reuse_dirs:
        reuse_path = os.path.join(blkdir, segsum)
        if os.path.exists(reuse_path):
            return reuse_path
    return None

    
if __name__ == '__main__':
    op = sys.argv[1]
    dumpdir = sys.argv[2]
    if op == "c":
        for_reuse = sys.argv[3:]
        DumpWriter(sys.stdin, dumpdir, for_reuse).run()
    elif op == "r":
        for block in generate_restored_dump(dumpdir):
            sys.stdout.write(block)

