
import os, hashlib, sys, zlib, lzma, re
import fletcher_sum_split
from pll_pipe import parallel_pipe
from lookahead_queue import LookaheadQueue

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

def parse_idxline(idxline):
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

def _uncompress_file_to_string(src_file, want_seg_sum=None, laq=None):
    f = open(src_file)
    format_byte = f.read(1)
    if format_byte == 'z':
        return zlib.decompress(f.read())
    elif format_byte == 'x':
        if want_seg_sum is None or laq is None:
            raise RuntimeError('require want_seg_sum and laq to unpack a compound block')
        overall_sum = f.readline().strip()
        embedded_seg_count = int(f.readline().strip())
        embedded_segs = list()
        for _ in range(embedded_seg_count):
            embedded_segs.append(f.readline())
        xz_data = f.read()
        unpacked_data = lzma.decompress(xz_data)
        offset = 0
        desired_data = None
        for idxline in embedded_segs:
            seg_len, seg_sum = parse_idxline(idxline)
            laq.put_answer(idxline, unpacked_data[offset:offset+seg_len])
            if seg_sum == want_seg_sum:
                desired_data = unpacked_data[offset:offset+seg_len]
            offset += seg_len
        if offset != len(unpacked_data):
            raise ValueError("xz data len not consistent with seg lens in header", src_file)
        if want_seg_sum == overall_sum:
            # desired data is the entire xz block
            return unpacked_data
        elif desired_data is not None:
            return desired_data
        else:
            raise ValueError("desired sum not found in compound block", (src_file, want_seg_sum))
    else:
        raise FormatError("invalid first byte of compressed block", (src_file, format_byte))

def repack_blocks(blockdir, repack_sums):
    idxlines = list()
    compound_data = ''
    for s in repack_sums:
        seg_data = _uncompress_file_to_string(os.path.join(blockdir, s))
        idxlines.append('%d %s\n' % (len(seg_data), s))
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
    idxline_laq = LookaheadQueue(src_iterable = open(os.path.join(dumpdir, 'index')))

    def _idxline_processor(idxline):
        if idxline_laq.have_answer(idxline):
            seg = idxline_laq.get_answer(idxline)
        else:
            seg_len, seg_sum = parse_idxline(idxline)
            blk_file = _path_search(seg_sum, seg_search_path)
            if blk_file is None:
                raise FormatError("index references a missing block file", (dumpdir, seg_sum, seg_search_path))
            seg = _uncompress_file_to_string(blk_file, seg_sum, idxline_laq)
            if len(seg) != seg_len:
                raise FormatError("Uncompressed segment has the wrong length", (dumpdir, blk_file, seg_len))

        idxline_laq.dec_refcnt(idxline)
        return seg

    return parallel_pipe(idxline_laq, _idxline_processor, thread_count)

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

    def _seg_processor(segment):
        segsum = hashlib.md5(segment).hexdigest()
        dest_path = os.path.join(blkdir, segsum)
        if segsum not in remote_segs and not os.path.exists(dest_path):
            reuse_path = _block_for_reuse(blk_reuse_dirs, segsum)
            if reuse_path is None:
                _compress_string_to_file(segment, dest_path)
            else:
                os.link(reuse_path, dest_path)
        return segsum, len(segment)
        
    src_iterator = split_filehandle_into_segments(src_fh)
    pipe = parallel_pipe(src_iterator, _seg_processor, thread_count)
    for segsum, seglen in pipe:
        idx_fh.write("%d %s\n" % (seglen, segsum))

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

