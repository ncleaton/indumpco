
import os, hashlib, sys, zlib, lzma, re
import fletcher_sum_split
import file_format
from pll_pipe import parallel_pipe
from qa_caching_q import QACacheQueue, NOT_IN_CACHE

class Error(Exception):
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

def repack_blocks(blockdir, repack_sums):
    idxlines = []
    compound_data = ''
    for s in repack_sums:
        seg_data = file_format.decompress_zfile_to_string(os.path.join(blockdir, s))
        idxlines.append(file_format.pack_idxline(len(seg_data), s))
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

def extract_dump(dumpdir, extra_block_dirs=[], thread_count=4):
    """ Generator function for restoring a compressed dump

        Concatenate the values yielded by this generator to get the
        decompressed dump.
    """
    seg_search_path = file_format.BlockDirs([_blockdir(dumpdir)] + extra_block_dirs)
    idxline_qa_iter = QACacheQueue(src_iterable = open(os.path.join(dumpdir, 'index')))

    def _idxline_processor(outq, idxline):
        seg = idxline_qa_iter.consume_cached_answer(idxline)
        if seg is NOT_IN_CACHE:
            seg_len, seg_sum = file_format.unpack_idxline(idxline)
            blk_filename = seg_search_path.find_block(seg_sum)
            blk_file_reader = file_format.BlockFileRead(seg_sum, blk_filename)
            if idxline_qa_iter.i_should_compute(idxline, blk_file_reader.extra_idxlines):
                extra_idxline_seg = []
                if blk_file_reader.is_x_group:
                    want_idxline_set = set([idxline]) | blk_file_reader.extra_idxlines
                    for got_idxline, got_seg in blk_file_reader.x_unpack_segs(want_idxline_set):
                        if got_idxline == idxline:
                            seg = got_seg
                        else:
                            extra_idxline_seg.append((got_idxline, got_seg))
                    if seg is NOT_IN_CACHE:
                        raise RuntimeError("didn't get expected idxline", (idxline, repr(want_idxline_set)))
                else:
                    seg = blk_file_reader.z_unpack_seg()
                idxline_qa_iter.i_have_computed(idxline, seg, extra_idxline_seg)
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
                file_format.compress_string_to_zfile(segment, dest_path)
            else:
                os.link(reuse_path, dest_path)
        q.put((segsum, len(segment)))
        
    src_iterator = split_filehandle_into_segments(src_fh)
    pipe = parallel_pipe(src_iterator, _seg_processor, thread_count)
    for segsum, seglen in pipe:
        idx_fh.write(file_format.pack_idxline(seglen, segsum))

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

