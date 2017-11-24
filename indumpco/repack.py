# -*- coding: utf-8 -*-

import hashlib
import os
import lzma
import file_format

def split_index_into_groups(index_fh):
    misses_since_last_hit = 0
    group = []
    for line in index_fh:
        group.append(line)
        seg_len, seg_sum = file_format.unpack_idxline(line)
        is_hit =  seg_sum[0] in ['0', '1', '2', '3']
        if is_hit:
            if misses_since_last_hit >= 4:
                yield group
                group = []
            misses_since_last_hit = 0
        else:
            misses_since_last_hit += 1

    if len(group):
        yield group

def repack_blocks(index_file, block_dir):
    bd = file_format.BlockDir(block_dir)
    for idxline_group in split_index_into_groups(open(index_file)):
        size_change = repack_idxgroup(bd, idxline_group)
        if size_change is not None:
            group_digest = hashlib.md5(''.join(idxline_group)).hexdigest()
            yield (group_digest, size_change)

def repack_idxgroup(bd, idxline_group):
    len_sum = [file_format.unpack_idxline(i) for i in idxline_group]
    len_sum_file = [(ls[0], ls[1], bd.filename(ls[1])) for ls in len_sum] 
    for seg_len, seg_sum, seg_file in len_sum_file:
        if seg_file is None:
            file_format.FormatError('index references missing block file', (index_file, seg_sum))
        if open(seg_file).read(1) != 'z':
            # cannot a repack a group unless it's all z-blocks
            return None
    
    # Unpack all the zlib blocks and repack as one big lzma block
    unpacked_data = ''.join((file_format.decompress_zfile_to_string(lsf[2]) for lsf in len_sum_file))
    overall_sum = hashlib.md5(unpacked_data).hexdigest()
    overall_len = len(unpacked_data)
    packed_data = lzma.compress(unpacked_data)
    del unpacked_data

    orig_compressed_size = sum([os.lstat(lsf[2]).st_size for lsf in len_sum_file])
    size_change = float(len(packed_data)) / float(orig_compressed_size)

    if size_change < 0.9:
        # Some compression improvement, we'll replace the files
    
        overall_file = bd.filename(overall_sum)
        tmp = overall_file + ".tmp"
        f = open(tmp, 'w')
        f.write('x%s\n%d\n%s' % (overall_sum, len(idxline_group), ''.join(idxline_group)))
        f.write(packed_data)
        f.close()
        os.rename(tmp, overall_file)

        # Replace all the zfiles now in this x block with hardlinks to the x block
        for _, _, filename in len_sum_file:
            tmp = filename + 'tmp'
            os.link(overall_file, tmp)
            os.rename(tmp, filename)

    return size_change

def rewrite_index(index_file, block_dir):
    bd = file_format.BlockDir(block_dir)
    for idxline_group in split_index_into_groups(open(index_file)):
        _, seg_sum = file_format.unpack_idxline(idxline_group[0])
        reader = file_format.BlockFileRead(seg_sum, bd.filename(seg_sum))
        if reader.is_x_group and idxline_group == reader.x_embedded_idxlines:
            overall_len = sum([file_format.unpack_idxline(i)[0] for i in idxline_group])
            print file_format.pack_idxline(overall_len, reader.x_overall_sum),
        else:
            print ''.join(idxline_group),
    
if __name__ == '__main__':
    import sys
    index_file, block_dir = sys.argv[1:]
    rewrite_index(index_file, block_dir)
