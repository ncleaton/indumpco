# -*- coding: utf-8 -*-

import zlib
import lzma
import re

class FormatError(Exception):
    pass

def pack_idxline(seg_len, seg_sum):
    return "%d %s\n" % (int(seg_len), seg_sum)

def unpack_idxline(idxline):
    hit = re.match(r'^([0-9]+) (\w+)\s*$', idxline)
    if not hit:
        raise FormatError("malformed index line", idxline)
    seg_len = int(hit.group(1))
    seg_sum = hit.group(2)
    return seg_len, seg_sum

class BlockFileRead(object):
    def __init__(self, seg_sum, filename):
        self.main_seg_sum = seg_sum
        self.filename = filename
        self.fh = open(filename)
        self.format_byte = self.fh.read(1)
        self.extra_idxlines = set()
        if self.format_byte == 'z':
            self.is_x_group = False
        elif self.format_byte == 'x':
            self.is_x_group = True
            self.x_overall_sum = self.fh.readline().strip()
            embedded_idxline_count = int(self.fh.readline().strip())
            self.x_embedded_idxlines = []
            self.x_overall_len = 0
            for _ in range(embedded_idxline_count):
                idxline = self.fh.readline()
                self.x_embedded_idxlines.append(idxline)
                xseglen, xsegsum = unpack_idxline(idxline)
                self.x_overall_len += xseglen
                if xsegsum != self.main_seg_sum:
                    self.extra_idxlines.add(idxline)
            self.x_overall_idxline = pack_idxline(self.x_overall_len, self.x_overall_sum)
            if self.main_seg_sum != self.x_overall_sum:
                self.extra_idxlines.add(self.x_overall_idxline)
        else:
            raise FormatError("invalid first byte of compressed block", (self.blk_file, format_byte))

    def x_unpack_segs(self, desired_idxline_set):
        xz_data = self.fh.read()
        unpacked_data = lzma.decompress(xz_data)
        if self.x_overall_idxline in desired_idxline_set:
            yield (self.x_overall_idxline, unpacked_data)
        offset = 0
        for idxline in self.x_embedded_idxlines:
            xseglen, xsegsum = unpack_idxline(idxline)
            if idxline in desired_idxline_set:
                yield (idxline, unpacked_data[offset:offset+xseglen])
            offset += xseglen
        if offset != len(unpacked_data):
            raise FormatError("lzma data len not consistent with seg lens in x header", self.filename)

    def z_unpack_seg(self):
        return zlib.decompress(self.fh.read())

def compress_string_to_zfile(src_str, dest_file):
    f = open(dest_file, 'w')
    f.write('z')
    f.write(zlib.compress(src_str, 9))
    f.close()

def decompress_zfile_to_string(src_file):
    f = open(src_file)
    formatbyte = f.read(1)
    if formatbyte != 'z':
        raise FormatError("blockfile is not a zlib file", (src_file, formatbyte))
    return zlib.decompress(f.read())
