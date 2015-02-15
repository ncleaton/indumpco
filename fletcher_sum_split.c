#include <Python.h>
#include <cStringIO.h>

#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

/*********************************************************************

Fletcher Sum Split
==================

Code to split a stream of input bytes into segments.  The intention is
to place the segment boundaries so that a local change to the input
would effect only 1 or 2 segments even if the change involves inserting
or deleting bytes.  With fixed segment sizes, an insertion or deletion
near the start of the stream would effect all subsequent segments by
shifting data across segment boundaries.

At each byte of input, computes a value in the range 0 to N that depends
pseudo-randomly on the recent bytes up to that point.  The pattern that
ends a segment is a run of M non-zero values followed by a zero value.
This pattern was chosen because it has a guaranteed minimum length (M+1)
so very small segments can be avoided.

We want segment lengths to be as consistent as possible, so for a
particular value of M (setting the min segment length) we want to choose
N so that the probability of the end of segment pattern is as high as
possible, so that the mean segment length is not too much larger than
the minimum.

If N is much too large then 0 values are very rare so end of segment
patterns are very rare.  If N is much too small then 0 values are common
and runs of M non-zero values are very rare so end of segment patterns
are rare.  There is an optimum in between, at which the probability of
end of segment pattern is at a maximum.  Long story short: N should be
about the same as M.

So, we choose a prime number of about the same size as M, and our
pseudo random value is the Fletcher sum of some preceding bytes modulo
that prime.

*********************************************************************/

/* The size of the input pattern that triggers the end of a segment */
#define MINSEGSIZE_BITS 20 /* 1M min seg size, 4M typical seg size */
#define MINSEGSIZE (1<<MINSEGSIZE_BITS)

/* The size of the rolling window for Fletcher Checksum computation */
#define SUM_WINDOW_BITS (MINSEGSIZE_BITS-1)
#define SUM_WINDOW (1<<SUM_WINDOW_BITS)

/* The mean segment size, by observation. */ 
#define MEAN_SEGMENT_SIZE (4 * MINSEGSIZE)

/* A prime number near to MINSEGSIZE. */
#if MINSEGSIZE_BITS == 8
# define PRIME 257
#elif MINSEGSIZE_BITS == 9
# define PRIME 509
#elif MINSEGSIZE_BITS == 10
# define PRIME 1031
#elif MINSEGSIZE_BITS == 11
# define PRIME 2053
#elif MINSEGSIZE_BITS == 12
# define PRIME 4093
#elif MINSEGSIZE_BITS == 13
# define PRIME 8191
#elif MINSEGSIZE_BITS == 14
# define PRIME 16381
#elif MINSEGSIZE_BITS == 15
# define PRIME 32771
#elif MINSEGSIZE_BITS == 16
# define PRIME 65537
#elif MINSEGSIZE_BITS == 17
# define PRIME 131071
#elif MINSEGSIZE_BITS == 18
# define PRIME 262147
#elif MINSEGSIZE_BITS == 19
# define PRIME 524287
#elif MINSEGSIZE_BITS == 20
# define PRIME 1048573
#elif MINSEGSIZE_BITS == 21
# define PRIME 2097143
#elif MINSEGSIZE_BITS == 22
# define PRIME 4194301
#elif MINSEGSIZE_BITS == 23
# define PRIME 8388593
#elif MINSEGSIZE_BITS == 24
# define PRIME 16777213
#elif MINSEGSIZE_BITS == 25
# define PRIME 33554467
#else
# error "unsupported value for MINSEGSIZE_BITS"
#endif

/* How many bits must an integer type have so that it can hold the sum
** all the chars in a fletchsum window without overflow ? */
#define CHARSUM_BITS (SUM_WINDOW_BITS+CHAR_BIT)

/* How many bits must an integer type have so that it can hold the fletcher
** checksum over a fletchsum window without overflow ? */
#define FLETCHSUM_BITS (CHARSUM_BITS + SUM_WINDOW_BITS - 1)

/* How many bits must an integer type have so that it can hold the fletcher
** checksum over a fletchsum window modulo the prime without overflow ? */
#define FLETCHSUM_MODPRIME_BITS (MINSEGSIZE_BITS < FLETCHSUM_BITS ? MINSEGSIZE_BITS : FLETCHSUM_BITS)

////////////////////////////////////////////////////////////////

// Int types big enough to store some things:

// Convert max value to number of bits.  Don't shift by more
// than 8 at a time, because shifting by more than the width
// of a type can raise compiler warnings.
#define BITS1(c ) ( (c) ? 1 : 0 )
#define BITS2(c ) ( (c)>>1 ? 1 + BITS1( (c)>>1) : BITS1(c) )
#define BITS4(c ) ( (c)>>2 ? 2 + BITS2( (c)>>2) : BITS2(c) ) 
#define BITS8(c ) ( (c)>>4 ? 4 + BITS4( (c)>>4) : BITS4(c) ) 
#define BITS16(c) ( (c)>>8 ? 8 + BITS8( (c)>>8) : BITS8(c) ) 
#define BITS24(c) ( (c)>>8 ? 8 + BITS16((c)>>8) : BITS8(c) ) 
#define BITS32(c) ( (c)>>8 ? 8 + BITS24((c)>>8) : BITS8(c) ) 
#define BITS40(c) ( (c)>>8 ? 8 + BITS32((c)>>8) : BITS8(c) ) 
#define BITS48(c) ( (c)>>8 ? 8 + BITS40((c)>>8) : BITS8(c) ) 
#define BITS56(c) ( (c)>>8 ? 8 + BITS48((c)>>8) : BITS8(c) ) 
#define BITS64(c) ( (c)>>8 ? 8 + BITS56((c)>>8) : BITS8(c) ) 
#define BITS70(c) ( (c)>>8 ? 8 + BITS64((c)>>8) : BITS8(c) ) 

// The number of bits in some integer types
#define USHRT_BITS  BITS70(USHRT_MAX)
#define UINT_BITS   BITS70(UINT_MAX)
#define ULONG_BITS  BITS70(ULONG_MAX)
#define ULLONG_BITS BITS70(ULLONG_MAX)

/* An integer type just big enough to hold a charsum */
#if UINT_BITS > CHARSUM_BITS
typedef unsigned int charsum_t;
#elif ULONG_BITS > CHARSUM_BITS
typedef unsigned long charsum_t;
#elif ULLONG_BITS > CHARSUM_BITS
typedef unsigned long long charsum_t;
#else
# error "no unsigned int type can hold a charsum"
#endif

/* An integer type just big enough to hold a fletchsum */
#if UINT_BITS > FLETCHSUM_BITS
typedef unsigned int fletchsum_t;
#elif ULONG_BITS > FLETCHSUM_BITS
typedef unsigned long fletchsum_t;
#elif ULLONG_BITS > FLETCHSUM_BITS
typedef unsigned long long fletchsum_t;
#else
# error "no unsigned int type can hold a fletchsum"
#endif

/* An integer type just big enough to hold a fletchsum modulo the prime */
#if UINT_BITS > FLETCHSUM_MODPRIME_BITS
typedef unsigned int fletchsum_mp_t;
#elif ULONG_BITS > FLETCHSUM_MODPRIME_BITS
typedef unsigned long fletchsum_mp_t;
#elif ULLONG_BITS > FLETCHSUM_MODPRIME_BITS
typedef unsigned long long fletchsum_mp_t;
#else
# error "no unsigned int type can hold a fletchsum mod prime"
#endif

/////////////////////////////////////////////////////////////////

typedef struct {
	FILE *input;               // The file handle from which to read
	size_t bytes_into_seg;     // How far into the current segment we are
	size_t last_hit_at;        // Most recent byte at which fletchsum mod prime was 0
	unsigned char *blk;        // The current input block of SUM_WINDOW bytes
	unsigned char *prev_blk;   // The previous input block of SUM_WINDOW bytes
	unsigned char *blockstore; // Storage for blk and prev_blk
	PyObject *outbuf;          // Where we accumulate the segment
	int eof;                   // Have we seen EOF on the input filehandle
	charsum_t char_sum;        // Current character sum
	fletchsum_mp_t fletch_sum; // Current fletcher sum modulo prime
	fletchsum_mp_t precomputed_remove_oldbyte[256];
		/* When rolling the fletcher sum window forward one byte, we need to
		** add something to fletchsum-mod-prime to remove the effect of the
		** char that's no-longer in the window.  A lookup table speeds this up
		** a bit. */
} fss_state;

static char *cobj_name = "indumpco.fletcher_sum_split.fss_state";

static void
fsss_destroy(fss_state *fsss)
{
	if (fsss) {
		if (fsss->input)
			fclose(fsss->input);
		free(fsss->blockstore);
		Py_XDECREF(fsss->outbuf);
		free(fsss);
	}
}

static void
fsss_pyobj_destroy(PyObject *p)
{
	fsss_destroy(PyCapsule_GetPointer(p, cobj_name));
}

static void
sums_from_scratch(fss_state *fsss, unsigned char *buf)
/* Compute the character sum and fletcher sum of a buffer of length SUM_WINDOW.
*/
{
	charsum_t char_sum =0;
	fletchsum_t fletch_sum =0;
	int i;

	for ( i=0 ; i<SUM_WINDOW ; i++ ) {
		char_sum += buf[i];
		fletch_sum += char_sum;
	}

	fsss->char_sum = char_sum;
	fsss->fletch_sum = fletch_sum % PRIME;
}

static PyObject *
fletcher_sum_split_new(PyObject *self, PyObject *args)
{
    fss_state *fsss;
	int gotbytes, fd, i;

	if (!PyArg_ParseTuple(args, "i", &fd))
		return NULL;

	fsss = malloc(sizeof(*fsss));
    if (!fsss)
		return PyErr_NoMemory();
	fsss->blockstore = malloc(2 * SUM_WINDOW);
	fsss->outbuf = PycStringIO->NewOutput(2 * MEAN_SEGMENT_SIZE);
	if (!fsss->blockstore || !fsss->outbuf) {
		fsss_destroy(fsss);
		return PyErr_NoMemory();
	}
	fsss->blk = fsss->blockstore;
	fsss->prev_blk = fsss->blockstore + SUM_WINDOW;

	for ( i=0 ; i<256 ; i++ ) {
		fsss->precomputed_remove_oldbyte[i] = \
				PRIME - (((charsum_t)SUM_WINDOW * (charsum_t)i) % PRIME);
	}

	fd = dup(fd);
	if (fd < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		fsss_destroy(fsss);
		return NULL;
	}

	fsss->input = fdopen(fd, "r");
	if (! fsss->input ) {
		PyErr_SetFromErrno(PyExc_IOError);
		fsss_destroy(fsss);
		close(fd);
		return NULL;
	}

	fsss->bytes_into_seg = 0;
	fsss->last_hit_at = 0;
	fsss->eof = 0;

	gotbytes = fread(fsss->prev_blk, 1, SUM_WINDOW, fsss->input);
	fsss->bytes_into_seg = gotbytes;
	if (PycStringIO->cwrite(fsss->outbuf, (const char *)fsss->prev_blk, gotbytes) != gotbytes) {
		fsss_destroy(fsss);
		return NULL;
	}

	if (gotbytes == SUM_WINDOW) {
		sums_from_scratch(fsss, fsss->prev_blk);
		if (fsss->fletch_sum == 0) {
			fsss->last_hit_at = SUM_WINDOW;
		}
	} else {
		fsss->eof = 1;
	}

	return PyCapsule_New(fsss, cobj_name, fsss_pyobj_destroy);
}

static PyObject *
convert_cstringio_to_string(PyObject *cstringio)
{
	PyObject *res;

	res = PycStringIO->cgetvalue(cstringio);
	Py_DECREF(cstringio);
	return res;
}

static PyObject *
fletcher_sum_split_readsegment(PyObject *self, PyObject *args)
{
    fss_state *fsss;
	PyObject *pobj, *complete_seg_outbuf, *newoutbuf;
	unsigned char *tmp;
	charsum_t char_sum;
	unsigned int gotbytes, newseg_bytes =0;
	fletchsum_mp_t fletch_sum;
	int i;

    if (!PyArg_ParseTuple(args, "O", &pobj))
        return NULL;
    fsss = PyCapsule_GetPointer(pobj, cobj_name);
	if (!fsss)
        return NULL;

	if (fsss->eof) {
		if (fsss->outbuf) {
			pobj = convert_cstringio_to_string(fsss->outbuf);
			fsss->outbuf = NULL;
			return pobj;
		} else {
			Py_RETURN_NONE;
		}
	}

	char_sum = fsss->char_sum;
	fletch_sum = fsss->fletch_sum;
	for (;;) {
		gotbytes = fread(fsss->blk, 1, SUM_WINDOW, fsss->input);
		if (gotbytes < SUM_WINDOW) {
			fsss->eof = 1;
			if (PycStringIO->cwrite(fsss->outbuf, (const char *)fsss->blk, gotbytes) != gotbytes)
				return NULL;
			pobj = fsss->outbuf;
			fsss->outbuf = NULL;
			return convert_cstringio_to_string(pobj);
		}

		complete_seg_outbuf = NULL;
		for ( i=0 ; i<SUM_WINDOW ; i++ ) {
			char_sum += fsss->blk[i] - fsss->prev_blk[i];
			fletch_sum += char_sum + fsss->precomputed_remove_oldbyte[fsss->prev_blk[i]];
			fletch_sum %= PRIME;

			if (fletch_sum == 0) {
				if (fsss->bytes_into_seg + i > fsss->last_hit_at + MINSEGSIZE) {
					// End of segment pattern found.
					// Split this block between the current segment and a new segment.
					if (complete_seg_outbuf) {
						PyErr_SetString(PyExc_AssertionError, "multiple segment end patterns in a block, should be impossible");
						return NULL;
					}
					if (PycStringIO->cwrite(fsss->outbuf, (const char *)fsss->blk, i+1) != i+1)
						return NULL;
					newoutbuf = PycStringIO->NewOutput(2 * MEAN_SEGMENT_SIZE);
					if (!newoutbuf)
						return NULL;
					newseg_bytes = SUM_WINDOW - (i+1);
					if (newseg_bytes > 0) {
						if (PycStringIO->cwrite(newoutbuf, (const char *)(fsss->blk+i+1), newseg_bytes) != newseg_bytes)
							return NULL;
					}
					complete_seg_outbuf = fsss->outbuf;
					fsss->outbuf = newoutbuf;
				}
				fsss->last_hit_at = fsss->bytes_into_seg + i;
			}
		}
		tmp = fsss->blk;
		fsss->blk = fsss->prev_blk;
		fsss->prev_blk = tmp;

		if (complete_seg_outbuf) {
			// Found the end of segment pattern somewhere in this block
			fsss->char_sum = char_sum;
			fsss->fletch_sum = fletch_sum;
			fsss->bytes_into_seg = newseg_bytes;
			return convert_cstringio_to_string(complete_seg_outbuf);
		} else {
			// No end of segment, this entire block goes in the current segment
			if (PycStringIO->cwrite(fsss->outbuf, (const char *)fsss->prev_blk, SUM_WINDOW) != SUM_WINDOW)
				return NULL;
			fsss->bytes_into_seg += SUM_WINDOW;
		}
	}
}

/*
static PyObject *
fletcher_sum_split_diagstuff(PyObject *self, PyObject *args)
{
	unsigned char *splurge;
	int i;
	fss_state fsss;

	fprintf(stderr, "MINSEGSIZE_BITS=%d, MINSEGSIZE=%d\n", MINSEGSIZE_BITS, MINSEGSIZE);
	fprintf(stderr, "SUM_WINDOW_BITS=%d, SUM_WINDOW=%d, PRIME=%d\n", SUM_WINDOW_BITS, SUM_WINDOW, PRIME);
	fprintf(stderr, "CHARSUM_BITS=%d, FLETCHSUM_BITS=%d, FLETCHSUM_MODPRIME_BITS=%d\n", CHARSUM_BITS, FLETCHSUM_BITS, FLETCHSUM_MODPRIME_BITS);
	fprintf(stderr, "sizeof(charsum_t)=%lu, sizeof(fletchsum_t)=%lu, sizeof(fletchsum_mp_t)=%lu\n", sizeof(charsum_t), sizeof(fletchsum_t), sizeof(fletchsum_mp_t));

	splurge = malloc(SUM_WINDOW);
	for ( i=0 ; i<SUM_WINDOW ; i++ ) {
		splurge[i] = i % 256;
	}

	for ( i=0 ; i<3000 ; i++ ) {
		sums_from_scratch(&fsss, splurge);
		splurge[i] += fsss.char_sum % 128;
	}

	Py_RETURN_NONE;
}
*/

static PyMethodDef fletcher_sum_split_methods[] = {
    {"new",  fletcher_sum_split_new, METH_VARARGS, "create a new segment reader"},
    {"readsegment",  fletcher_sum_split_readsegment, METH_VARARGS, "read the next segment"},
    /* {"diagstuff",  fletcher_sum_split_diagstuff, METH_VARARGS, "output diagnostics"}, */
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

void
initfletcher_sum_split(void)
{
	PycString_IMPORT;
	Py_InitModule("indumpco.fletcher_sum_split", fletcher_sum_split_methods);
}
