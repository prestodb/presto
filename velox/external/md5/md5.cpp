/*
 *  This code is taken from duckdb/common/crypto/md5.hpp with following changes:
 *  1. removed other duckdb dependencies.
 *  2. Add the DigestToBase10 method which produce the md5 in decimal
 */
/*
** This code taken from the SQLite test library.  Originally found on
** the internet.  The original header comment follows this comment.
** The code is largerly unchanged, but there have been some modifications.
*/
/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */
#include "md5.h"
#include <folly/Conv.h>
#include <folly/Format.h>

namespace facebook::velox::crypto {

/*
 * Note: this code is harmless on little-endian machines.
*/
    static void ByteReverse(unsigned char *buf, unsigned longs) {
        unsigned int t;
        do {
            t = static_cast<unsigned int>(static_cast<unsigned>(buf[3]) << 8 | buf[2]) << 16 | (static_cast<unsigned>(buf[1]) << 8 | buf[0]);
            *reinterpret_cast<unsigned int *>(buf) = t;
            buf += 4;
        } while (--longs);
    }
/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) ((x) ^ (y) ^ (z))
#define F4(x, y, z) ((y) ^ ((x) | ~(z)))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) ((w) += f(x, y, z) + (data), (w) = (w) << (s) | (w) >> (32 - (s)), (w) += (x))

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
    static void MD5Transform(unsigned int buf[4], const unsigned int in[16]) {
        unsigned int a, b, c, d;

        a = buf[0];
        b = buf[1];
        c = buf[2];
        d = buf[3];

        MD5STEP(F1, a, b, c, d, in[0] + static_cast<unsigned int>(0xd76aa478), 7);
        MD5STEP(F1, d, a, b, c, in[1] + static_cast<unsigned int>(0xe8c7b756), 12);
        MD5STEP(F1, c, d, a, b, in[2] + static_cast<unsigned int>(0x242070db), 17);
        MD5STEP(F1, b, c, d, a, in[3] + static_cast<unsigned int>(0xc1bdceee), 22);
        MD5STEP(F1, a, b, c, d, in[4] + static_cast<unsigned int>(0xf57c0faf), 7);
        MD5STEP(F1, d, a, b, c, in[5] + static_cast<unsigned int>(0x4787c62a), 12);
        MD5STEP(F1, c, d, a, b, in[6] + static_cast<unsigned int>(0xa8304613), 17);
        MD5STEP(F1, b, c, d, a, in[7] + static_cast<unsigned int>(0xfd469501), 22);
        MD5STEP(F1, a, b, c, d, in[8] + static_cast<unsigned int>(0x698098d8), 7);
        MD5STEP(F1, d, a, b, c, in[9] + static_cast<unsigned int>(0x8b44f7af), 12);
        MD5STEP(F1, c, d, a, b, in[10] + static_cast<unsigned int>(0xffff5bb1), 17);
        MD5STEP(F1, b, c, d, a, in[11] + static_cast<unsigned int>(0x895cd7be), 22);
        MD5STEP(F1, a, b, c, d, in[12] + static_cast<unsigned int>(0x6b901122), 7);
        MD5STEP(F1, d, a, b, c, in[13] + static_cast<unsigned int>(0xfd987193), 12);
        MD5STEP(F1, c, d, a, b, in[14] + static_cast<unsigned int>(0xa679438e), 17);
        MD5STEP(F1, b, c, d, a, in[15] + static_cast<unsigned int>(0x49b40821), 22);

        MD5STEP(F2, a, b, c, d, in[1] + static_cast<unsigned int>(0xf61e2562), 5);
        MD5STEP(F2, d, a, b, c, in[6] + static_cast<unsigned int>(0xc040b340), 9);
        MD5STEP(F2, c, d, a, b, in[11] + static_cast<unsigned int>(0x265e5a51), 14);
        MD5STEP(F2, b, c, d, a, in[0] + static_cast<unsigned int>(0xe9b6c7aa), 20);
        MD5STEP(F2, a, b, c, d, in[5] + static_cast<unsigned int>(0xd62f105d), 5);
        MD5STEP(F2, d, a, b, c, in[10] + static_cast<unsigned int>(0x02441453), 9);
        MD5STEP(F2, c, d, a, b, in[15] + static_cast<unsigned int>(0xd8a1e681), 14);
        MD5STEP(F2, b, c, d, a, in[4] + static_cast<unsigned int>(0xe7d3fbc8), 20);
        MD5STEP(F2, a, b, c, d, in[9] + static_cast<unsigned int>(0x21e1cde6), 5);
        MD5STEP(F2, d, a, b, c, in[14] + static_cast<unsigned int>(0xc33707d6), 9);
        MD5STEP(F2, c, d, a, b, in[3] + static_cast<unsigned int>(0xf4d50d87), 14);
        MD5STEP(F2, b, c, d, a, in[8] + static_cast<unsigned int>(0x455a14ed), 20);
        MD5STEP(F2, a, b, c, d, in[13] + static_cast<unsigned int>(0xa9e3e905), 5);
        MD5STEP(F2, d, a, b, c, in[2] + static_cast<unsigned int>(0xfcefa3f8), 9);
        MD5STEP(F2, c, d, a, b, in[7] + static_cast<unsigned int>(0x676f02d9), 14);
        MD5STEP(F2, b, c, d, a, in[12] + static_cast<unsigned int>(0x8d2a4c8a), 20);

        MD5STEP(F3, a, b, c, d, in[5] + static_cast<unsigned int>(0xfffa3942), 4);
        MD5STEP(F3, d, a, b, c, in[8] + static_cast<unsigned int>(0x8771f681), 11);
        MD5STEP(F3, c, d, a, b, in[11] + static_cast<unsigned int>(0x6d9d6122), 16);
        MD5STEP(F3, b, c, d, a, in[14] + static_cast<unsigned int>(0xfde5380c), 23);
        MD5STEP(F3, a, b, c, d, in[1] + static_cast<unsigned int>(0xa4beea44), 4);
        MD5STEP(F3, d, a, b, c, in[4] + static_cast<unsigned int>(0x4bdecfa9), 11);
        MD5STEP(F3, c, d, a, b, in[7] + static_cast<unsigned int>(0xf6bb4b60), 16);
        MD5STEP(F3, b, c, d, a, in[10] + static_cast<unsigned int>(0xbebfbc70), 23);
        MD5STEP(F3, a, b, c, d, in[13] + static_cast<unsigned int>(0x289b7ec6), 4);
        MD5STEP(F3, d, a, b, c, in[0] + static_cast<unsigned int>(0xeaa127fa), 11);
        MD5STEP(F3, c, d, a, b, in[3] + static_cast<unsigned int>(0xd4ef3085), 16);
        MD5STEP(F3, b, c, d, a, in[6] + static_cast<unsigned int>(0x04881d05), 23);
        MD5STEP(F3, a, b, c, d, in[9] + static_cast<unsigned int>(0xd9d4d039), 4);
        MD5STEP(F3, d, a, b, c, in[12] + static_cast<unsigned int>(0xe6db99e5), 11);
        MD5STEP(F3, c, d, a, b, in[15] + static_cast<unsigned int>(0x1fa27cf8), 16);
        MD5STEP(F3, b, c, d, a, in[2] + static_cast<unsigned int>(0xc4ac5665), 23);

        MD5STEP(F4, a, b, c, d, in[0] + static_cast<unsigned int>(0xf4292244), 6);
        MD5STEP(F4, d, a, b, c, in[7] + static_cast<unsigned int>(0x432aff97), 10);
        MD5STEP(F4, c, d, a, b, in[14] + static_cast<unsigned int>(0xab9423a7), 15);
        MD5STEP(F4, b, c, d, a, in[5] + static_cast<unsigned int>(0xfc93a039), 21);
        MD5STEP(F4, a, b, c, d, in[12] + static_cast<unsigned int>(0x655b59c3), 6);
        MD5STEP(F4, d, a, b, c, in[3] + static_cast<unsigned int>(0x8f0ccc92), 10);
        MD5STEP(F4, c, d, a, b, in[10] + static_cast<unsigned int>(0xffeff47d), 15);
        MD5STEP(F4, b, c, d, a, in[1] + static_cast<unsigned int>(0x85845dd1), 21);
        MD5STEP(F4, a, b, c, d, in[8] + static_cast<unsigned int>(0x6fa87e4f), 6);
        MD5STEP(F4, d, a, b, c, in[15] + static_cast<unsigned int>(0xfe2ce6e0), 10);
        MD5STEP(F4, c, d, a, b, in[6] + static_cast<unsigned int>(0xa3014314), 15);
        MD5STEP(F4, b, c, d, a, in[13] + static_cast<unsigned int>(0x4e0811a1), 21);
        MD5STEP(F4, a, b, c, d, in[4] + static_cast<unsigned int>(0xf7537e82), 6);
        MD5STEP(F4, d, a, b, c, in[11] + static_cast<unsigned int>(0xbd3af235), 10);
        MD5STEP(F4, c, d, a, b, in[2] + static_cast<unsigned int>(0x2ad7d2bb), 15);
        MD5STEP(F4, b, c, d, a, in[9] + static_cast<unsigned int>(0xeb86d391), 21);

        buf[0] += a;
        buf[1] += b;
        buf[2] += c;
        buf[3] += d;
    }

/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */
    MD5Context::MD5Context() {
        buf[0] = 0x67452301;
        buf[1] = 0xefcdab89;
        buf[2] = 0x98badcfe;
        buf[3] = 0x10325476;
        bits[0] = 0;
        bits[1] = 0;
    }

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
    void MD5Context::MD5Update(const unsigned char* input, unsigned long long len) {
        unsigned int t;

        /* Update bitcount */

        t = bits[0];
        if ((bits[0] = t + (static_cast<unsigned int>(len) << 3)) < t) {
            bits[1]++; /* Carry from low to high */
        }
        bits[1] += len >> 29;

        t = (t >> 3) & 0x3f; /* Bytes already in shsInfo->data */

        /* Handle any leading odd-sized chunks */

        if (t) {
            unsigned char *p = static_cast<unsigned char *>(in) + t;

            t = 64 - t;
            if (len < t) {
                std::memcpy(p, input, len);
                return;
            }
            std::memcpy(p, input, t);
            ByteReverse(in, 16);
            MD5Transform(buf, reinterpret_cast<unsigned int *>(in));
            input += t;
            len -= t;
        }

        /* Process data in 64-byte chunks */

        while (len >= 64) {
            std::memcpy(in, input, 64);
            ByteReverse(in, 16);
            MD5Transform(buf, reinterpret_cast<unsigned int *>(in));
            input += 64;
            len -= 64;
        }

        /* Handle any remaining bytes of data. */
        std::memcpy(in, input, len);
    }

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
    void MD5Context::Finish(unsigned char* out_digest) {
        unsigned count;
        unsigned char *p;

        /* Compute number of bytes mod 64 */
        count = (bits[0] >> 3) & 0x3F;

        /* Set the first char of padding to 0x80.  This is safe since there is
           always at least one byte free */
        p = in + count;
        *p++ = 0x80;

        /* Bytes of padding needed to make 64 bytes */
        count = 64 - 1 - count;

        /* Pad out to 56 mod 64 */
        if (count < 8) {
            /* Two lots of padding:  Pad the first block to 64 bytes */
            std::memset(p, 0, count);
            ByteReverse(in, 16);
            MD5Transform(buf, (unsigned int *)in);

            /* Now fill the next block with 56 bytes */
            std::memset(in, 0, 56);
        } else {
            /* Pad block to 56 bytes */
            std::memset(p, 0, count - 8);
        }
        ByteReverse(in, 14);

        /* Append length in bits and transform */
        (reinterpret_cast<unsigned int *>(in))[14] = bits[0];
        (reinterpret_cast<unsigned int *>(in))[15] = bits[1];

        MD5Transform(buf, reinterpret_cast<unsigned int *>(in));
        ByteReverse(reinterpret_cast<unsigned char *>(buf), 4);
        std::memcpy(out_digest, buf, 16);
    }

    void MD5Context::DigestToBase16(const unsigned char* digest, char *zbuf) {
        static char const HEX_CODES[] = "0123456789abcdef";
        int i, j;

        for (j = i = 0; i < MD5_HASH_LENGTH_BINARY; i++) {
            int a = digest[i];
            zbuf[j++] = HEX_CODES[(a >> 4) & 0xf];
            zbuf[j++] = HEX_CODES[a & 0xf];
        }
    }

    std::string MD5Context::DigestToBase10(const unsigned char* digest) {
      __uint128_t val = 0;
      for (int i = 0; i < MD5_HASH_LENGTH_BINARY; i++) {
        val = static_cast<__uint128_t>(val << 4) | ((digest[i] >> 4) & 0xf);
        val = static_cast<__uint128_t>(val << 4) | (digest[i] & 0xf);
      }
      auto dec = folly::to<std::string>(static_cast<__uint128_t>(val));
      return dec;
    }

    int MD5Context::FinishHex(char *out_digest) {
      unsigned char digest[MD5_HASH_LENGTH_BINARY];
      Finish(digest);
      DigestToBase16(digest, out_digest);
      return 32;
    }

    int MD5Context::FinishDec(char *out_digest, bool needPadding) {
      unsigned char digest[MD5_HASH_LENGTH_BINARY];
      Finish(digest);
      // padding the string with 0 at beginning if the length is less than 32 and the needPadding == true
      auto dec = needPadding ? folly::sformat("{:0>*}", 32, DigestToBase10(digest)) : DigestToBase10(digest);
      int size = dec.size();
      std::memcpy(out_digest, dec.data(), size);
      return size;
    }

    std::string MD5Context::FinishHex() {
      char digest[MD5_HASH_LENGTH_TEXT];
      FinishHex(digest);
      return std::string(digest, MD5_HASH_LENGTH_TEXT);
    }

} // namespace facebook::velox::crypto
