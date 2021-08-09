/*
 *  This code is taken from duckdb/src/include/duckdb/common/crypto/md5.hpp
*   with following changes:
 *  1. removed other duckdb dependencies.
 *  2. Add the DigestToBase10 method which produce the md5 in decimal
 */
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/crypto/md5.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstring>

namespace facebook::velox::crypto {

    class MD5Context {
    public:
        static constexpr unsigned long long MD5_HASH_LENGTH_BINARY = 16;
        static constexpr unsigned long long MD5_HASH_LENGTH_TEXT = 32;

    public:
        MD5Context();

        void Add(const unsigned char* data, unsigned long long len) {
            MD5Update(data, len);
        }
        void Add(const std::string &data) {
            MD5Update((const unsigned char*)data.c_str(), data.size());
        }

        //! Write the 16-byte (binary) digest to the specified location
        void Finish(unsigned char* out_digest);
        //! Write the 32-character digest (in hexadecimal format) to the specified location
        int FinishHex(char *out_digest);
        //! Write the digest (in decimal format) to the specified location.
        // padding the string with 0 at beginning if the length is less than 32 and the needPadding == true
        int FinishDec(char *out_digest, bool needPadding = false);
        //! Returns the 32-character digest (in hexadecimal format) as a string
        std::string FinishHex();

    private:
        void MD5Update(const unsigned char* data, unsigned long long len);
        static void DigestToBase16(const unsigned char* digest, char *zBuf);
        static std::string DigestToBase10(const unsigned char* digest);

        unsigned int buf[4];
        unsigned int bits[2];
        unsigned char in[64];
    };

} // namespace facebook::velox::crypto
