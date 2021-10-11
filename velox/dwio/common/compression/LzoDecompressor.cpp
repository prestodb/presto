/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "folly/String.h"
#include "velox/dwio/common/exception/Exceptions.h"

#include <ios>
#include <sstream>
#include <string>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {
namespace compression {

static const int32_t DEC_32_TABLE[] = {4, 1, 2, 1, 4, 4, 4, 4};
static const int32_t DEC_64_TABLE[] = {0, 0, 0, -1, 0, 1, 2, 3};

static const int32_t SIZE_OF_SHORT = 2;
static const int32_t SIZE_OF_INT = 4;
static const int32_t SIZE_OF_LONG = 8;

static std::string toHex(uint64_t val) {
  std::ostringstream out;
  out << "0x" << std::hex << val;
  return out.str();
}

static std::string toString(int64_t val) {
  return folly::to<std::string>(val);
}

namespace {
class MalformedInputException : public common::ParseError {
 public:
  explicit MalformedInputException(int64_t off)
      : ParseError("MalformedInputException at " + toString(off)) {}

  MalformedInputException(int64_t off, const std::string& msg)
      : ParseError("MalformedInputException " + msg + " at " + toString(off)) {}

  MalformedInputException(const MalformedInputException& other)
      : ParseError(other.what()) {}
  MalformedInputException& operator=(const MalformedInputException&) = delete;

  ~MalformedInputException() noexcept override = default;
};
} // namespace

uint64_t lzoDecompress(
    const char* inputAddress,
    const char* inputLimit,
    char* outputAddress,
    char* outputLimit) {
  // nothing compresses to nothing
  if (inputAddress == inputLimit) {
    return 0;
  }

  // maximum offset in buffers to which it's safe to write long-at-a-time
  char* const fastOutputLimit = outputLimit - SIZE_OF_LONG;

  // LZO can concat two blocks together so, decode until the input data is
  // consumed
  const char* input = inputAddress;
  char* output = outputAddress;
  while (input < inputLimit) {
    //
    // Note: For safety some of the code below may stop decoding early or
    // skip decoding, because input is not available.  This makes the code
    // safe, and since LZO requires an explicit "stop" command, the decoder
    // will still throw a exception.
    //

    bool firstCommand = true;
    uint32_t lastLiteralLength = 0;
    while (true) {
      if (input >= inputLimit) {
        throw MalformedInputException(input - inputAddress);
      }
      uint32_t command = *(input++) & 0xFF;
      if (command == 0x11) {
        break;
      }

      // Commands are described using a bit pattern notation:
      // 0: bit is not set
      // 1: bit is set
      // L: part of literal length
      // P: part of match offset position
      // M: part of match length
      // ?: see documentation in command decoder

      int32_t matchLength;
      int32_t matchOffset;
      uint32_t literalLength;
      if ((command & 0xf0) == 0) {
        if (lastLiteralLength == 0) {
          // 0b0000_LLLL (0bLLLL_LLLL)*

          // copy length :: fixed
          //   0
          matchOffset = 0;

          // copy offset :: fixed
          //   0
          matchLength = 0;

          // literal length - 3 :: variable bits :: valid range [4..]
          //   3 + variableLength(command bits [0..3], 4)
          literalLength = command & 0xf;
          if (literalLength == 0) {
            literalLength = 0xf;

            uint32_t nextByte = 0;
            while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
              literalLength += 0xff;
            }
            literalLength += nextByte;
          }
          literalLength += 3;
        } else if (lastLiteralLength <= 3) {
          // 0b0000_PPLL 0bPPPP_PPPP

          // copy length: fixed
          //   3
          matchLength = 3;

          // copy offset :: 12 bits :: valid range [2048..3071]
          //   [0..1] from command [2..3]
          //   [2..9] from trailer [0..7]
          //   [10] unset
          //   [11] set
          if (input >= inputLimit) {
            throw MalformedInputException(input - inputAddress);
          }
          matchOffset = (command & 0xc) >> 2;
          matchOffset |= (*(input++) & 0xFF) << 2;
          matchOffset |= 0x800;

          // literal length :: 2 bits :: valid range [0..3]
          //   [0..1] from command [0..1]
          literalLength = (command & 0x3);
        } else {
          // 0b0000_PPLL 0bPPPP_PPPP

          // copy length :: fixed
          //   2
          matchLength = 2;

          // copy offset :: 10 bits :: valid range [0..1023]
          //   [0..1] from command [2..3]
          //   [2..9] from trailer [0..7]
          if (input >= inputLimit) {
            throw MalformedInputException(input - inputAddress);
          }
          matchOffset = (command & 0xc) >> 2;
          matchOffset |= (*(input++) & 0xFF) << 2;

          // literal length :: 2 bits :: valid range [0..3]
          //   [0..1] from command [0..1]
          literalLength = (command & 0x3);
        }
      } else if (firstCommand) {
        // first command has special handling when high nibble is set
        matchLength = 0;
        matchOffset = 0;
        literalLength = command - 17;
      } else if ((command & 0xf0) == 0x10) {
        // 0b0001_?MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

        // copy length - 2 :: variable bits :: valid range [3..]
        //   2 + variableLength(command bits [0..2], 3)
        matchLength = command & 0x7;
        if (matchLength == 0) {
          matchLength = 0x7;

          int32_t nextByte = 0;
          while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
            matchLength += 0xff;
          }
          matchLength += nextByte;
        }
        matchLength += 2;

        // read trailer
        if (input + SIZE_OF_SHORT > inputLimit) {
          throw MalformedInputException(input - inputAddress);
        }
        uint32_t trailer = *reinterpret_cast<const uint16_t*>(input) & 0xFFFF;
        input += SIZE_OF_SHORT;

        // copy offset :: 16 bits :: valid range [32767..49151]
        //   [0..13] from trailer [2..15]
        //   [14] if command bit [3] unset
        //   [15] if command bit [3] set
        matchOffset = trailer >> 2;
        if ((command & 0x8) == 0) {
          matchOffset |= 0x4000;
        } else {
          matchOffset |= 0x8000;
        }
        matchOffset--;

        // literal length :: 2 bits :: valid range [0..3]
        //   [0..1] from trailer [0..1]
        literalLength = trailer & 0x3;
      } else if ((command & 0xe0) == 0x20) {
        // 0b001M_MMMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

        // copy length - 2 :: variable bits :: valid range [3..]
        //   2 + variableLength(command bits [0..4], 5)
        matchLength = command & 0x1f;
        if (matchLength == 0) {
          matchLength = 0x1f;

          int nextByte = 0;
          while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
            matchLength += 0xff;
          }
          matchLength += nextByte;
        }
        matchLength += 2;

        // read trailer
        if (input + SIZE_OF_SHORT > inputLimit) {
          throw MalformedInputException(input - inputAddress);
        }
        int32_t trailer = *reinterpret_cast<const int16_t*>(input) & 0xFFFF;
        input += SIZE_OF_SHORT;

        // copy offset :: 14 bits :: valid range [0..16383]
        //  [0..13] from trailer [2..15]
        matchOffset = trailer >> 2;

        // literal length :: 2 bits :: valid range [0..3]
        //   [0..1] from trailer [0..1]
        literalLength = trailer & 0x3;
      } else if ((command & 0xc0) != 0) {
        // 0bMMMP_PPLL 0bPPPP_PPPP

        // copy length - 1 :: 3 bits :: valid range [1..8]
        //   [0..2] from command [5..7]
        //   add 1
        matchLength = (command & 0xe0) >> 5;
        matchLength += 1;

        // copy offset :: 11 bits :: valid range [0..4095]
        //   [0..2] from command [2..4]
        //   [3..10] from trailer [0..7]
        if (input >= inputLimit) {
          throw MalformedInputException(input - inputAddress);
        }
        matchOffset = (command & 0x1c) >> 2;
        matchOffset |= (*(input++) & 0xFF) << 3;

        // literal length :: 2 bits :: valid range [0..3]
        //   [0..1] from command [0..1]
        literalLength = (command & 0x3);
      } else {
        throw MalformedInputException(
            input - inputAddress - 1, "Invalid LZO command " + toHex(command));
      }
      firstCommand = false;

      // copy match
      if (matchLength != 0) {
        // lzo encodes match offset minus one
        matchOffset++;

        char* matchAddress = output - matchOffset;
        if (matchAddress < outputAddress ||
            output + matchLength > outputLimit) {
          throw MalformedInputException(input - inputAddress);
        }
        char* matchOutputLimit = output + matchLength;

        if (output > fastOutputLimit) {
          // slow match copy
          while (output < matchOutputLimit) {
            *(output++) = *(matchAddress++);
          }
        } else {
          // copy repeated sequence
          if (matchOffset < SIZE_OF_LONG) {
            // 8 bytes apart so that we can copy long-at-a-time below
            int32_t increment32 = DEC_32_TABLE[matchOffset];
            int32_t decrement64 = DEC_64_TABLE[matchOffset];

            output[0] = *matchAddress;
            output[1] = *(matchAddress + 1);
            output[2] = *(matchAddress + 2);
            output[3] = *(matchAddress + 3);
            output += SIZE_OF_INT;
            matchAddress += increment32;

            *reinterpret_cast<int32_t*>(output) =
                *reinterpret_cast<int32_t*>(matchAddress);
            output += SIZE_OF_INT;
            matchAddress -= decrement64;
          } else {
            *reinterpret_cast<int64_t*>(output) =
                *reinterpret_cast<int64_t*>(matchAddress);
            matchAddress += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
          }

          if (matchOutputLimit >= fastOutputLimit) {
            if (matchOutputLimit > outputLimit) {
              throw MalformedInputException(input - inputAddress);
            }

            while (output < fastOutputLimit) {
              *reinterpret_cast<int64_t*>(output) =
                  *reinterpret_cast<int64_t*>(matchAddress);
              matchAddress += SIZE_OF_LONG;
              output += SIZE_OF_LONG;
            }

            while (output < matchOutputLimit) {
              *(output++) = *(matchAddress++);
            }
          } else {
            while (output < matchOutputLimit) {
              *reinterpret_cast<int64_t*>(output) =
                  *reinterpret_cast<int64_t*>(matchAddress);
              matchAddress += SIZE_OF_LONG;
              output += SIZE_OF_LONG;
            }
          }
        }
        output = matchOutputLimit; // correction in case we over-copied
      }

      // copy literal
      char* literalOutputLimit = output + literalLength;
      if (literalOutputLimit > fastOutputLimit ||
          input + literalLength > inputLimit - SIZE_OF_LONG) {
        if (literalOutputLimit > outputLimit) {
          throw MalformedInputException(input - inputAddress);
        }

        // slow, precise copy
        memcpy(output, input, literalLength);
        input += literalLength;
        output += literalLength;
      } else {
        // fast copy. We may over-copy but there's enough room in input
        // and output to not overrun them
        do {
          *reinterpret_cast<int64_t*>(output) =
              *reinterpret_cast<const int64_t*>(input);
          input += SIZE_OF_LONG;
          output += SIZE_OF_LONG;
        } while (output < literalOutputLimit);
        // adjust index if we over-copied
        input -= (output - literalOutputLimit);
        output = literalOutputLimit;
      }
      lastLiteralLength = literalLength;
    }

    if (input + SIZE_OF_SHORT > inputLimit &&
        *reinterpret_cast<const int16_t*>(input) != 0) {
      throw MalformedInputException(input - inputAddress);
    }
    input += SIZE_OF_SHORT;
  }

  return static_cast<uint64_t>(output - outputAddress);
}

} // namespace compression
} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
