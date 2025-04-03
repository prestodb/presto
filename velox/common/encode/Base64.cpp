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
#include "velox/common/encode/Base64.h"

#include <folly/Portability.h>
#include <folly/container/Foreach.h>
#include <folly/io/Cursor.h>
#include <cstdint>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::encoding {

// Constants defining the size in bytes of binary and encoded blocks for Base64
// encoding.
// Size of a binary block in bytes (3 bytes = 24 bits)
constexpr static int kBinaryBlockByteSize = 3;
// Size of an encoded block in bytes (4 bytes = 24 bits)
constexpr static int kEncodedBlockByteSize = 4;

// Character sets for Base64 and Base64 URL encoding
constexpr const Base64::Charset kBase64Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

constexpr const Base64::Charset kBase64UrlCharset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

// Reverse lookup tables for decoding
constexpr const Base64::ReverseIndex kBase64ReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    255, 255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

constexpr const Base64::ReverseIndex kBase64UrlReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    62,  255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

// Validate the character in charset with ReverseIndex table
constexpr bool checkForwardIndex(
    uint8_t index,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& reverseIndex) {
  return (reverseIndex[static_cast<uint8_t>(charset[index])] == index) &&
      (index > 0 ? checkForwardIndex(index - 1, charset, reverseIndex) : true);
}

// Verify that for every entry in kBase64Charset, the corresponding entry
// in kBase64ReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64Charset) - 1,
        kBase64Charset,
        kBase64ReverseIndexTable),
    "kBase64Charset has incorrect entries");

// Verify that for every entry in kBase64UrlCharset, the corresponding entry
// in kBase64UrlReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64UrlCharset) - 1,
        kBase64UrlCharset,
        kBase64UrlReverseIndexTable),
    "kBase64UrlCharset has incorrect entries");

// Searches for a character within a charset up to a certain index.
constexpr bool findCharacterInCharset(
    const Base64::Charset& charset,
    uint8_t index,
    const char targetChar) {
  return index < charset.size() &&
      ((charset[index] == targetChar) ||
       findCharacterInCharset(charset, index + 1, targetChar));
}

// Checks the consistency of a reverse index mapping for a given character
// set.
constexpr bool checkReverseIndex(
    uint8_t index,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& reverseIndex) {
  return (reverseIndex[index] == 255
              ? !findCharacterInCharset(charset, 0, static_cast<char>(index))
              : (charset[reverseIndex[index]] == index)) &&
      (index > 0 ? checkReverseIndex(index - 1, charset, reverseIndex) : true);
}

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
static_assert(
    checkReverseIndex(
        sizeof(kBase64ReverseIndexTable) - 1,
        kBase64Charset,
        kBase64ReverseIndexTable),
    "kBase64ReverseIndexTable has incorrect entries.");

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
// We can't run this check as the URL version has two duplicate entries so
// that the url decoder can handle url encodings and default encodings
// static_assert(
//     checkReverseIndex(
//         sizeof(kBase64UrlReverseIndexTable) - 1,
//         kBase64UrlCharset,
//         kBase64UrlReverseIndexTable),
//     "kBase64UrlReverseIndexTable has incorrect entries.");

// Implementation of Base64 encoding and decoding functions.
// static
template <class T>
std::string Base64::encodeImpl(
    const T& input,
    const Charset& charset,
    bool includePadding) {
  size_t encodedSize = calculateEncodedSize(input.size(), includePadding);
  std::string encodedResult;
  encodedResult.resize(encodedSize);
  encodeImpl(input, charset, includePadding, encodedResult.data());
  return encodedResult;
}

// static
size_t Base64::calculateEncodedSize(size_t inputSize, bool withPadding) {
  if (inputSize == 0) {
    return 0;
  }

  // Calculate the output size assuming that we are including padding.
  size_t encodedSize = ((inputSize + 2) / 3) * 4;
  if (!withPadding) {
    // If the padding was not requested, subtract the padding bytes.
    encodedSize -= (3 - (inputSize % 3)) % 3;
  }
  return encodedSize;
}

// static
void Base64::encode(const char* input, size_t inputSize, char* output) {
  encodeImpl(
      folly::StringPiece(input, inputSize), kBase64Charset, true, output);
}

// static
void Base64::encodeUrl(const char* input, size_t inputSize, char* output) {
  encodeImpl(
      folly::StringPiece(input, inputSize), kBase64UrlCharset, true, output);
}

// static
template <class T>
void Base64::encodeImpl(
    const T& input,
    const Charset& charset,
    bool includePadding,
    char* outputBuffer) {
  auto inputSize = input.size();
  if (inputSize == 0) {
    return;
  }

  auto outputPointer = outputBuffer;
  auto inputIterator = input.begin();

  // For each group of 3 bytes (24 bits) in the input, split that into
  // 4 groups of 6 bits and encode that using the supplied charset lookup
  for (; inputSize > 2; inputSize -= 3) {
    uint32_t inputBlock = static_cast<uint8_t>(*inputIterator++) << 16;
    inputBlock |= static_cast<uint8_t>(*inputIterator++) << 8;
    inputBlock |= static_cast<uint8_t>(*inputIterator++);

    *outputPointer++ = charset[(inputBlock >> 18) & 0x3f];
    *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
    *outputPointer++ = charset[(inputBlock >> 6) & 0x3f];
    *outputPointer++ = charset[inputBlock & 0x3f];
  }

  if (inputSize > 0) {
    // We have either 1 or 2 input bytes left.  Encode this similar to the
    // above (assuming 0 for all other bytes).  Optionally append the '='
    // character if it is requested.
    uint32_t inputBlock = static_cast<uint8_t>(*inputIterator++) << 16;
    *outputPointer++ = charset[(inputBlock >> 18) & 0x3f];
    if (inputSize > 1) {
      inputBlock |= static_cast<uint8_t>(*inputIterator) << 8;
      *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
      *outputPointer++ = charset[(inputBlock >> 6) & 0x3f];
      if (includePadding) {
        *outputPointer = kPadding;
      }
    } else {
      *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
      if (includePadding) {
        *outputPointer++ = kPadding;
        *outputPointer = kPadding;
      }
    }
  }
}

// static
std::string Base64::encode(folly::StringPiece text) {
  return encodeImpl(text, kBase64Charset, true);
}

// static
std::string Base64::encode(const char* input, size_t inputSize) {
  return encode(folly::StringPiece(input, inputSize));
}

namespace {

// This is a quick and simple iterator implementation for an IOBuf so that the
// template that uses iterators can work on IOBuf chains. It only implements
// postfix increment because that is all the algorithm needs, and it is a no-op
// since the read<>() function already increments the cursor.
class IOBufWrapper {
 private:
  class Iterator {
   public:
    explicit Iterator(const folly::IOBuf* inputBuffer) : cursor_(inputBuffer) {}

    Iterator& operator++(int32_t) {
      // This is a no-op since reading from the Cursor has already moved the
      // position.
      return *this;
    }

    uint8_t operator*() {
      // This will read _and_ increment the cursor.
      return cursor_.read<uint8_t>();
    }

   private:
    folly::io::Cursor cursor_;
  };

 public:
  explicit IOBufWrapper(const folly::IOBuf* inputBuffer)
      : input_(inputBuffer) {}
  size_t size() const {
    return input_->computeChainDataLength();
  }

  Iterator begin() const {
    return Iterator(input_);
  }

 private:
  const folly::IOBuf* input_;
};

} // namespace

// static
std::string Base64::encode(const folly::IOBuf* inputBuffer) {
  return encodeImpl(IOBufWrapper(inputBuffer), kBase64Charset, true);
}

// static
std::string Base64::decode(folly::StringPiece encodedText) {
  std::string decodedResult;
  decode(std::make_pair(encodedText.data(), encodedText.size()), decodedResult);
  return decodedResult;
}

// static
void Base64::decode(
    const std::pair<const char*, int32_t>& payload,
    std::string& decodedOutput) {
  size_t inputSize = payload.second;
  auto decodedSize = calculateDecodedSize(payload.first, inputSize);
  if (decodedSize.hasError()) {
    VELOX_USER_FAIL(decodedSize.error().message());
  }
  decodedOutput.resize(decodedSize.value());
  auto status = decode(
      payload.first, inputSize, decodedOutput.data(), decodedOutput.size());
  if (!status.ok()) {
    VELOX_USER_FAIL(status.message());
  }
}

// static
void Base64::decode(const char* input, size_t inputSize, char* outputBuffer) {
  size_t outputSize = inputSize / 4 * 3;
  auto status = decode(input, inputSize, outputBuffer, outputSize);
  if (!status.ok()) {
    VELOX_USER_FAIL(status.message());
  }
}

// static
Expected<uint8_t> Base64::base64ReverseLookup(
    char encodedChar,
    const ReverseIndex& reverseIndex) {
  auto reverseLookupValue = reverseIndex[static_cast<uint8_t>(encodedChar)];
  if (reverseLookupValue >= 0x40) {
    return folly::makeUnexpected(Status::UserError(
        "decode() - invalid input string: invalid character '{}'",
        encodedChar));
  }
  return reverseLookupValue;
}

// static
Status Base64::decode(
    const char* input,
    size_t inputSize,
    char* output,
    size_t outputSize) {
  auto decodedSize = decodeImpl(
      input, inputSize, output, outputSize, kBase64ReverseIndexTable);
  if (decodedSize.hasError()) {
    return decodedSize.error();
  }
  return Status::OK();
}

// static
Expected<size_t> Base64::calculateDecodedSize(
    const char* input,
    size_t& inputSize) {
  if (inputSize == 0) {
    return 0;
  }

  // Check if the input string is padded
  if (isPadded(input, inputSize)) {
    // If padded, ensure that the string length is a multiple of the encoded
    // block size
    if (inputSize % kEncodedBlockByteSize != 0) {
      return folly::makeUnexpected(
          Status::UserError("Base64::decode() - invalid input string: "
                            "string length is not a multiple of 4."));
    }

    auto decodedSize =
        (inputSize * kBinaryBlockByteSize) / kEncodedBlockByteSize;
    auto paddingCount = numPadding(input, inputSize);
    inputSize -= paddingCount;

    // Adjust the needed size by deducting the bytes corresponding to the
    // padding from the calculated size.
    return decodedSize -
        ((paddingCount * kBinaryBlockByteSize) + (kEncodedBlockByteSize - 1)) /
        kEncodedBlockByteSize;
  }
  // If not padded, Calculate extra bytes, if any
  auto extraBytes = inputSize % kEncodedBlockByteSize;
  auto decodedSize = (inputSize / kEncodedBlockByteSize) * kBinaryBlockByteSize;

  // Adjust the needed size for extra bytes, if present
  if (extraBytes) {
    if (extraBytes == 1) {
      return folly::makeUnexpected(Status::UserError(
          "Base64::decode() - invalid input string: "
          "string length cannot be 1 more than a multiple of 4."));
    }
    decodedSize += (extraBytes * kBinaryBlockByteSize) / kEncodedBlockByteSize;
  }

  return decodedSize;
}

// static
Expected<size_t> Base64::decodeImpl(
    const char* input,
    size_t inputSize,
    char* outputBuffer,
    size_t outputSize,
    const ReverseIndex& reverseIndex) {
  if (inputSize == 0) {
    return 0;
  }

  auto decodedSize = calculateDecodedSize(input, inputSize);
  if (decodedSize.hasError()) {
    return folly::makeUnexpected(decodedSize.error());
  }

  if (outputSize < decodedSize.value()) {
    return folly::makeUnexpected(
        Status::UserError("Base64::decode() - invalid output string: "
                          "output string is too small."));
  }
  outputSize = decodedSize.value();

  // Handle full groups of 4 characters
  for (; inputSize > 4; inputSize -= 4, input += 4, outputBuffer += 3) {
    // Each character of the 4 encodes 6 bits of the original, grab each with
    // the appropriate shifts to rebuild the original and then split that back
    // into the original 8-bit bytes.
    uint32_t decodedBlock = 0;
    for (int i = 0; i < 4; ++i) {
      auto reverseLookupValue = base64ReverseLookup(input[i], reverseIndex);
      if (reverseLookupValue.hasError()) {
        return folly::makeUnexpected(reverseLookupValue.error());
      }
      decodedBlock |= reverseLookupValue.value() << (18 - 6 * i);
    }
    outputBuffer[0] = static_cast<char>((decodedBlock >> 16) & 0xff);
    outputBuffer[1] = static_cast<char>((decodedBlock >> 8) & 0xff);
    outputBuffer[2] = static_cast<char>(decodedBlock & 0xff);
  }

  // Handle the last 2-4 characters. This is similar to the above, but the
  // last 2 characters may or may not exist.
  if (inputSize >= 2) {
    uint32_t decodedBlock = 0;

    // Process the first two characters
    for (int i = 0; i < 2; ++i) {
      auto reverseLookupValue = base64ReverseLookup(input[i], reverseIndex);
      if (reverseLookupValue.hasError()) {
        return folly::makeUnexpected(reverseLookupValue.error());
      }
      decodedBlock |= reverseLookupValue.value() << (18 - 6 * i);
    }
    outputBuffer[0] = static_cast<char>((decodedBlock >> 16) & 0xff);

    if (inputSize > 2) {
      auto reverseLookupValue = base64ReverseLookup(input[2], reverseIndex);
      if (reverseLookupValue.hasError()) {
        return folly::makeUnexpected(reverseLookupValue.error());
      }
      decodedBlock |= reverseLookupValue.value() << 6;
      outputBuffer[1] = static_cast<char>((decodedBlock >> 8) & 0xff);

      if (inputSize > 3) {
        auto reverseLookupValue = base64ReverseLookup(input[3], reverseIndex);
        if (reverseLookupValue.hasError()) {
          return folly::makeUnexpected(reverseLookupValue.error());
        }
        decodedBlock |= reverseLookupValue.value();
        outputBuffer[2] = static_cast<char>(decodedBlock & 0xff);
      }
    }
  }

  return decodedSize.value();
}

// static
std::string Base64::encodeUrl(folly::StringPiece text) {
  return encodeImpl(text, kBase64UrlCharset, false);
}

// static
std::string Base64::encodeUrl(const char* input, size_t inputSize) {
  return encodeUrl(folly::StringPiece(input, inputSize));
}

// static
std::string Base64::encodeUrl(const folly::IOBuf* inputBuffer) {
  return encodeImpl(IOBufWrapper(inputBuffer), kBase64UrlCharset, false);
}

// static
Status Base64::decodeUrl(
    const char* input,
    size_t inputSize,
    char* outputBuffer,
    size_t outputSize) {
  auto decodedSize = decodeImpl(
      input, inputSize, outputBuffer, outputSize, kBase64UrlReverseIndexTable);
  if (decodedSize.hasError()) {
    return decodedSize.error();
  }
  return Status::OK();
}

// static
std::string Base64::decodeUrl(folly::StringPiece encodedText) {
  std::string decodedOutput;
  decodeUrl(
      std::make_pair(encodedText.data(), encodedText.size()), decodedOutput);
  return decodedOutput;
}

// static
void Base64::decodeUrl(
    const std::pair<const char*, int32_t>& payload,
    std::string& decodedOutput) {
  size_t expectedDecodedSize = (payload.second + 3) / 4 * 3;
  decodedOutput.resize(expectedDecodedSize, '\0');
  auto decodedSize = decodeImpl(
      payload.first,
      payload.second,
      decodedOutput.data(),
      expectedDecodedSize,
      kBase64UrlReverseIndexTable);
  if (decodedSize.hasError()) {
    VELOX_USER_FAIL(decodedSize.error().message());
  }
  decodedOutput.resize(decodedSize.value());
}

} // namespace facebook::velox::encoding
