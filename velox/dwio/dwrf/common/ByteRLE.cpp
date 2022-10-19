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
#include "velox/dwio/dwrf/common/ByteRLE.h"

#include <algorithm>

#include "velox/dwio/common/exception/Exceptions.h"

namespace facebook::velox::dwrf {

class ByteRleEncoderImpl : public ByteRleEncoder {
 public:
  explicit ByteRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : outputStream{std::move(output)},
        numLiterals{0},
        repeat{false},
        tailRunLength{0},
        bufferPosition{0},
        bufferLength{0},
        buffer{nullptr} {}

  uint64_t add(
      const char* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override;

  uint64_t add(
      const std::function<char(vector_size_t)>& valueAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& isNullAt) override;

  uint64_t addBits(
      const uint64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls,
      bool invert) override {
    throw std::runtime_error("addBits is only for bool stream");
  }

  uint64_t addBits(
      const std::function<bool(vector_size_t)>& isNullAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& valueAt,
      bool invert) override {
    throw std::runtime_error("addBits is only for bool stream");
  }

  uint64_t getBufferSize() const override {
    return outputStream->size();
  }

  uint64_t flush() override;

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override;

 protected:
  std::unique_ptr<BufferedOutputStream> outputStream;
  std::array<char, RLE_MAX_LITERAL_SIZE> literals;
  int32_t numLiterals;
  bool repeat;
  int32_t tailRunLength;
  int32_t bufferPosition;
  int32_t bufferLength;
  char* buffer;

  void writeByte(char c);
  void writeValues();
  void write(char c);
};

void ByteRleEncoderImpl::writeByte(char c) {
  if (UNLIKELY(bufferPosition == bufferLength)) {
    int32_t addedSize = 0;
    DWIO_ENSURE(
        outputStream->Next(reinterpret_cast<void**>(&buffer), &addedSize),
        "Allocation failure");
    bufferPosition = 0;
    bufferLength = addedSize;
  }
  buffer[bufferPosition++] = c;
}

uint64_t ByteRleEncoderImpl::add(
    const char* data,
    const common::Ranges& ranges,
    const uint64_t* nulls) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        write(data[pos]);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      write(data[pos]);
      ++count;
    }
  }
  return count;
}

uint64_t ByteRleEncoderImpl::add(
    const std::function<char(vector_size_t)>& valueAt,
    const common::Ranges& ranges,
    const std::function<bool(vector_size_t)>& isNullAt) {
  uint64_t count = 0;
  if (isNullAt) {
    for (auto& pos : ranges) {
      if (!isNullAt(pos)) {
        write(valueAt(pos));
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      write(valueAt(pos));
      ++count;
    }
  }
  return count;
}

void ByteRleEncoderImpl::writeValues() {
  if (numLiterals != 0) {
    if (repeat) {
      writeByte(static_cast<char>(numLiterals - RLE_MINIMUM_REPEAT));
      writeByte(literals[0]);
    } else {
      writeByte(static_cast<char>(-numLiterals));
      for (int32_t i = 0; i < numLiterals; ++i) {
        writeByte(literals[i]);
      }
    }
    repeat = false;
    tailRunLength = 0;
    numLiterals = 0;
  }
}

uint64_t ByteRleEncoderImpl::flush() {
  writeValues();
  outputStream->BackUp(bufferLength - bufferPosition);
  uint64_t dataSize = outputStream->flush();
  bufferLength = bufferPosition = 0;
  return dataSize;
}

void ByteRleEncoderImpl::write(char value) {
  if (numLiterals == 0) {
    literals[numLiterals++] = value;
    tailRunLength = 1;
  } else if (repeat) {
    if (value == literals[0]) {
      if (++numLiterals == RLE_MAXIMUM_REPEAT) {
        writeValues();
      }
    } else {
      writeValues();
      literals[numLiterals++] = value;
      tailRunLength = 1;
    }
  } else {
    if (value == literals[numLiterals - 1]) {
      tailRunLength += 1;
    } else {
      tailRunLength = 1;
    }
    if (tailRunLength == RLE_MINIMUM_REPEAT) {
      if (numLiterals + 1 > RLE_MINIMUM_REPEAT) {
        numLiterals -= (RLE_MINIMUM_REPEAT - 1);
        writeValues();
        literals[0] = value;
      }
      repeat = true;
      numLiterals = RLE_MINIMUM_REPEAT;
    } else {
      literals[numLiterals++] = value;
      if (numLiterals == RLE_MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }
}

void ByteRleEncoderImpl::recordPosition(
    PositionRecorder& recorder,
    int32_t strideIndex) const {
  outputStream->recordPosition(
      recorder, bufferLength, bufferPosition, strideIndex);
  recorder.add(static_cast<uint64_t>(numLiterals), strideIndex);
}

std::unique_ptr<ByteRleEncoder> createByteRleEncoder(
    std::unique_ptr<BufferedOutputStream> output) {
  return std::make_unique<ByteRleEncoderImpl>(std::move(output));
}

class BooleanRleEncoderImpl : public ByteRleEncoderImpl {
 public:
  explicit BooleanRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : ByteRleEncoderImpl{std::move(output)}, bitsRemained{8}, current{0} {}

  uint64_t add(
      const char* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override;

  uint64_t addBits(
      const uint64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls,
      bool invert) override;

  uint64_t addBits(
      const std::function<bool(vector_size_t)>& isNullAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& valueAt,
      bool invert) override;

  uint64_t flush() override {
    if (bitsRemained != 8) {
      writeByte();
    }
    return ByteRleEncoderImpl::flush();
  }

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override {
    ByteRleEncoderImpl::recordPosition(recorder, strideIndex);
    recorder.add(static_cast<uint64_t>(8 - bitsRemained), strideIndex);
  }

 private:
  int32_t bitsRemained;
  char current;

  void writeByte() {
    write(current);
    bitsRemained = 8;
    current = 0;
  }

  void writeBool(bool val) {
    --bitsRemained;
    current |= ((val ? 1 : 0) << bitsRemained);
    if (bitsRemained == 0) {
      writeByte();
    }
  }
};

uint64_t BooleanRleEncoderImpl::add(
    const char* data,
    const common::Ranges& ranges,
    const uint64_t* nulls) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        writeBool(!data || data[pos]);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      writeBool(!data || data[pos]);
      ++count;
    }
  }
  return count;
}

uint64_t BooleanRleEncoderImpl::addBits(
    const uint64_t* data,
    const common::Ranges& ranges,
    const uint64_t* nulls,
    bool invert) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        bool val = (!data || invert != bits::isBitSet(data, pos));
        writeBool(val);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      bool val = (!data || invert != bits::isBitSet(data, pos));
      writeBool(val);
      ++count;
    }
  }
  return count;
}

uint64_t BooleanRleEncoderImpl::addBits(
    const std::function<bool(vector_size_t)>& valueAt,
    const common::Ranges& ranges,
    const std::function<bool(vector_size_t)>& isNullAt,
    bool invert) {
  uint64_t count = 0;
  if (isNullAt) {
    for (auto& pos : ranges) {
      if (!isNullAt(pos)) {
        bool val = (!valueAt || invert != valueAt(pos));
        writeBool(val);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      bool val = (!valueAt || invert != valueAt(pos));
      writeBool(val);
      ++count;
    }
  }
  return count;
}

std::unique_ptr<ByteRleEncoder> createBooleanRleEncoder(
    std::unique_ptr<BufferedOutputStream> output) {
  return std::make_unique<BooleanRleEncoderImpl>(std::move(output));
}

void ByteRleDecoder::nextBuffer() {
  int32_t bufferLength;
  const void* bufferPointer;
  DWIO_ENSURE(
      inputStream->Next(&bufferPointer, &bufferLength),
      "bad read in nextBuffer ",
      encodingKey_.toString(),
      ", ",
      inputStream->getName());
  bufferStart = static_cast<const char*>(bufferPointer);
  bufferEnd = bufferStart + bufferLength;
}

void ByteRleDecoder::seekToRowGroup(
    dwio::common::PositionProvider& positionProvider) {
  // move the input stream
  inputStream->seekToPosition(positionProvider);
  // force a re-read from the stream
  bufferEnd = bufferStart;
  // force reading a new header
  remainingValues = 0;
  // skip ahead the given number of records
  ByteRleDecoder::skip(positionProvider.next());
}

void ByteRleDecoder::skipBytes(size_t count) {
  size_t consumedBytes = count;
  while (consumedBytes > 0) {
    if (bufferStart == bufferEnd) {
      nextBuffer();
    }
    size_t skipSize = std::min(
        static_cast<size_t>(consumedBytes),
        static_cast<size_t>(bufferEnd - bufferStart));
    bufferStart += skipSize;
    consumedBytes -= skipSize;
  }
}

void ByteRleDecoder::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (remainingValues == 0) {
      readHeader();
    }
    size_t count = std::min(static_cast<size_t>(numValues), remainingValues);
    remainingValues -= count;
    numValues -= count;
    // for literals we need to skip over count bytes, which may involve
    // reading from the underlying stream
    if (!repeating) {
      skipBytes(count);
    }
  }
}

void ByteRleDecoder::next(
    char* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  uint64_t position = 0;
  // skip over null values
  while (nulls && position < numValues && bits::isBitNull(nulls, position)) {
    position += 1;
  }
  while (position < numValues) {
    // if we are out of values, read more
    if (remainingValues == 0) {
      readHeader();
    }
    // how many do we read out of this block?
    size_t count =
        std::min(static_cast<size_t>(numValues - position), remainingValues);
    uint64_t consumed = 0;
    if (repeating) {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] = value;
            consumed += 1;
          }
        }
      } else {
        memset(data + position, value, count);
        consumed = count;
      }
    } else {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] = readByte();
            consumed += 1;
          }
        }
      } else {
        uint64_t i = 0;
        while (i < count) {
          if (bufferStart == bufferEnd) {
            nextBuffer();
          }
          uint64_t copyBytes = std::min(
              static_cast<uint64_t>(count - i),
              static_cast<uint64_t>(bufferEnd - bufferStart));
          std::copy(bufferStart, bufferStart + copyBytes, data + position + i);
          bufferStart += copyBytes;
          i += copyBytes;
        }
        consumed = count;
      }
    }
    remainingValues -= consumed;
    position += count;
    // skip over any null values
    while (nulls && position < numValues && bits::isBitNull(nulls, position)) {
      position += 1;
    }
  }
}

std::unique_ptr<ByteRleDecoder> createByteRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek) {
  return std::make_unique<ByteRleDecoder>(std::move(input), ek);
}

void BooleanRleDecoder::seekToRowGroup(
    dwio::common::PositionProvider& positionProvider) {
  ByteRleDecoder::seekToRowGroup(positionProvider);
  uint64_t consumed = positionProvider.next();
  DWIO_ENSURE_LE(
      consumed,
      8,
      "bad position ",
      encodingKey_.toString(),
      ", ",
      inputStream->getName());
  if (consumed != 0) {
    remainingBits = 8 - consumed;
    ByteRleDecoder::next(
        reinterpret_cast<char*>(&reversedLastByte), 1, nullptr);
    bits::reverseBits(&reversedLastByte, 1);
  } else {
    remainingBits = 0;
  }
}

void BooleanRleDecoder::skip(uint64_t numValues) {
  if (numValues <= remainingBits) {
    remainingBits -= numValues;
  } else {
    numValues -= remainingBits;
    remainingBits = 0;
    uint64_t bytesSkipped = numValues / 8;
    ByteRleDecoder::skip(bytesSkipped);
    uint64_t bitsToSkip = numValues % 8;
    if (bitsToSkip) {
      ByteRleDecoder::next(
          reinterpret_cast<char*>(&reversedLastByte), 1, nullptr);
      bits::reverseBits(&reversedLastByte, 1);
      remainingBits = 8 - bitsToSkip;
    }
  }
}

void BooleanRleDecoder::next(
    char* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  uint64_t nonNulls = numValues;
  if (nulls) {
    nonNulls = bits::countNonNulls(nulls, 0, numValues);
  }

  const uint32_t outputBytes = (numValues + 7) / 8;
  if (nonNulls == 0) {
    memset(data, 0, outputBytes);
    return;
  }

  if (remainingBits >= nonNulls) {
    // The remaining bits from last round is enough for this round, and we don't
    // need to read new data. Since remainingBits should be less than or equal
    // to 8, therefore nonNulls must be less than 8.
    data[0] = reversedLastByte >> (8 - remainingBits) & 0xff >> (8 - nonNulls);
    remainingBits -= nonNulls;
  } else {
    // Put the remaining bits, if any, into previousByte
    uint8_t previousByte = 0;
    if (remainingBits > 0) {
      previousByte = reversedLastByte >> (8 - remainingBits);
    }

    // We need to read in (nonNulls - remainingBits) values and it must be a
    // positive number if nonNulls is positive
    const uint64_t bytesRead = ((nonNulls - remainingBits) + 7) / 8;
    ByteRleDecoder::next(data, bytesRead, nullptr);

    bits::reverseBits(reinterpret_cast<uint8_t*>(data), bytesRead);
    reversedLastByte = data[bytesRead - 1];

    // Now shift the data in place
    if (remainingBits > 0) {
      uint64_t nonNullDWords = nonNulls / 64;
      // Shift 64 bits a time when there're enough data. Note that the data
      // buffer was created 64-bits aligned so there won't be performance
      // degradation shifting it in 64-bit unit.
      for (uint64_t i = 0; i < nonNullDWords; i++) {
        uint64_t tmp = reinterpret_cast<uint64_t*>(data)[i];
        reinterpret_cast<uint64_t*>(data)[i] =
            previousByte | tmp << remainingBits; // previousByte is LSB
        previousByte = (tmp >> (64 - remainingBits)) & 0xff;
      }

      // Shift 8 bits a time for the remaining bits
      const uint64_t nonNullOutputBytes = (nonNulls + 7) / 8;
      for (int32_t i = nonNullDWords * 8; i < nonNullOutputBytes; i++) {
        uint8_t tmp = data[i]; // already reversed
        data[i] = previousByte | tmp << remainingBits; // previousByte is LSB
        previousByte = tmp >> (8 - remainingBits);
      }
    }
    remainingBits = bytesRead * 8 + remainingBits - nonNulls;
  }

  // unpack data for nulls
  if (numValues > nonNulls) {
    bits::scatterBits(nonNulls, numValues, data, nulls, data);
  }

  // clear the most significant bits in the last byte which will be processed in
  // the next round
  data[outputBytes - 1] &= 0xff >> (outputBytes * 8 - numValues);
}

std::unique_ptr<BooleanRleDecoder> createBooleanRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek) {
  return std::make_unique<BooleanRleDecoder>(std::move(input), ek);
}

} // namespace facebook::velox::dwrf
