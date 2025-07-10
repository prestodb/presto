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

#pragma once

#include <cstring>
#include <string_view>

#include "folly/io/IOBuf.h"

// Primitives for reading and writing to a stream of bytes.

namespace facebook::wave::nimble::encoding {

// The write functions serialize a datum into a buffer, advancing the
// buffer pointer appropriately.

// Write a type in native format for numeric types, via WriteString
// for string types.
template <typename T>
inline void write(T value, char*& pos) {
  *reinterpret_cast<T*>(pos) = value;
  pos += sizeof(T);
}

inline void writeChar(char value, char*& pos) {
  write<char>(value, pos);
}

inline void writeUint32(uint32_t value, char*& pos) {
  write<uint32_t>(value, pos);
}

inline void writeUint64(uint64_t value, char*& pos) {
  write<uint64_t>(value, pos);
}

// Just the chars, no leading length.
inline void writeBytes(std::string_view value, char*& pos) {
  std::copy(value.cbegin(), value.cend(), pos);
  pos += value.size();
}

// Just the buffers, no leading length.
inline void writeBuffers(const folly::IOBuf& buffers, char*& pos) {
  for (const auto buffer : buffers) {
    std::copy(buffer.cbegin(), buffer.cend(), pos);
    pos += buffer.size();
  }
}

// 4 byte length followed by chars.
inline void writeString(std::string_view value, char*& pos) {
  writeUint32(value.size(), pos);
  writeBytes(value, pos);
}

template <>
inline void write(std::string_view value, char*& pos) {
  writeString(value, pos);
}

template <>
inline void write(const std::string& value, char*& pos) {
  writeString({value.data(), value.size()}, pos);
}

// The read functions extract a datum from the buffer (stored in the
// format output by the Write functions), advancing the buffer
// pointer appropriately.

// Reads a type written via Write.
template <typename T, typename TReturn = T>
inline TReturn read(const char*& pos) {
  const T value = *reinterpret_cast<const T*>(pos);
  pos += sizeof(T);
  return static_cast<TReturn>(value);
}

inline char readChar(const char*& pos) {
  return read<char>(pos);
}

inline uint32_t readUint32(const char*& pos) {
  return read<uint32_t>(pos);
}

inline uint64_t readUint64(const char*& pos) {
  return read<uint64_t>(pos);
}

inline std::string_view readString(const char*& pos) {
  const uint32_t size = readUint32(pos);
  std::string_view result(pos, size);
  pos += size;
  return result;
}

inline std::string readOwnedString(const char*& pos) {
  const uint32_t size = readUint32(pos);
  std::string result(pos, size);
  pos += size;
  return result;
}

template <>
inline std::string_view read<std::string_view, std::string_view>(
    const char*& pos) {
  return readString(pos);
}

template <>
inline std::string read<std::string, std::string>(const char*& pos) {
  return readOwnedString(pos);
}

template <typename T, typename TReturn = T>
inline TReturn peek(const char* pos) {
  return static_cast<TReturn>(*reinterpret_cast<const T*>(pos));
}

} // namespace facebook::wave::nimble::encoding
