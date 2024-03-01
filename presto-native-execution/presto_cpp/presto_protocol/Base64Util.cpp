/*
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
#include "presto_cpp/presto_protocol/Base64Util.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
namespace facebook::presto::protocol {
namespace {
ByteInputStream toByteStream(const std::string& input) {
  ByteRange byteRange{
      reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
      (int32_t)input.length(),
      0};
  return ByteInputStream({byteRange});
}
} // namespace

velox::VectorPtr readBlock(
    const velox::TypePtr& type,
    const std::string& base64Encoded,
    velox::memory::MemoryPool* pool) {
  const std::string data = velox::encoding::Base64::decode(base64Encoded);

  auto byteStream = toByteStream(data);
  VectorPtr result;
  serializer::presto::PrestoVectorSerde serde;
  serde.deserializeSingleColumn(&byteStream, pool, type, &result, nullptr);
  return result;
}

} // namespace facebook::presto::protocol
