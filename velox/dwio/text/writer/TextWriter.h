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

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/text/writer/BufferedWriterSink.h"

namespace facebook::velox::text {

using dwio::common::SerDeOptions;

struct WriterOptions : public dwio::common::WriterOptions {
  int64_t defaultFlushCount = 10 << 10;
  uint8_t headerLineCount =
      0; // number of lines in the header, currently only support 0 or 1
};

/// Encodes Velox vectors in TextFormat and writes into a FileSink.
class TextWriter : public dwio::common::Writer {
 public:
  /// Constructs a writer with output to a 'sink'.
  /// @param schema specifies the file's overall schema, and it is always
  /// non-null.
  /// @param sink output sink
  /// @param options writer options
  /// @param serDeOptions specifies the serialization options
  TextWriter(
      RowTypePtr schema,
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<text::WriterOptions>& options,
      const SerDeOptions& serDeOptions = SerDeOptions());

  ~TextWriter() override = default;

  void write(const VectorPtr& data) override;

  void flush() override;

  bool finish() override {
    close();
    return true;
  }

  void close() override;

  void abort() override;

 private:
  uint8_t getDelimiterForDepth(uint8_t depth) const;

  void writeCellValue(
      const std::shared_ptr<DecodedVector>& decodedColumnVector,
      TypeKind type,
      vector_size_t row,
      uint8_t depth,
      std::optional<uint8_t> delimiter);

  std::string addEscapeChar(std::string&& dataToWrite, uint8_t depth);

  const RowTypePtr schema_;
  const std::unique_ptr<BufferedWriterSink> bufferedWriterSink_;

  uint8_t headerLineCount_;
  SerDeOptions serDeOptions_;
};

class TextWriterFactory : public dwio::common::WriterFactory {
 public:
  TextWriterFactory() : WriterFactory(dwio::common::FileFormat::TEXT) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<dwio::common::WriterOptions>& options) override;

  std::unique_ptr<dwio::common::WriterOptions> createWriterOptions() override;
};

} // namespace facebook::velox::text
