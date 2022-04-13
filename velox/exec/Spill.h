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

#include "velox/common/file/File.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

// Input stream backed by spill file.
class SpillInput : public ByteStream {
 public:
  // Reads from 'input' using 'buffer' for buffering reads.
  SpillInput(std::unique_ptr<ReadFile>&& input, BufferPtr buffer)
      : input_(std::move(input)),
        buffer_(std::move(buffer)),
        size_(input_->size()) {
    next(true);
  }

  void next(bool throwIfPastEnd) override;

  // True if all of the file has been read into vectors.
  bool atEnd() const {
    return offset_ >= size_ && ranges()[0].position >= ranges()[0].size;
  }

 private:
  std::unique_ptr<ReadFile> input_;
  BufferPtr buffer_;
  const uint64_t size_;
  // Offset of first byte not in 'buffer_'
  uint64_t offset_ = 0;
};

// A source of spilled RowVectors coming either from a file or memory.
class SpillStream : public MergeStream {
 public:
  SpillStream(RowTypePtr type, int32_t numSortingKeys, memory::MemoryPool& pool)
      : type_(std::move(type)),
        numSortingKeys_(numSortingKeys),
        pool_(pool),
        ordinal_(++ordinalCounter_) {}

  virtual ~SpillStream() = default;

  bool hasData() const final {
    return index_ < size_;
  }

  bool operator<(const MergeStream& other) const final {
    return compare(other) < 0;
  }

  int32_t compare(const MergeStream& other) const {
    auto& otherStream = static_cast<const SpillStream&>(other);
    auto& children = rowVector_->children();
    auto& otherChildren = otherStream.current().children();
    int32_t key = 0;
    do {
      auto result = children[key]->compare(
          otherChildren[key].get(), index_, otherStream.index_);
      if (result) {
        return result;
      }
    } while (++key < numSortingKeys_);
    return 0;
  }

  virtual std::string label() const {
    return fmt::format("{}", ordinal_);
  }

  void pop();

  const RowVector& current() const {
    return *rowVector_;
  }

  vector_size_t currentIndex() const {
    return index_;
  }

  // Returns a DecodedVector set decoding the 'index'th child of 'rowVector_'
  DecodedVector& decoded(int32_t index) {
    ensureDecodedValid(index);
    return decoded_[index];
  }

  virtual uint64_t size() const = 0;

 protected:
  // loads the next 'rowVector' and sets 'decoded_' if this is initialized.
  void setNextBatch() {
    nextBatch();
    if (!decoded_.empty()) {
      ensureRows();
      for (auto i = 0; i < decoded_.size(); ++i) {
        decoded_[i].decode(*rowVector_->childAt(i), rows_);
      }
    }
  }

  void ensureDecodedValid(int32_t index) {
    int32_t oldSize = decoded_.size();
    if (index < oldSize) {
      return;
    }
    ensureRows();
    decoded_.resize(index + 1);
    for (auto i = oldSize; i <= index; ++i) {
      decoded_[index].decode(*rowVector_->childAt(index), rows_);
    }
  }

  void ensureRows() {
    if (rows_.size() != rowVector_->size()) {
      rows_.resize(size_);
    }
  }

  // Loads the next RowVector from the backing storage, e.g. spill file or
  // RowContainer.
  virtual void nextBatch() = 0;

  // Type of 'rowVector_'. Needed for setting up writing.
  const RowTypePtr type_;

  // Number of leading columns of 'rowVector_'  on which the values are sorted,
  // 0 if not sorted.
  const int32_t numSortingKeys_;

  memory::MemoryPool& pool_;

  // Current batch of rows.
  RowVectorPtr rowVector_;

  // The current row in 'rowVector_'
  vector_size_t index_{0};

  // Number of rows in 'rowVector_'
  vector_size_t size_{0};

  // Decoded vectors for leading parts of 'rowVector_'. Initialized on first use
  // and maintained when updating 'rowVector_'.
  std::vector<DecodedVector> decoded_;

  // Covers all rows inn 'rowVector_' Set if 'decoded_' is non-empty.
  SelectivityVector rows_;

  // Ordinal number used for making a label for debugging.
  const int32_t ordinal_;

  static std::atomic<int32_t> ordinalCounter_;
};

// Represents a spill file that is first in write mode and then
// turns into a source of spilled RowVectors. Owns a file system file that
// contains the spilled data and is live for the duration of 'this'.
class SpillFile : public SpillStream {
 public:
  SpillFile(
      RowTypePtr type,
      int32_t numSortingKeys,
      const std::string& path,
      memory::MemoryPool& pool)
      : SpillStream(std::move(type), numSortingKeys, pool),
        path_(fmt::format("{}-{}", path, ordinalCounter_++)) {}

  ~SpillFile() override;

  // Returns a file for writing spilled data. The caller constructs
  // this, then calls output() and writes serialized data to the file
  // and calls finishWrite when the file has reached its final
  // size. For sorted spilling, the data in one file is expected to be
  // sorted.
  WriteFile& output();

  bool isWritable() const {
    return output_ != nullptr;
  }

  // Finishes writing and flushes any unwritten data.
  void finishWrite() {
    VELOX_CHECK(output_);
    fileSize_ = output_->size();
    output_ = nullptr;
  }

  // Prepares 'this' for reading. Positions the read at the first row of
  // content. The caller must call output() and finishWrite() before this.
  void startRead();

  // Returns the file size in bytes. During the writing phase this is
  // the current size of the file, during reading this is the final
  // size.
  uint64_t size() const override {
    if (output_) {
      return output_->size();
    }
    return fileSize_;
  }

  // Sets 'result' to refer to the next row of content of 'this'.
  void read(RowVector& result);

 private:
  void nextBatch() override;

  const std::string path_;
  // Byte size of the backing file. Set when finishing writing.
  uint64_t fileSize_ = 0;
  std::unique_ptr<WriteFile> output_;
  std::unique_ptr<SpillInput> input_;
};

// Sequence of files for one partition of the spilled data. If data is
// sorted, each file is sorted. The globally sorted order is produced
// by merging the constituent files.
class SpillFileList {
 public:
  // Constructs a set of spill files. 'type' is a RowType describing the
  // content. 'numSortingKeys' is the number of leading columns on which the
  // data is sorted. 'path' is a file path prefix. ' 'targetFileSize' is the
  // target byte size of a single file in the file set. 'pool' and
  // 'mappedMemory' are used for buffering and constructing the result data read
  // from 'this'.
  //
  // When writing sorted spill runs, the caller is responsible for buffering and
  // sorting the data. write is called multiple times, followed by flush().
  SpillFileList(
      RowTypePtr type,
      int32_t numSortingKeys,
      const std::string& path,
      uint64_t targetFileSize,
      memory::MemoryPool& pool,
      memory::MappedMemory& mappedMemory)
      : type_(type),
        numSortingKeys_(numSortingKeys),
        path_(path),
        targetFileSize_(targetFileSize),
        pool_(pool),
        mappedMemory_(mappedMemory) {}

  // Adds 'rows' for the positions in 'indices' into 'this'. The indices
  // must produce a view where the rows are sorted if sorting is desired.
  // Consecutive calls must have sorted data so that the first row of the
  // next call is not less than the last row of the previous call.
  void write(
      const RowVectorPtr& rows,
      const folly::Range<IndexRange*>& indices);

  // Closes the current output file if any. Subsequent calls to write will start
  // a new one.
  void finishFile();

  std::vector<std::unique_ptr<SpillFile>> files() {
    VELOX_CHECK(!files_.empty());
    finishFile();
    return std::move(files_);
  }

  int64_t spilledBytes() const;

 private:
  // Returns the current file to write to and creates one if needed.
  WriteFile& currentOutput();
  // Writes data from 'batch_' to the current output file.
  void flush();
  const RowTypePtr type_;
  const int32_t numSortingKeys_;
  const std::string path_;
  const uint64_t targetFileSize_;
  memory::MemoryPool& pool_;
  memory::MappedMemory& mappedMemory_;
  std::unique_ptr<VectorStreamGroup> batch_;
  std::vector<std::unique_ptr<SpillFile>> files_;
};

// Represents all spilled data of an operator, e.g. order by or group
// by. This has one SpillFileList per partition of spill data.
class SpillState {
 public:
  // Constructs a SpillState. 'type' is the content RowType. 'path' is
  // the file system path prefix. 'bits' is the hash bit field for
  // partitioning data between files. This also gives the maximum
  // number of partitions. 'numSortingKeys' is the number of leading columns
  // on which the data is sorted, 0 if only hash partitioning is used.
  // 'targetFileSize' is the target size of a single
  // file.  'pool' and 'mappedMemory' own
  // the memory for state and results.
  SpillState(
      const std::string& path,
      int32_t maxPartitions,
      int32_t numSortingKeys,
      uint64_t targetFileSize,
      memory::MemoryPool& pool,
      memory::MappedMemory& mappedMemory)
      : path_(path),
        maxPartitions_(maxPartitions),
        numSortingKeys_(numSortingKeys),
        targetFileSize_(targetFileSize),
        files_(maxPartitions_),
        pool_(pool),
        mappedMemory_(mappedMemory) {}

  int32_t numPartitions() const {
    return numPartitions_;
  }

  // Sets how many of the spill partitions are in use.
  void setNumPartitions(int32_t numPartitions);

  // Returns how many ways spilled data can be partitioned.
  int32_t maxPartitions() const {
    return maxPartitions_;
  }

  uint64_t targetFileSize() const {
    return targetFileSize_;
  }

  memory::MemoryPool& pool() const {
    return pool_;
  }

  // Appends data to 'partition'. The rows given by 'indices' must be
  // sorted for a sorted spill and must hash to 'partition'. It is
  // safe to call this on multiple threads if all threads specify a
  // different partition.
  void appendToPartition(int32_t partition, const RowVectorPtr& rows);

  // Finishes a sorted run for 'partition'. If write is called for 'partition'
  // again, the data does not have to be sorted relative to the data
  // written so far.
  void finishWrite(int32_t partition) {
    files_[partition]->finishFile();
  }

  // Starts reading values for 'partition'. If 'extra' is non-null, it can be
  // a stream of rows from a RowContainer so as to merge unspilled
  // data with spilled data.
  std::unique_ptr<TreeOfLosers<SpillStream>> startMerge(
      int32_t partition,
      std::unique_ptr<SpillStream>&& extra);

  bool hasFiles(int32_t partition) {
    return partition < files_.size() && files_[partition];
  }

  int64_t spilledBytes() const;

 private:
  const RowTypePtr type_;
  const std::string path_;
  const int32_t maxPartitions_;
  const int32_t numSortingKeys_;
  // Number of currently spilling partitions.
  int32_t numPartitions_ = 0;
  const uint64_t targetFileSize_;
  // A file list for each spilled partition. Only partitions that have
  // started spilling have an entry here.
  std::vector<std::unique_ptr<SpillFileList>> files_;
  memory::MemoryPool& pool_;
  memory::MappedMemory& mappedMemory_;
};

} // namespace facebook::velox::exec
