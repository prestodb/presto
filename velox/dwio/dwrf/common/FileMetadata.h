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

#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/common/compression/Compression.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/common/wrap/orc-proto-wrapper.h"

namespace facebook::velox::dwrf {

enum class DwrfFormat : uint8_t {
  kDwrf = 0,
  kOrc = 1,
};

class ProtoWrapperBase {
 protected:
  ProtoWrapperBase(DwrfFormat format, const void* impl)
      : format_{format}, impl_{impl} {}

  DwrfFormat format_;
  const void* impl_;

 public:
  DwrfFormat format() const {
    return format_;
  }

  inline const void* rawProtoPtr() const {
    return impl_;
  }
};

/***
 * PostScript that takes the ownership of proto::PostScript /
 *proto::orc::PostScript and provides access to the attributes
 ***/
class PostScript {
  DwrfFormat format_;
  std::unique_ptr<google::protobuf::Message> impl_;

 public:
  PostScript() = delete;

  explicit PostScript(std::unique_ptr<proto::PostScript> ps)
      : format_{DwrfFormat::kDwrf}, impl_{std::move(ps)} {}

  explicit PostScript(proto::PostScript&& ps)
      : format_{DwrfFormat::kDwrf},
        impl_{std::make_unique<proto::PostScript>(std::move(ps))} {}

  explicit PostScript(std::unique_ptr<proto::orc::PostScript> ps)
      : format_{DwrfFormat::kOrc}, impl_{std::move(ps)} {}

  explicit PostScript(proto::orc::PostScript&& ps)
      : format_{DwrfFormat::kOrc},
        impl_{std::make_unique<proto::orc::PostScript>(std::move(ps))} {}

  const proto::PostScript* getDwrfPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr();
  }

  const proto::orc::PostScript* getOrcPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kOrc);
    return orcPtr();
  }

  DwrfFormat format() const {
    return format_;
  }

  // General methods
  bool hasFooterLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_footerlength()
                                        : orcPtr()->has_footerlength();
  }

  uint64_t footerLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->footerlength()
                                        : orcPtr()->footerlength();
  }

  bool hasCompression() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_compression()
                                        : orcPtr()->has_compression();
  }

  common::CompressionKind compression() const;

  bool hasCompressionBlockSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_compressionblocksize()
                                        : orcPtr()->has_compressionblocksize();
  }

  uint64_t compressionBlockSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->compressionblocksize()
                                        : orcPtr()->compressionblocksize();
  }

  bool hasWriterVersion() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_writerversion()
                                        : orcPtr()->has_writerversion();
  }

  uint32_t writerVersion() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->writerversion()
                                        : orcPtr()->writerversion();
  }

  // DWRF-specific methods
  bool hasCacheMode() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_cachemode() : false;
  }

  StripeCacheMode cacheMode() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return static_cast<StripeCacheMode>(dwrfPtr()->cachemode());
  }

  bool hasCacheSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_cachesize() : false;
  }

  uint32_t cacheSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->cachesize();
  }

  PostScript copy() const {
    switch (format_) {
      case DwrfFormat::kDwrf:
        return PostScript(proto::PostScript(*dwrfPtr()));
      case DwrfFormat::kOrc:
        return PostScript(proto::orc::PostScript(*orcPtr()));
    }
  }

 private:
  inline const proto::PostScript* dwrfPtr() const {
    return reinterpret_cast<proto::PostScript*>(impl_.get());
  }

  inline const proto::orc::PostScript* orcPtr() const {
    return reinterpret_cast<proto::orc::PostScript*>(impl_.get());
  }
};

class StripeInformationWrapper : public ProtoWrapperBase {
 public:
  explicit StripeInformationWrapper(const proto::StripeInformation* si)
      : ProtoWrapperBase(DwrfFormat::kDwrf, si) {}

  explicit StripeInformationWrapper(const proto::orc::StripeInformation* si)
      : ProtoWrapperBase(DwrfFormat::kOrc, si) {}

  const proto::StripeInformation* getDwrfPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr();
  }

  const proto::orc::StripeInformation* getOrcPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kOrc);
    return orcPtr();
  }

  uint64_t offset() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->offset()
                                        : orcPtr()->offset();
  }

  uint64_t indexLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->indexlength()
                                        : orcPtr()->indexlength();
  }

  uint64_t dataLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->datalength()
                                        : orcPtr()->datalength();
  }

  uint64_t footerLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->footerlength()
                                        : orcPtr()->footerlength();
  }

  uint64_t numberOfRows() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->numberofrows()
                                        : orcPtr()->numberofrows();
  }

  // DWRF-specific fields
  uint64_t rawDataSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->rawdatasize();
  }

  int64_t checksum() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->checksum();
  }

  uint64_t groupSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->groupsize();
  }

  int keyMetadataSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->keymetadata_size();
  }

  const std::string& keyMetadata(int index) const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->keymetadata(index);
  }

  const ::google::protobuf::RepeatedPtrField<std::string>& keyMetadata() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->keymetadata();
  }

 private:
  // private helper with no format checking
  inline const proto::StripeInformation* dwrfPtr() const {
    return reinterpret_cast<const proto::StripeInformation*>(rawProtoPtr());
  }

  inline const proto::orc::StripeInformation* orcPtr() const {
    return reinterpret_cast<const proto::orc::StripeInformation*>(
        rawProtoPtr());
  }
};

class TypeWrapper : public ProtoWrapperBase {
 public:
  explicit TypeWrapper(const proto::Type* t)
      : ProtoWrapperBase(DwrfFormat::kDwrf, t) {}
  explicit TypeWrapper(const proto::orc::Type* t)
      : ProtoWrapperBase(DwrfFormat::kOrc, t) {}

  const proto::Type* getDwrfPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr();
  }

  const proto::orc::Type* getOrcPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kOrc);
    return orcPtr();
  }

  TypeKind kind() const;

  int subtypesSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->subtypes_size()
                                        : orcPtr()->subtypes_size();
  }

  const ::google::protobuf::RepeatedField<::google::protobuf::uint32>&
  subtypes() {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->subtypes()
                                        : orcPtr()->subtypes();
  }

  uint32_t subtypes(int index) const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->subtypes(index)
                                        : orcPtr()->subtypes(index);
  }

  int fieldNamesSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->fieldnames_size()
                                        : orcPtr()->fieldnames_size();
  }

  const ::google::protobuf::RepeatedPtrField<std::string>& fieldNames() {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->fieldnames()
                                        : orcPtr()->fieldnames();
  }

  const std::string& fieldNames(int index) const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->fieldnames(index)
                                        : orcPtr()->fieldnames(index);
  }

 private:
  // private helper with no format checking
  inline const proto::Type* dwrfPtr() const {
    return reinterpret_cast<const proto::Type*>(rawProtoPtr());
  }
  inline const proto::orc::Type* orcPtr() const {
    return reinterpret_cast<const proto::orc::Type*>(rawProtoPtr());
  }
};

class UserMetadataItemWrapper : public ProtoWrapperBase {
 public:
  explicit UserMetadataItemWrapper(const proto::UserMetadataItem* item)
      : ProtoWrapperBase(DwrfFormat::kDwrf, item) {}

  explicit UserMetadataItemWrapper(const proto::orc::UserMetadataItem* item)
      : ProtoWrapperBase(DwrfFormat::kOrc, item) {}

  const std::string& name() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->name() : orcPtr()->name();
  }

  const std::string& value() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->value()
                                        : orcPtr()->value();
  }

 private:
  // private helper with no format checking
  inline const proto::UserMetadataItem* dwrfPtr() const {
    return reinterpret_cast<const proto::UserMetadataItem*>(rawProtoPtr());
  }
  inline const proto::orc::UserMetadataItem* orcPtr() const {
    return reinterpret_cast<const proto::orc::UserMetadataItem*>(rawProtoPtr());
  }
};

class IntegerStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit IntegerStatisticsWrapper(
      const proto::IntegerStatistics* intStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, intStatistics) {}

  explicit IntegerStatisticsWrapper(
      const proto::orc::IntegerStatistics* intStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, intStatistics) {}

  bool hasMinimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_minimum()
                                        : orcPtr()->has_minimum();
  }

  int64_t minimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->minimum()
                                        : orcPtr()->minimum();
  }

  bool hasMaximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_maximum()
                                        : orcPtr()->has_maximum();
  }

  int64_t maximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->maximum()
                                        : orcPtr()->maximum();
  }

  bool hasSum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_sum()
                                        : orcPtr()->has_sum();
  }

  int64_t sum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->sum() : orcPtr()->sum();
  }

 private:
  // private helper with no format checking
  inline const proto::IntegerStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::IntegerStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::IntegerStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::IntegerStatistics*>(
        rawProtoPtr());
  }
};

class DoubleStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit DoubleStatisticsWrapper(
      const proto::DoubleStatistics* doubleStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, doubleStatistics) {}

  explicit DoubleStatisticsWrapper(
      const proto::orc::DoubleStatistics* doubleStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, doubleStatistics) {}

  bool hasMinimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_minimum()
                                        : orcPtr()->has_minimum();
  }

  double minimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->minimum()
                                        : orcPtr()->minimum();
  }

  bool hasMaximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_maximum()
                                        : orcPtr()->has_maximum();
  }

  double maximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->maximum()
                                        : orcPtr()->maximum();
  }

  bool hasSum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_sum()
                                        : orcPtr()->has_sum();
  }

  double sum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->sum() : orcPtr()->sum();
  }

 private:
  // private helper with no format checking
  inline const proto::DoubleStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::DoubleStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::DoubleStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::DoubleStatistics*>(rawProtoPtr());
  }
};

class StringStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit StringStatisticsWrapper(
      const proto::StringStatistics* stringStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, stringStatistics) {}

  explicit StringStatisticsWrapper(
      const proto::orc::StringStatistics* stringStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, stringStatistics) {}

  bool hasMinimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_minimum()
                                        : orcPtr()->has_minimum();
  }

  const std::string& minimum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->minimum()
                                        : orcPtr()->minimum();
  }

  bool hasMaximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_maximum()
                                        : orcPtr()->has_maximum();
  }

  const std::string& maximum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->maximum()
                                        : orcPtr()->maximum();
  }

  bool hasSum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_sum()
                                        : orcPtr()->has_sum();
  }

  int64_t sum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->sum() : orcPtr()->sum();
  }

 private:
  // private helper with no format checking
  inline const proto::StringStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::StringStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::StringStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::StringStatistics*>(rawProtoPtr());
  }
};

class BucketStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit BucketStatisticsWrapper(
      const proto::BucketStatistics* bucketStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, bucketStatistics) {}

  explicit BucketStatisticsWrapper(
      const proto::orc::BucketStatistics* bucketStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, bucketStatistics) {}

  int countSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->count_size()
                                        : orcPtr()->count_size();
  }

  uint64_t count(int index) const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->count(index)
                                        : orcPtr()->count(index);
  }

 private:
  // private helper with no format checking
  inline const proto::BucketStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::BucketStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::BucketStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::BucketStatistics*>(rawProtoPtr());
  }
};

class BinaryStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit BinaryStatisticsWrapper(
      const proto::BinaryStatistics* binaryStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, binaryStatistics) {}

  explicit BinaryStatisticsWrapper(
      const proto::orc::BinaryStatistics* binaryStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, binaryStatistics) {}

  bool hasSum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_sum()
                                        : orcPtr()->has_sum();
  }

  int64_t sum() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->sum() : orcPtr()->sum();
  }

 private:
  // private helper with no format checking
  inline const proto::BinaryStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::BinaryStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::BinaryStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::BinaryStatistics*>(rawProtoPtr());
  }
};

class ColumnStatisticsWrapper : public ProtoWrapperBase {
 public:
  explicit ColumnStatisticsWrapper(
      const proto::ColumnStatistics* columnStatistics)
      : ProtoWrapperBase(DwrfFormat::kDwrf, columnStatistics) {}

  explicit ColumnStatisticsWrapper(
      const proto::orc::ColumnStatistics* columnStatistics)
      : ProtoWrapperBase(DwrfFormat::kOrc, columnStatistics) {}

  bool hasNumberOfValues() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_numberofvalues()
                                        : orcPtr()->has_numberofvalues();
  }

  uint64_t numberOfValues() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->numberofvalues()
                                        : orcPtr()->numberofvalues();
  }

  bool hasHasNull() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_hasnull()
                                        : orcPtr()->has_hasnull();
  }

  bool hasNull() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->hasnull()
                                        : orcPtr()->hasnull();
  }

  bool hasRawSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_rawsize() : false;
  }

  uint64_t rawSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->rawsize() : 0;
  }

  bool hasSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_size() : false;
  }

  uint64_t size() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->size() : 0;
  }

  bool hasIntStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_intstatistics()
                                        : orcPtr()->has_intstatistics();
  }

  IntegerStatisticsWrapper intStatistics() const {
    return format_ == DwrfFormat::kDwrf
        ? IntegerStatisticsWrapper(&dwrfPtr()->intstatistics())
        : IntegerStatisticsWrapper(&orcPtr()->intstatistics());
  }

  bool hasDoubleStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_doublestatistics()
                                        : orcPtr()->has_doublestatistics();
  }

  DoubleStatisticsWrapper doubleStatistics() const {
    return format_ == DwrfFormat::kDwrf
        ? DoubleStatisticsWrapper(&dwrfPtr()->doublestatistics())
        : DoubleStatisticsWrapper(&orcPtr()->doublestatistics());
  }

  bool hasStringStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_stringstatistics()
                                        : orcPtr()->has_stringstatistics();
  }

  StringStatisticsWrapper stringStatistics() const {
    return format_ == DwrfFormat::kDwrf
        ? StringStatisticsWrapper(&dwrfPtr()->stringstatistics())
        : StringStatisticsWrapper(&orcPtr()->stringstatistics());
  }

  bool hasBucketStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_bucketstatistics()
                                        : orcPtr()->has_bucketstatistics();
  }

  BucketStatisticsWrapper bucketStatistics() const {
    return format_ == DwrfFormat::kDwrf
        ? BucketStatisticsWrapper(&dwrfPtr()->bucketstatistics())
        : BucketStatisticsWrapper(&orcPtr()->bucketstatistics());
  }

  bool hasBinaryStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_binarystatistics()
                                        : orcPtr()->has_binarystatistics();
  }

  BinaryStatisticsWrapper binaryStatistics() const {
    return format_ == DwrfFormat::kDwrf
        ? BinaryStatisticsWrapper(&dwrfPtr()->binarystatistics())
        : BinaryStatisticsWrapper(&orcPtr()->binarystatistics());
  }

  bool hasMapStatistics() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_mapstatistics()
                                        : false;
  }

  const ::facebook::velox::dwrf::proto::MapStatistics& mapStatistics() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->mapstatistics();
  }

 private:
  // private helper with no format checking
  inline const proto::ColumnStatistics* dwrfPtr() const {
    return reinterpret_cast<const proto::ColumnStatistics*>(rawProtoPtr());
  }
  inline const proto::orc::ColumnStatistics* orcPtr() const {
    return reinterpret_cast<const proto::orc::ColumnStatistics*>(rawProtoPtr());
  }
};

class FooterWrapper : public ProtoWrapperBase {
 public:
  explicit FooterWrapper(const proto::Footer* footer)
      : ProtoWrapperBase(DwrfFormat::kDwrf, footer) {}

  explicit FooterWrapper(const proto::orc::Footer* footer)
      : ProtoWrapperBase(DwrfFormat::kOrc, footer) {}

  const proto::Footer* getDwrfPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return reinterpret_cast<const proto::Footer*>(rawProtoPtr());
  }

  const proto::orc::Footer* getOrcPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kOrc);
    return reinterpret_cast<const proto::orc::Footer*>(rawProtoPtr());
  }

  bool hasHeaderLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_headerlength()
                                        : orcPtr()->has_headerlength();
  }

  uint64_t headerLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->headerlength()
                                        : orcPtr()->headerlength();
  }

  bool hasContentLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_contentlength()
                                        : orcPtr()->has_contentlength();
  }

  uint64_t contentLength() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->contentlength()
                                        : orcPtr()->contentlength();
  }

  bool hasNumberOfRows() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_numberofrows()
                                        : orcPtr()->has_numberofrows();
  }

  uint64_t numberOfRows() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->numberofrows()
                                        : orcPtr()->numberofrows();
  }

  bool hasRawDataSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_rawdatasize() : false;
  }

  uint64_t rawDataSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->rawdatasize();
  }

  bool hasChecksumAlgorithm() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_checksumalgorithm()
                                        : false;
  }

  const proto::ChecksumAlgorithm checksumAlgorithm() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->checksumalgorithm();
  }

  bool hasRowIndexStride() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_rowindexstride()
                                        : orcPtr()->has_rowindexstride();
  }

  uint32_t rowIndexStride() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->rowindexstride()
                                        : orcPtr()->rowindexstride();
  }

  int stripeCacheOffsetsSize() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->stripecacheoffsets_size();
  }

  const ::google::protobuf::RepeatedField<::google::protobuf::uint32>&
  stripeCacheOffsets() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->stripecacheoffsets();
  }

  int statisticsSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->statistics_size()
                                        : orcPtr()->statistics_size();
  }

  const ::google::protobuf::RepeatedPtrField<
      ::facebook::velox::dwrf::proto::ColumnStatistics>&
  statistics() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->statistics();
  }

  const ::facebook::velox::dwrf::proto::ColumnStatistics& dwrfStatistics(
      int index) const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->statistics(index);
  }

  ColumnStatisticsWrapper statistics(int index) const {
    return format_ == DwrfFormat::kDwrf
        ? ColumnStatisticsWrapper(&dwrfPtr()->statistics(index))
        : ColumnStatisticsWrapper(&orcPtr()->statistics(index));
  }

  // TODO: ORC has not supported encryption yet
  bool hasEncryption() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->has_encryption() : false;
  }

  const ::facebook::velox::dwrf::proto::Encryption& encryption() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->encryption();
  }

  int stripesSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->stripes_size()
                                        : orcPtr()->stripes_size();
  }

  StripeInformationWrapper stripes(int index) const {
    return format_ == DwrfFormat::kDwrf
        ? StripeInformationWrapper(&dwrfPtr()->stripes(index))
        : StripeInformationWrapper(&orcPtr()->stripes(index));
  }

  int typesSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->types_size()
                                        : orcPtr()->types_size();
  }

  TypeWrapper types(int index) const {
    return format_ == DwrfFormat::kDwrf ? TypeWrapper(&dwrfPtr()->types(index))
                                        : TypeWrapper(&orcPtr()->types(index));
  }

  int metadataSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->metadata_size()
                                        : orcPtr()->metadata_size();
  }

  UserMetadataItemWrapper metadata(int index) const {
    return format_ == DwrfFormat::kDwrf
        ? UserMetadataItemWrapper(&dwrfPtr()->metadata(index))
        : UserMetadataItemWrapper(&orcPtr()->metadata(index));
  }

 private:
  // private helper with no format checking
  inline const proto::Footer* dwrfPtr() const {
    return reinterpret_cast<const proto::Footer*>(rawProtoPtr());
  }
  inline const proto::orc::Footer* orcPtr() const {
    return reinterpret_cast<const proto::orc::Footer*>(rawProtoPtr());
  }
};

} // namespace facebook::velox::dwrf

template <>
struct fmt::formatter<facebook::velox::dwrf::DwrfFormat> : formatter<int> {
  auto format(facebook::velox::dwrf::DwrfFormat s, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
