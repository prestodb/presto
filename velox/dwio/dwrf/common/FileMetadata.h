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
#include "velox/dwio/common/Common.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/common/wrap/orc-proto-wrapper.h"

namespace facebook::velox::dwrf {

enum class DwrfFormat : uint8_t { kDwrf, kOrc };

class ProtoWrapperBase {
 protected:
  DwrfFormat format_;
  void* impl_;

 public:
  ProtoWrapperBase() = delete;

  ProtoWrapperBase(DwrfFormat format, void* impl)
      : format_{format}, impl_{impl} {}

  DwrfFormat format() const {
    return format_;
  }

  inline void* rawProtoPtr() const {
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

  dwio::common::CompressionKind compression() const;

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
  inline proto::PostScript* dwrfPtr() const {
    return reinterpret_cast<proto::PostScript*>(impl_.get());
  }

  inline proto::orc::PostScript* orcPtr() const {
    return reinterpret_cast<proto::orc::PostScript*>(impl_.get());
  }
};

class StripeInformationWrapper : public ProtoWrapperBase {
 public:
  explicit StripeInformationWrapper(proto::StripeInformation* si)
      : ProtoWrapperBase(DwrfFormat::kDwrf, si) {}

  explicit StripeInformationWrapper(proto::orc::StripeInformation* si)
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
    return reinterpret_cast<proto::StripeInformation*>(rawProtoPtr());
  }

  inline const proto::orc::StripeInformation* orcPtr() const {
    return reinterpret_cast<proto::orc::StripeInformation*>(rawProtoPtr());
  }
};

class TypeWrapper : public ProtoWrapperBase {
 public:
  explicit TypeWrapper(proto::Type* t)
      : ProtoWrapperBase(DwrfFormat::kDwrf, t) {}
  explicit TypeWrapper(proto::orc::Type* t)
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
  inline proto::Type* dwrfPtr() const {
    return reinterpret_cast<proto::Type*>(rawProtoPtr());
  }
  inline proto::orc::Type* orcPtr() const {
    return reinterpret_cast<proto::orc::Type*>(rawProtoPtr());
  }
};

class UserMetadataItemWrapper : public ProtoWrapperBase {
 public:
  explicit UserMetadataItemWrapper(proto::UserMetadataItem* item)
      : ProtoWrapperBase(DwrfFormat::kDwrf, item) {}

  explicit UserMetadataItemWrapper(proto::orc::UserMetadataItem* item)
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
  inline proto::UserMetadataItem* dwrfPtr() const {
    return reinterpret_cast<proto::UserMetadataItem*>(rawProtoPtr());
  }
  inline proto::orc::UserMetadataItem* orcPtr() const {
    return reinterpret_cast<proto::orc::UserMetadataItem*>(rawProtoPtr());
  }
};

class FooterWrapper : public ProtoWrapperBase {
 public:
  explicit FooterWrapper(proto::Footer* footer)
      : ProtoWrapperBase(DwrfFormat::kDwrf, footer) {}

  explicit FooterWrapper(proto::orc::Footer* footer)
      : ProtoWrapperBase(DwrfFormat::kOrc, footer) {}

  const proto::Footer* getDwrfPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return reinterpret_cast<proto::Footer*>(rawProtoPtr());
  }

  const proto::orc::Footer* getOrcPtr() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kOrc);
    return reinterpret_cast<proto::orc::Footer*>(rawProtoPtr());
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
                                        : false;
  }

  uint32_t rowIndexStride() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->rowindexstride() : 0;
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

  // TODO: ORC has not supported column statistics yet
  int statisticsSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->statistics_size() : 0;
  }

  const ::google::protobuf::RepeatedPtrField<
      ::facebook::velox::dwrf::proto::ColumnStatistics>&
  statistics() const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->statistics();
  }

  const ::facebook::velox::dwrf::proto::ColumnStatistics& statistics(
      int index) const {
    VELOX_CHECK_EQ(format_, DwrfFormat::kDwrf);
    return dwrfPtr()->statistics(index);
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
        ? StripeInformationWrapper(dwrfPtr()->mutable_stripes(index))
        : StripeInformationWrapper(orcPtr()->mutable_stripes(index));
  }

  int typesSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->types_size()
                                        : orcPtr()->types_size();
  }

  TypeWrapper types(int index) const {
    return format_ == DwrfFormat::kDwrf
        ? TypeWrapper(dwrfPtr()->mutable_types(index))
        : TypeWrapper(orcPtr()->mutable_types(index));
  }

  int metadataSize() const {
    return format_ == DwrfFormat::kDwrf ? dwrfPtr()->metadata_size()
                                        : orcPtr()->metadata_size();
  }

  UserMetadataItemWrapper metadata(int index) const {
    return format_ == DwrfFormat::kDwrf
        ? UserMetadataItemWrapper(dwrfPtr()->mutable_metadata(index))
        : UserMetadataItemWrapper(orcPtr()->mutable_metadata(index));
  }

 private:
  // private helper with no format checking
  inline proto::Footer* dwrfPtr() const {
    return reinterpret_cast<proto::Footer*>(rawProtoPtr());
  }
  inline proto::orc::Footer* orcPtr() const {
    return reinterpret_cast<proto::orc::Footer*>(rawProtoPtr());
  }
};

} // namespace facebook::velox::dwrf
