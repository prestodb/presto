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

#include "velox/dwio/dwrf/writer/WriterShared.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"

namespace facebook::velox::dwrf {

// We currently use previous stripe raw size as the proxy for the expected
// stripe raw size. For the first stripe, we are more conservative about
// flush overhead memory unless we know otherwise, e.g. perhaps from
// encoding DB work.
// This can be fitted linearly just like flush overhead or perhaps
// figured out from the schema.
// This can be simplified with Slice::estimateMemory().
size_t estimateNextWriteSize(const WriterContext& context, size_t numRows) {
  // This is 0 for first slice. We are assuming reasonable input for now.
  return folly::to<size_t>(ceil(context.getAverageRowSize() * numRows));
}

int64_t getTotalMemoryUsage(const WriterContext& context) {
  const auto& outputStreamPool =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM);
  const auto& dictionaryPool =
      context.getMemoryUsage(MemoryUsageCategory::DICTIONARY);
  const auto& generalPool =
      context.getMemoryUsage(MemoryUsageCategory::GENERAL);

  // TODO: override getCurrentBytes() to be lock-free for leaf level pools.
  return outputStreamPool.getCurrentBytes() + dictionaryPool.getCurrentBytes() +
      generalPool.getCurrentBytes();
}

uint64_t WriterShared::flushTimeMemoryUsageEstimate(
    const WriterContext& context,
    size_t nextWriteSize) const {
  return getTotalMemoryUsage(context) +
      context.getEstimatedStripeSize(nextWriteSize) +
      context.getEstimatedFlushOverhead(context.stripeRawSize + nextWriteSize);
}

// Flush if we cannot take one additional slice/stride based on current stripe
// raw size.
bool WriterShared::overMemoryBudget(
    const WriterContext& context,
    size_t writeLength) const {
  size_t nextWriteSize = estimateNextWriteSize(context, writeLength);
  return flushTimeMemoryUsageEstimate(context, nextWriteSize) >
      context.getMemoryBudget();
}

// Writer will flush to make more memory if the incoming stride would make
// it exceed memory budget with the default flush policy. Other policies
// can intentionally throw and expect the application to retry.
//
// The current approach is to assume that the customer passes in slices of
// similar sizes, perhaps even bounded by a configurable amount. We then
// compute the soft_cap = hard_budget - expected_increment_per_slice, and
// compare that against a dynamically determined flush_overhead +
// current_total_usage and try to flush preemptively after writing each
// slice/stride to bring the current memory usage below the soft_cap again.
//
// Using less memory than the soft_cap ensures being able to
// write a new slice/stride, unless the slice/stride is drastically bigger
// than the previous ones.
bool WriterShared::shouldFlush(
    const WriterContext& context,
    size_t nextWriteLength) {
  // If we are hitting memory budget before satisfying flush criteria, try
  // entering low memory mode to work with less memory-intensive encodings.
  bool overBudget = overMemoryBudget(context, nextWriteLength);
  if (UNLIKELY(
          overBudget && !context.isLowMemoryMode() &&
          !flushPolicy_(false, context))) {
    enterLowMemoryMode();
    // Recalculate memory usage due to encoding switch.
    overBudget = overMemoryBudget(context, nextWriteLength);
  }
  return flushPolicy_(overBudget, context);
}

void WriterShared::setLowMemoryMode() {
  getContext().setLowMemoryMode();
}

bool safeToSwitchEncoding(const WriterContext& context) {
  return getTotalMemoryUsage(context) +
      context.getEstimatedEncodingSwitchOverhead() >
      context.getMemoryBudget();
}

// Low memory allows for the writer to write the same data with a lower
// memory budget.
// Currently this method is only called locally to switch encoding if
// we couldn't meet flush criteria without exceeding memory budget.
// NOTE: switching encoding is not a good mitigation for immediate memory
// pressure because the switch consumes even more memory than a flush.
void WriterShared::enterLowMemoryMode() {
  auto& context = getContext();
  // Until we have capability to abandon dictionary after the first
  // stripe, do nothing and rely solely on flush to comply with budget.
  // TODO: extract context.canSwitchEncoding().
  if (UNLIKELY(context.checkLowMemoryMode() && context.stripeIndex == 0)) {
    if (safeToSwitchEncoding(context)) {
      // Idempotent call to switch to less memory intensive encodings.
      abandonDictionariesImpl();
    }
  }
}

void WriterShared::flushStripe(bool close) {
  auto& context = getContext();
  auto preFlushTotalMemoryUsage = getTotalMemoryUsage(context);
  int64_t preFlushStreamMemoryUsage =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM)
          .getCurrentBytes();
  if (context.stripeRowCount == 0) {
    return;
  }

  dwio::common::MetricsLog::StripeFlushMetrics metrics;
  metrics.outputStreamMemoryEstimate = context.getEstimatedOutputStreamSize();
  metrics.stripeSizeEstimate =
      context.getEstimatedStripeSize(context.stripeRawSize);

  if (context.isIndexEnabled && context.indexRowCount > 0) {
    createRowIndexEntry();
  }

  auto& handler = context.getEncryptionHandler();
  proto::StripeFooter footer;
  std::vector<proto::StripeEncryptionGroup> groups;

  // initialize encryption groups
  if (handler.isEncrypted()) {
    auto count = handler.getEncryptionGroupCount();
    // We use uint32_t::max to represent non-encrypted when adding streams, so
    // make sure number of encryption groups is smaller than that
    DWIO_ENSURE_LT(count, std::numeric_limits<uint32_t>::max());
    groups.resize(count);
  }

  flushImpl([&](uint32_t nodeId) -> proto::ColumnEncoding& {
    if (handler.isEncrypted(nodeId)) {
      auto index = handler.getEncryptionGroupIndex(nodeId);
      return *groups.at(index).add_encoding();
    } else {
      return *footer.add_encoding();
    }
  });

  // Collects the memory increment from flushing data to output streams.
  auto flushOverhead =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM)
          .getCurrentBytes() -
      preFlushStreamMemoryUsage;
  context.recordFlushOverhead(flushOverhead);
  metrics.flushOverhead = flushOverhead;

  auto& sink = getSink();
  auto stripeOffset = sink.size();

  uint32_t lastIndex = 0;
  uint64_t offset = 0;
  auto addStream = [&](const auto& stream, const auto& out) {
    proto::Stream* s;
    uint32_t currentIndex;
    auto nodeId = stream.node;
    if (handler.isEncrypted(nodeId)) {
      currentIndex = handler.getEncryptionGroupIndex(nodeId);
      s = groups.at(currentIndex).add_streams();
    } else {
      s = footer.add_streams();
      currentIndex = std::numeric_limits<uint32_t>::max();
    }

    // set offset only when needed, ie. when offset of current stream cannot be
    // calculated based on offset and length of previous stream. In that case,
    // it must be that current stream and previous stream doesn't belong to same
    // encryption group or neither are encrypted. So the logic is simplified to
    // check if group index are the same for current and previous stream
    if (offset > 0 && lastIndex != currentIndex) {
      s->set_offset(offset);
    }
    lastIndex = currentIndex;

    // Jolly/Presto readers can't read streams bigger than 2GB.
    validateStreamSize(stream, out.size());

    s->set_kind(static_cast<proto::Stream_Kind>(stream.kind));
    s->set_node(nodeId);
    s->set_column(stream.column);
    s->set_sequence(stream.sequence);
    s->set_length(out.size());
    s->set_usevints(context.getConfig(Config::USE_VINTS));
    offset += out.size();

    context.incrementNodeSize(nodeId, out.size());
  };

  // TODO: T45025996 Discard all empty streams at flush time.
  // deals with streams
  uint64_t indexLength = 0;
  sink.setMode(WriterSink::Mode::Index);
  LayoutPlanner planner(context);
  planner.iterateIndexStreams([&](auto& streamId, auto& content) {
    DWIO_ENSURE_EQ(
        streamId.kind,
        StreamKind::StreamKind_ROW_INDEX,
        "unexpected stream kind ",
        streamId.kind);
    indexLength += content.size();
    addStream(streamId, content);
    sink.addBuffers(content);
  });

  uint64_t dataLength = 0;
  sink.setMode(WriterSink::Mode::Data);
  planner.iterateDataStreams([&](auto& streamId, auto& content) {
    DWIO_ENSURE_NE(
        streamId.kind,
        StreamKind::StreamKind_ROW_INDEX,
        "unexpected stream kind ",
        streamId.kind);
    dataLength += content.size();
    addStream(streamId, content);
    sink.addBuffers(content);
  });
  DWIO_ENSURE_GT(dataLength, 0);
  metrics.stripeSize = dataLength;

  if (handler.isEncrypted()) {
    // fill encryption metadata
    for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
      auto group = footer.add_encryptiongroups();
      writeProtoAsString(
          *group,
          groups.at(i),
          std::addressof(handler.getEncryptionProviderByIndex(i)));
    }
  }

  // flush footer
  uint64_t footerOffset = sink.size();
  DWIO_ENSURE_EQ(footerOffset, stripeOffset + dataLength + indexLength);

  sink.setMode(WriterSink::Mode::Footer);
  writeProto(footer);
  sink.setMode(WriterSink::Mode::None);

  auto& stripe = addStripeInfo();
  stripe.set_offset(stripeOffset);
  stripe.set_indexlength(indexLength);
  stripe.set_datalength(dataLength);
  stripe.set_footerlength(sink.size() - footerOffset);

  // set encryption key metadata
  if (handler.isEncrypted() && context.stripeIndex == 0) {
    for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
      *stripe.add_keymetadata() =
          handler.getEncryptionProviderByIndex(i).getKey();
    }
  }

  context.recordAverageRowSize();
  context.recordCompressionRatio(dataLength);

  auto totalMemoryUsage = getTotalMemoryUsage(context);
  metrics.limit = totalMemoryUsage;
  metrics.availableMemory = context.getMemoryBudget() - totalMemoryUsage;

  auto& dictionaryDataMemoryUsage =
      context.getMemoryUsage(MemoryUsageCategory::DICTIONARY);
  metrics.dictionaryMemory = dictionaryDataMemoryUsage.getCurrentBytes();
  // TODO: what does this try to capture?
  metrics.maxDictSize = dictionaryDataMemoryUsage.getMaxBytes();

  metrics.stripeIndex = context.stripeIndex;
  metrics.rawStripeSize = context.stripeRawSize;
  metrics.rowsInStripe = context.stripeRowCount;
  metrics.compressionRatio = context.getCompressionRatio();
  metrics.flushOverheadRatio = context.getFlushOverheadRatio();
  metrics.averageRowSize = context.getAverageRowSize();
  metrics.groupSize = 0;
  metrics.close = close;

  LOG(INFO) << fmt::format(
      "Flush overhead = {}, data length = {}",
      metrics.flushOverhead,
      metrics.stripeSize);
  // Add flush overhead and other ratio logging.
  context.metricLogger->logStripeFlush(metrics);

  // prepare for next stripe
  context.nextStripe();
  resetImpl();
}

void WriterShared::flush(bool close) {
  flushStripe(close);

  auto& context = getContext();
  auto& footer = getFooter();
  // if this is the last stripe, add footer
  if (close) {
    auto& handler = context.getEncryptionHandler();
    std::vector<std::vector<proto::FileStatistics>> stats;
    proto::Encryption* encryption = nullptr;

    // initialize encryption related metadata only when there is data written
    if (handler.isEncrypted() && footer.stripes_size() > 0) {
      auto count = handler.getEncryptionGroupCount();
      stats.resize(count);
      encryption = footer.mutable_encryption();
      encryption->set_keyprovider(
          encryption::toProto(handler.getKeyProviderType()));
      for (uint32_t i = 0; i < count; ++i) {
        encryption->add_encryptiongroups();
      }
    }

    std::optional<uint32_t> lastRoot;
    std::unordered_map<proto::ColumnStatistics*, proto::ColumnStatistics*>
        statsMap;
    writeFileStatsImpl([&](uint32_t nodeId) -> proto::ColumnStatistics& {
      auto entry = footer.add_statistics();
      if (!encryption || !handler.isEncrypted(nodeId)) {
        return *entry;
      }

      auto root = handler.getEncryptionRoot(nodeId);
      auto groupIndex = handler.getEncryptionGroupIndex(nodeId);
      auto& group = stats.at(groupIndex);
      if (!lastRoot || root != lastRoot.value()) {
        // this is a new root, add to the footer, and use a new slot
        group.emplace_back();
        encryption->mutable_encryptiongroups(groupIndex)->add_nodes(root);
      }
      lastRoot = root;
      auto encryptedStats = group.back().add_statistics();
      statsMap[entry] = encryptedStats;
      return *encryptedStats;
    });

#define COPY_STAT(from, to, stat) \
  if (from->has_##stat()) {       \
    to->set_##stat(from->stat()); \
  }

    // fill basic stats
    for (auto& pair : statsMap) {
      COPY_STAT(pair.second, pair.first, numberofvalues);
      COPY_STAT(pair.second, pair.first, hasnull);
      COPY_STAT(pair.second, pair.first, rawsize);
      COPY_STAT(pair.second, pair.first, size);
    }

#undef COPY_STAT

    // set metadata for each encryption group
    if (encryption) {
      for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
        auto group = encryption->mutable_encryptiongroups(i);
        // set stats. No need to set key metadata since it just reused the same
        // key of the first stripe
        for (auto& s : stats.at(i)) {
          writeProtoAsString(
              *group->add_statistics(),
              s,
              std::addressof(handler.getEncryptionProviderByIndex(i)));
        }
      }
    }

    writeFooter(*schema_->type);
  }

  // flush to sink
  auto& sink = getSink();
  sink.flush();

  if (close) {
    context.metricLogger->logFileClose(
        footer.contentlength(),
        sink.size(),
        sink.getCacheSize(),
        sink.getCacheOffsets().size() - 1,
        static_cast<int32_t>(sink.getCacheMode()),
        context.stripeIndex,
        context.stripeRowCount,
        context.stripeRawSize,
        context.getStreamCount(),
        context.getWriterMemoryUsage().getCurrentBytes());
  }
}

void WriterShared::close() {
  flush(true);
  WriterBase::close();
}

} // namespace facebook::velox::dwrf
