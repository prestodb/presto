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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.stream.StreamDataOutput;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * StreamLayout is used by the Writer to determine the order of the data streams.
 * In ORC, a column is stored in multiple streams (for example DATA, PRESENT,
 * LENGTH, DICTIONARY and so on).
 */
public interface StreamLayout
{
    void reorder(List<StreamDataOutput> dataStreams);

    /**
     * Streams are ordered by the Stream Size. There is no ordering between
     * two streams of the same size. It orders them by ascending order of the
     * stream size, so that multiple of these small IOs can be combined. Note:
     * usually columns contain small (e.g. PRESENT) and large streams (e.g. DATA).
     * This strategy places them far apart and may result in increased IO.
     */
    class ByStreamSize
            implements StreamLayout
    {
        @Override
        public void reorder(List<StreamDataOutput> dataStreams)
        {
            Collections.sort(requireNonNull(dataStreams, "dataStreams is null"));
        }

        @Override
        public String toString()
        {
            return "ByStreamSize{}";
        }
    }

    /**
     * Streams are ordered by Column Size. If two columns have same size then
     * columnId, stream size and stream kind are used for ordering. It orders them
     * by ascending order of column size to read multiple small columns in one IO.
     * All streams for a column are stored together to read one column in one IO.
     */
    class ByColumnSize
            implements StreamLayout
    {
        @Override
        public void reorder(List<StreamDataOutput> dataStreams)
        {
            requireNonNull(dataStreams, "dataStreams is null");
            if (dataStreams.isEmpty()) {
                return;
            }

            Map<Integer, Long> columnSizes = dataStreams.stream()
                    .collect(toImmutableMap(
                            s -> s.getStream().getColumn(),
                            s -> (long) s.getStream().getLength(),
                            Long::sum));

            dataStreams.sort((left, right) -> {
                Stream leftStream = left.getStream();
                Stream rightStream = right.getStream();

                long sizeDelta = columnSizes.get(leftStream.getColumn()) - columnSizes.get(rightStream.getColumn());
                if (sizeDelta != 0) {
                    return sizeDelta < 0 ? -1 : 1;
                }

                int columnDelta = leftStream.getColumn() - rightStream.getColumn();
                if (columnDelta != 0) {
                    return columnDelta;
                }

                sizeDelta = leftStream.getLength() - rightStream.getLength();
                if (sizeDelta != 0) {
                    return sizeDelta < 0 ? -1 : 1;
                }

                return leftStream.getStreamKind().compareTo(rightStream.getStreamKind());
            });
        }

        @Override
        public String toString()
        {
            return "ByColumnSize{}";
        }
    }
}
