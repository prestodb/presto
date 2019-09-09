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

import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

class ReadTracker
{
    private final List<StreamId> readOrder = new ArrayList();
    private final Object2LongOpenHashMap<StreamId> readCounts = new Object2LongOpenHashMap();
    private long totalReads;
    private boolean hasData;

    private static final LoadingCache<String, ReadTracker> readTrackers =
            CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(CacheLoader.from(key -> new ReadTracker()));

    public ReadTracker()
    {
        readCounts.defaultReturnValue(0);
    }

    public void recordRead(StreamId streamId)
    {
        long count = readCounts.getLong(streamId);
        synchronized (this) {
            if (count == 0) {
                readOrder.add(streamId);
                hasData = true;
            }
            readCounts.put(streamId, count + 1);
            totalReads++;
        }
    }

    public boolean needsPrefetch(StreamId streamId)
    {
        long count = readCounts.get(streamId);
        int numStreams = readOrder.size();
        if (numStreams == 0) {
            return false;
        }
        return count > (totalReads / numStreams) / 2;
    }

    public void schedulePrefetch(Map<StreamId, DiskRange> ranges, OrcDataSource dataSource)
    {
        for (StreamId streamId : readOrder) {
            if (needsPrefetch(streamId)) {
                DiskRange range = ranges.get(streamId);
                if (range == null) {
                    continue;
                }
                FileCache.asyncPrefetch(dataSource,
                        range.getOffset(),
                        Math.min(FileCacheInput.MAX_BUFFER_SIZE, range.getLength()),
                        FileCache.getListener(streamId.getLabel()),
                        100);
            }
        }
    }

    public static ReadTracker getTracker()
    {
        String threadName = Thread.currentThread().getName();
        try {
            ReadTracker tracker = readTrackers.get(threadName);
            if (!tracker.hasData) {
                // This is a new tracker, see if there is another with data about this query/task.
                int prefixLength = taskIdPrefixLength(threadName);
                Map<String, ReadTracker> map = readTrackers.asMap();
                for (Map.Entry<String, ReadTracker> entry : map.entrySet()) {
                    if (entry.getKey().regionMatches(0, threadName, 0, prefixLength)) {
                        ReadTracker other = entry.getValue();
                        if (other.hasData) {
                            // Read the read order of the other to init read order of this.
                            synchronized (other) {
                                int numStreams = other.readOrder.size();
                                tracker.totalReads = other.totalReads;
                                for (int i = 0; i < numStreams; i++) {
                                    StreamId otherStream = other.readOrder.get(i);
                                    if (otherStream == null) {
                                        continue;
                                    }
                                    long otherCount = other.readCounts.getLong(otherStream);
                                    if (otherCount > 0) {
                                        long ownCount = tracker.readCounts.getLong(otherStream);
                                        if (ownCount == 0) {
                                            tracker.readOrder.add(otherStream);
                                            tracker.readCounts.put(otherStream, otherCount);
                                        }
                                    }
                                }
                                tracker.hasData = true;
                            }
                            break;
                        }
                    }
                }
            }
            return tracker;
        }
        catch (Exception e) {
            throw new PrestoException(NOT_SUPPORTED, e); //return null;
        }
    }

    private static int taskIdPrefixLength(String threadName)
    {
        int dash = threadName.lastIndexOf('-');
        return threadName.lastIndexOf('.', dash);
    }
}
