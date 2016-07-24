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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MILLISECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Abstract class for storing index metrics.  Implementors will be notified when they'll need to create
 * a resource when a Presto table is created, dropped, or renamed.  Also contains functions to factory
 * implementations of {@link MetricsWriter} and {@link MetricsReader}.
 */
public abstract class MetricsStorage
{
    public enum TimestampPrecision
    {
        MILLISECOND,
        SECOND,
        MINUTE,
        HOUR,
        DAY
    }

    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("___METRICS_TABLE___".getBytes(UTF_8));
    public static final ByteBuffer METRICS_TABLE_ROWS_COLUMN = wrap("___rows___".getBytes(UTF_8));

    protected final AccumuloConfig config;
    protected final Connector connector;

    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();
    private static final Logger LOG = Logger.get(MetricsStorage.class);

    /**
     * Gets the default implementation of {@link MetricsStorage}, {@link AccumuloMetricsStorage}
     *
     * @param connector Accumulo Connector
     * @param config Connector configuration
     * @return The default implementation
     */
    public static MetricsStorage getDefault(Connector connector, AccumuloConfig config)
    {
        return new AccumuloMetricsStorage(connector, config);
    }

    public MetricsStorage(Connector connector, AccumuloConfig config)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Gets a Boolean value indicating whether or not metric storage exists for the given Presto table
     *
     * @param table Presto table
     * @return True if it exists, false otherwise
     */
    public abstract boolean exists(SchemaTableName table);

    /**
     * Create metric storage for the given Presto table.
     * <p>
     * This method must always succeed (barring any exception),
     * i.e. if the metric storage already exists for the table, then do not
     * attempt to re-create it or throw an exception.
     *
     * @param table Presto table
     */
    public abstract void create(AccumuloTable table);

    /**
     * Rename the metric storage for the 'old' Presto table to the new Presto table
     *
     * @param oldTable Previous table info
     * @param newTable New table info
     */
    public abstract void rename(AccumuloTable oldTable, AccumuloTable newTable);

    /**
     * Drop metric storage for the given Presto table
     * <p>
     * This method must always succeed (barring any exception),
     * i.e. if the metric storage does not exist for the table, then do nothing
     * -- do not raise an exception.
     *
     * @param table Presto table
     */
    public abstract void drop(AccumuloTable table);

    /**
     * Creates a new instance of {@link MetricsWriter} for the given table
     *
     * @param table Presto table
     * @return Metrics writer
     */
    public abstract MetricsWriter newWriter(AccumuloTable table);

    /**
     * Creates a new instance of {@link MetricsReader}
     *
     * @return Metrics reader
     */
    public abstract MetricsReader newReader();

    public static Map<TimestampPrecision, Long> getTruncatedTimestamps(long value)
    {
        return ImmutableMap.of(
                DAY, value / 86400000L * 86400000L,
                HOUR, value / 3600000L * 3600000L,
                MINUTE, value / 60000L * 60000L,
                SECOND, value / 1000L * 1000L);
    }

    /**
     * Gets a Boolean value indicating if the given Range is an exact value
     *
     * @param range Range to check
     * @return True if exact, false otherwise
     */
    public static boolean isExact(Range range)
    {
        return !range.isInfiniteStartKey() && !range.isInfiniteStopKey()
                && range.getStartKey().followingKey(PartialKey.ROW).equals(range.getEndKey());
    }

    public static Multimap<TimestampPrecision, Range> splitTimestampRange(Range value)
    {
        requireNonNull(value);

        // Selfishly refusing to split any infinite-ended ranges
        if (value.isInfiniteStopKey() || value.isInfiniteStartKey() || isExact(value)) {
            return ImmutableMultimap.of(MILLISECOND, value);
        }

        Multimap<TimestampPrecision, Range> splitTimestampRange = MultimapBuilder.enumKeys(TimestampPrecision.class).arrayListValues().build();

        Text text = new Text();
        value.getStartKey().getRow(text);
        boolean startKeyInclusive = text.getLength() == 9;
        Timestamp startTime = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(text.getBytes(), 0, 9)));

        value.getEndKey().getRow(text);
        Timestamp endTime = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(text.getBytes(), 0, 9)));
        boolean endKeyInclusive = text.getLength() == 10;
        LOG.debug(format("Start: %s  %s  End: %s %s", startTime, startKeyInclusive, endTime, endKeyInclusive));
        if (startTime.getTime() + 1000L > endTime.getTime()) {
            LOG.debug(format("%s %s %s", MILLISECOND, new Timestamp(SERIALIZER.decode(TIMESTAMP, value.getStartKey().getRow().copyBytes())), new Timestamp(SERIALIZER.decode(TIMESTAMP, value.getEndKey().getRow().copyBytes()))));
            return ImmutableMultimap.of(MILLISECOND, value);
        }

        Pair<TimestampPrecision, Timestamp> nextTime = Pair.of(getPrecision(startTime), startTime);
        Pair<TimestampPrecision, Range> previousRange = null;

        switch (nextTime.getLeft()) {
            case MILLISECOND:
                break;
            case SECOND:
                int compare = Long.compare(startTime.getTime() + 1000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(SECOND, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case MINUTE:
                compare = Long.compare(startTime.getTime() + 60000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(MINUTE, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(SECOND, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case HOUR:
                compare = Long.compare(startTime.getTime() + 3600000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(HOUR, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(MINUTE, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case DAY:
                compare = Long.compare(startTime.getTime() + 86400000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(DAY, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(HOUR, new Range(new Text(SERIALIZER.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
        }

        boolean cont = true;
        do {
            Pair<TimestampPrecision, Timestamp> prevTime = nextTime;
            nextTime = getNextTimestamp(prevTime.getRight(), endTime);

            Pair<TimestampPrecision, Range> nextRange;
            if (prevTime.getLeft() == MILLISECOND && nextTime.getLeft() == MILLISECOND) {
                nextRange = Pair.of(MILLISECOND, new Range(new Text(SERIALIZER.encode(TIMESTAMP, prevTime.getRight().getTime())), startKeyInclusive, new Text(SERIALIZER.encode(TIMESTAMP, nextTime.getRight().getTime())), true));
            }
            else if (nextTime.getLeft() == MILLISECOND) {
                Pair<TimestampPrecision, Timestamp> followingTime = getNextTimestamp(nextTime.getRight(), endTime);
                nextRange = Pair.of(MILLISECOND, new Range(new Text(SERIALIZER.encode(TIMESTAMP, nextTime.getRight().getTime())), true, new Text(SERIALIZER.encode(TIMESTAMP, followingTime.getRight().getTime())), endKeyInclusive));
                cont = false;
            }
            else {
                nextRange = Pair.of(nextTime.getLeft(), new Range(new Text(SERIALIZER.encode(TIMESTAMP, nextTime.getRight().getTime()))));
            }

            // Combine this range into previous range
            if (previousRange != null) {
                if (previousRange.getLeft().equals(nextRange.getLeft())) {
                    previousRange = Pair.of(previousRange.getLeft(), new Range(
                            previousRange.getRight().getStartKey(),
                            previousRange.getRight().isStartKeyInclusive(),
                            nextRange.getRight().getEndKey(),
                            nextRange.getRight().isEndKeyInclusive()));
                }
                else {
                    // Add range to map and roll over
                    Timestamp s = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(previousRange.getRight().getStartKey().getRow().getBytes(), 0, 9)));
                    if (isExact(previousRange.getRight())) {
                        LOG.debug(format("%s %s", previousRange.getLeft(), s));
                    }
                    else {
                        Timestamp e = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(previousRange.getRight().getEndKey().getRow().getBytes(), 0, 9)));
                        LOG.debug(format("%s %s %s", previousRange.getLeft(), s, e));
                    }

                    splitTimestampRange.put(previousRange.getLeft(), previousRange.getRight());
                    previousRange = nextRange;
                }
            }
            else {
                previousRange = nextRange;
            }
        }
        while (cont && !nextTime.getRight().equals(endTime));

        Timestamp s = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(previousRange.getRight().getStartKey().getRow().getBytes(), 0, 9)));
        if (isExact(previousRange.getRight())) {
            LOG.debug(format("%s %s", previousRange.getLeft(), s));
        }
        else {
            Timestamp e = new Timestamp(SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(previousRange.getRight().getEndKey().getRow().getBytes(), 0, 9)));
            LOG.debug(format("%s %s %s", previousRange.getLeft(), s, e));
        }

        splitTimestampRange.put(previousRange.getLeft(), previousRange.getRight());
        return ImmutableMultimap.copyOf(splitTimestampRange);
    }

    private static Pair<TimestampPrecision, Timestamp> getNextTimestamp(Timestamp start, Timestamp end)
    {
        Timestamp nextTimestamp = advanceTimestamp(start, end);
        TimestampPrecision nextPrecision = getPrecision(nextTimestamp);
        if (nextTimestamp.equals(end)) {
            return Pair.of(nextPrecision, nextTimestamp);
        }

        TimestampPrecision followingPrecision = getPrecision(advanceTimestamp(nextTimestamp, end));

        int compare = nextPrecision.compareTo(followingPrecision);
        if (compare == 0) {
            return Pair.of(nextPrecision, nextTimestamp);
        }
        else if (compare < 0) {
            return Pair.of(nextPrecision, nextTimestamp);
        }
        else {
            return Pair.of(followingPrecision, nextTimestamp);
        }
    }

    private static Timestamp advanceTimestamp(Timestamp start, Timestamp end)
    {
        switch (getPrecision(start)) {
            case DAY:
                long time = start.getTime() + 86400000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case HOUR:
                time = start.getTime() + 3600000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case MINUTE:
                time = start.getTime() + 60000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case SECOND:
                time = start.getTime() + 1000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case MILLISECOND:
                if ((start.getTime() - 999L) % 1000 == 0) {
                    return new Timestamp(Math.min(start.getTime() + 1, end.getTime()));
                }
                else {
                    return new Timestamp(Math.min((start.getTime() / 1000L * 1000L) + 999L, end.getTime()));
                }
            default:
                throw new PrestoException(NOT_FOUND, "Unknown precision:" + getPrecision(start));
        }
    }

    private static TimestampPrecision getPrecision(Timestamp time)
    {
        if (time.getTime() % 1000 == 0) {
            if (time.getTime() % 60000L == 0) {
                if (time.getTime() % 3600000L == 0) {
                    if (time.getTime() % 86400000L == 0) {
                        return DAY;
                    }
                    else {
                        return HOUR;
                    }
                }
                else {
                    return MINUTE;
                }
            }
            else {
                return SECOND;
            }
        }
        else {
            return MILLISECOND;
        }
    }
}
