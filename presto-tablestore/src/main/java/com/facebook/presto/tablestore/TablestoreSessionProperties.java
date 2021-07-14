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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.TimeRange;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public class TablestoreSessionProperties
{
    private static final Logger log = Logger.get(TablestoreSessionProperties.class);

    public static final long DEFAULT_SPLIT_UNIT_MB = 10;
    public static final long MIN_SPLIT_UNIT_MB = 1;
    public static final long MAX_SPLIT_UNIT_MB = 102400;
    public static final int DEFAULT_FETCH_SIZE = 10000;
    public static final int MIN_FETCH_SIZE = 1000;
    public static final int MAX_FETCH_SIZE = 100000;
    public static final int DEFAULT_QUERY_VERSION = 2;
    public static final int MIN_QUERY_VERSION = 1;
    public static final int MAX_QUERY_VERSION = 2;
    public static final int DEFAULT_FILTER_VERSION = 2;
    public static final int MIN_FILTER_VERSION = 1;
    public static final int MAX_FILTER_VERSION = 2;

    public static final boolean DEFAULT_SPLIT_OPTIMIZE = false;
    public static final boolean DEFAULT_PARTITION_PRUNING = true;
    public static final boolean DEFAULT_INSERT_AS_UPDATE = false;
    public static final boolean DEFAULT_LOOSE_CAST = false;
    public static final boolean DEFAULT_INDEX_PARALLEL_SCAN_MODE = false;
    public static final String DEFAULT_INDEX_FIRST = "";
    public static final int MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS = 1000;
    public static final int MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT = 20;
    public static final double DEFAULT_SPLIT_SIZE_RATIO = 0.5;
    public static final double MIN_SPLIT_SIZE_RATIO = 0.0001;
    public static final double MAX_SPLIT_SIZE_RATIO = 1.0;

    public static final String HINT_QUERY_VERSION = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-query-version";
    public static final String HINT_FILTER_VERSION = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-filter-version";
    public static final String HINT_SPLIT_UNIT_MB = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-split-unit-mb";
    public static final String HINT_FETCH_SIZE = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-fetch-size";
    public static final String HINT_START_VERSION = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-start-version";
    public static final String HINT_END_VERSION = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-end-version";
    public static final String HINT_SPLIT_OPTIMIZE = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-split-optimize";
    public static final String HINT_SPLIT_SIZE_RATIO = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-split-size-ratio";
    public static final String HINT_PARTITION_PRUNE = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-partition-prune";
    public static final String HINT_INSERT_AS_UPDATE = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-insert-as-update";
    public static final String HINT_LOOSE_CAST = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-loose-cast";
    public static final String HINT_INDEX_FIRST = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-index-selection-strategy";
    public static final String HINT_INDEX_PARALLEL_SCAN_MODE = TablestoreConnectorId.TABLESTORE_CONNECTOR + "-index-parallel-scan-mode";

    private final List<PropertyMetadata<?>> sessionProperties;

    public TablestoreSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        HINT_QUERY_VERSION,
                        "Specify a proper query version of protocol to communicate with TableStore, " +
                                "DEFAULT=" + DEFAULT_QUERY_VERSION + " and MIN=" + MIN_QUERY_VERSION + " and MAX=" + MAX_QUERY_VERSION,
                        DEFAULT_QUERY_VERSION,
                        false),

                integerProperty(
                        HINT_FILTER_VERSION,
                        "Specify a proper filter version of protocol to communicate with TableStore, " +
                                "DEFAULT=" + DEFAULT_FILTER_VERSION + " and MIN=" + MIN_FILTER_VERSION + " and MAX=" + MAX_FILTER_VERSION,
                        DEFAULT_FILTER_VERSION,
                        false),

                longProperty(
                        HINT_SPLIT_UNIT_MB,
                        "Unit size in MetaBytes(MB) for every split by TableStore to cut, DEFAULT=" +
                                DEFAULT_SPLIT_UNIT_MB + " and MIN=" + MIN_SPLIT_UNIT_MB + " and MAX=" + MAX_SPLIT_UNIT_MB,
                        DEFAULT_SPLIT_UNIT_MB,
                        false),

                integerProperty(
                        HINT_FETCH_SIZE,
                        "Specify the fetch size each http request to TableStore, DEFAULT=" +
                                DEFAULT_FETCH_SIZE + " and MIN=" + MIN_FETCH_SIZE + " and MAX=" + MAX_FETCH_SIZE,
                        DEFAULT_FETCH_SIZE,
                        false),

                longProperty(
                        HINT_START_VERSION,
                        "Specify the start version of data to fetch, DEFAULT=-1 and MIN=0",
                        -1L,
                        false),

                longProperty(
                        HINT_END_VERSION,
                        "Specify the end version of data to fetch, DEFAULT=-1 and MIN=0",
                        -1L,
                        false),

                booleanProperty(
                        HINT_SPLIT_OPTIMIZE,
                        "Switch on/off split optimization for a great number of splits, DEFAULT=" +
                                DEFAULT_SPLIT_OPTIMIZE,
                        DEFAULT_SPLIT_OPTIMIZE,
                        false),

                doubleProperty(
                        HINT_SPLIT_SIZE_RATIO,
                        "When you specify the '" + HINT_SPLIT_OPTIMIZE + "' option, and you expect DLA " +
                                "to reduce or control the total count of splits which protects underlying Tablestore server " +
                                "from 'ServerIsBusy' exception, DEFAULT=" + DEFAULT_SPLIT_SIZE_RATIO + " and MIN=" +
                                MIN_SPLIT_SIZE_RATIO + " and MAX=" + MAX_SPLIT_SIZE_RATIO,
                        DEFAULT_SPLIT_SIZE_RATIO,
                        false),

                booleanProperty(
                        HINT_PARTITION_PRUNE,
                        "Switch on/off partitions pruning for a great number of partitions, DEFAULT=" +
                                DEFAULT_PARTITION_PRUNING,
                        DEFAULT_PARTITION_PRUNING,
                        false),

                booleanProperty(
                        HINT_INSERT_AS_UPDATE,
                        "Use 'update' instead of 'insert' according to primary keys, so it can update a row partially, DEFAULT=" +
                                DEFAULT_INSERT_AS_UPDATE,
                        DEFAULT_INSERT_AS_UPDATE,
                        false),

                booleanProperty(
                        HINT_LOOSE_CAST,
                        "Use loose cast to process data whose types are compatible, so DLA does not throw cast exceptions, DEFAULT=" +
                                DEFAULT_LOOSE_CAST,
                        DEFAULT_LOOSE_CAST,
                        false),

                stringProperty(
                        HINT_INDEX_FIRST,
                        "When you specify this option, DLA prefers using search index of current table to pulling data from original table, " +
                                "which is performance-friendly sometimes because it's more filterable, DEFAULT=" +
                                DEFAULT_INDEX_FIRST,
                        DEFAULT_INDEX_FIRST,
                        false),

                booleanProperty(
                        HINT_INDEX_PARALLEL_SCAN_MODE,
                        "Switch on/off parallel scan mode, DEFAULT=" + DEFAULT_INDEX_PARALLEL_SCAN_MODE,
                        DEFAULT_INDEX_PARALLEL_SCAN_MODE,
                        false));
    }

    public static BiFunction<String, Long, String> errorSupplierForLong(long defaultV, long minV, long maxV)
    {
        return (k, v) -> format("Invalid value '%s' for hint '%s', TYPE=long and DEFAULT=%s and MIN=%s and MAX=%s",
                v, k, defaultV, minV, maxV);
    }

    public static BiFunction<String, Integer, String> errorSupplierForInt(int defaultV, int minV, int maxV)
    {
        return (k, v) -> format("Invalid value '%s' for hint '%s', TYPE=int and DEFAULT=%s and MIN=%s and MAX=%s",
                v, k, defaultV, minV, maxV);
    }

    public static BiFunction<String, Double, String> errorSupplierForDouble(double defaultV, double minV, double maxV)
    {
        return (k, v) -> format("Invalid value '%.4f' for hint '%s', TYPE=double and DEFAULT=%.4f and MIN=%.4f and MAX=%.4f",
                v, k, defaultV, minV, maxV);
    }

    public static BiFunction<String, Boolean, String> errorSupplierForBoolean(boolean defaultV)
    {
        return (k, v) -> format("Invalid value '%s' for hint '%s', TYPE=boolean and DEFAULT=%s and OPTIONAL VALUES=true,false", v, k, defaultV);
    }

    protected static <R> R getFromConnectorSession(ConnectorSession session, String key,
            Class<R> expected,
            BiFunction<String, R, String> errorSupplier,
            Predicate<R> rangeChecker)
    {
        String queryId = session.getQueryId();
        log.debug("Query tablestore with hint[%s], queryId=%s", key, queryId);
        R v;
        try {
            v = session.getProperty(key, expected);
        }
        catch (Throwable e) {
            log.error("Parsing failed for hint[%s], queryId=%s", key, queryId, e);
            throw e;
        }
        if (!rangeChecker.test(v)) {
            log.error("Parsed and checking range failed for hint[%s=%s], queryId=%s", key, v, queryId);
            throw new IllegalArgumentException(errorSupplier.apply(key, v));
        }
        return v;
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    protected static int getQueryVersion(ConnectorSession session)
    {
        BiFunction<String, Integer, String> f = errorSupplierForInt(DEFAULT_QUERY_VERSION, MIN_QUERY_VERSION, MAX_QUERY_VERSION);
        return getFromConnectorSession(session, HINT_QUERY_VERSION, Integer.class, f,
                x -> x >= MIN_QUERY_VERSION && x <= MAX_QUERY_VERSION);
    }

    protected static int getFilterVersion(ConnectorSession session)
    {
        BiFunction<String, Integer, String> f = errorSupplierForInt(DEFAULT_FILTER_VERSION, MIN_FILTER_VERSION, MAX_FILTER_VERSION);
        return getFromConnectorSession(session, HINT_FILTER_VERSION, Integer.class, f,
                x -> x >= MIN_FILTER_VERSION && x <= MAX_FILTER_VERSION);
    }

    protected static long getSplitUnitBytes(ConnectorSession session)
    {
        BiFunction<String, Long, String> f = errorSupplierForLong(DEFAULT_SPLIT_UNIT_MB, MIN_SPLIT_UNIT_MB, MAX_SPLIT_UNIT_MB);
        return getFromConnectorSession(session, HINT_SPLIT_UNIT_MB, Long.class, f,
                x -> x >= MIN_SPLIT_UNIT_MB && x <= MAX_SPLIT_UNIT_MB) * 1024L * 1024L;
    }

    protected static int getFetchSize(ConnectorSession session)
    {
        BiFunction<String, Integer, String> f = errorSupplierForInt(DEFAULT_FETCH_SIZE, MIN_FETCH_SIZE, MAX_FETCH_SIZE);
        return getFromConnectorSession(session, HINT_FETCH_SIZE, Integer.class, f,
                x -> x >= MIN_FETCH_SIZE && x <= MAX_FETCH_SIZE);
    }

    protected static Optional<TimeRange> getTimeRange(ConnectorSession session)
    {
        long start = getFromConnectorSession(session, HINT_START_VERSION, Long.class, (k, v) -> v + "", x -> true);
        long end = getFromConnectorSession(session, HINT_END_VERSION, Long.class, (k, v) -> v + "", x -> true);

        if (start >= 0 && end >= 0) {
            TimeRange t = new TimeRange(start, end);
            return Optional.of(t);
        }
        else if (start >= 0) {
            TimeRange t = new TimeRange(start);
            return Optional.of(t);
        }
        else if (end >= 0) {
            throw new RuntimeException("You can't only specify end version but no start version, it's invalid.");
        }
        return Optional.empty();
    }

    protected static boolean enableSplitOptimization(ConnectorSession session)
    {
        BiFunction<String, Boolean, String> f = errorSupplierForBoolean(DEFAULT_SPLIT_OPTIMIZE);
        return getFromConnectorSession(session, HINT_SPLIT_OPTIMIZE, Boolean.class, f, b -> true);
    }

    protected static double getSplitSizeRatio(ConnectorSession session)
    {
        BiFunction<String, Double, String> f = errorSupplierForDouble(DEFAULT_SPLIT_SIZE_RATIO, MIN_SPLIT_SIZE_RATIO, MAX_SPLIT_SIZE_RATIO);
        return getFromConnectorSession(session, HINT_SPLIT_SIZE_RATIO, Double.class, f,
                x -> x >= MIN_SPLIT_SIZE_RATIO && x <= MAX_SPLIT_SIZE_RATIO);
    }

    protected static boolean enablePartitionPruning(ConnectorSession session)
    {
        BiFunction<String, Boolean, String> f = errorSupplierForBoolean(DEFAULT_PARTITION_PRUNING);
        return getFromConnectorSession(session, HINT_PARTITION_PRUNE, Boolean.class, f, b -> true);
    }

    protected static boolean enableInsertAsUpdate(ConnectorSession session)
    {
        BiFunction<String, Boolean, String> f = errorSupplierForBoolean(DEFAULT_INSERT_AS_UPDATE);
        return getFromConnectorSession(session, HINT_INSERT_AS_UPDATE, Boolean.class, f, b -> true);
    }

    protected static boolean enableLooseCast(ConnectorSession session)
    {
        BiFunction<String, Boolean, String> f = errorSupplierForBoolean(DEFAULT_LOOSE_CAST);
        return getFromConnectorSession(session, HINT_LOOSE_CAST, Boolean.class, f, b -> true);
    }

    protected static IndexSelectionStrategy extractIndexFirst(ConnectorSession session)
    {
        String string = getFromConnectorSession(session, HINT_INDEX_FIRST, String.class, (k, v) -> v, b -> true);
        return IndexSelectionStrategy.parse(Optional.empty(), HINT_INDEX_FIRST, string);
    }

    protected static boolean isParallelScanMode(ConnectorSession session)
    {
        BiFunction<String, Boolean, String> f = errorSupplierForBoolean(DEFAULT_INDEX_PARALLEL_SCAN_MODE);
        return getFromConnectorSession(session, HINT_INDEX_PARALLEL_SCAN_MODE, Boolean.class, f, b -> true);
    }
}
