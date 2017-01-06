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

import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.spi.PrestoException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Map;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROWS_COLUMN;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROW_ID;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Abstract class used to read metrics regarding the table index.
 */
public abstract class MetricsReader
        implements AutoCloseable
{
    private static final Range METRICS_TABLE_ROWID_RANGE = new Range(new Text(METRICS_TABLE_ROW_ID.array()));
    private static final String METRICS_TABLE_ROWS_COLUMN_STRING = new String(METRICS_TABLE_ROWS_COLUMN.array(), UTF_8);
    private static final Authorizations EMPTY_AUTHS = new Authorizations();

    protected MetricsStorage metricsStorage;

    public MetricsReader(MetricsStorage storage)
    {
        this.metricsStorage = requireNonNull(storage, "storage is null");
    }

    /**
     * Gets the number of rows for the given table based on the user authorizations
     *
     * @param schema Schema name
     * @param table Table name
     * @return Number of rows in the given table
     */
    public long getNumRowsInTable(String schema, String table)
            throws Exception
    {
        return getCardinality(new MetricCacheKey(metricsStorage, schema, table, METRICS_TABLE_ROWS_COLUMN_STRING, null, false, EMPTY_AUTHS, METRICS_TABLE_ROWID_RANGE));
    }

    /**
     * Gets the number of rows in the table where a column contains a specific value,
     * based on the data in the given {@link MetricCacheKey}.
     * <p>
     * Implementations must account for both exact and non-exact Accumulo Range objects.
     *
     * @param key Metric key
     * @return Cardinality of the given value/column combination
     */
    public abstract long getCardinality(MetricCacheKey key)
            throws Exception;

    /**
     * Gets the number of rows in the table where a column contains a specific value,
     * based on the data in the given collection of {@link MetricCacheKey}.  All Range objects
     * in the collection of keys contain <b>exact</b> Accumulo Range objects, i.e. they are a
     * single value (vs. a range of values).
     * <p>
     * Note that the returned map <b>must</b> contain an entry for each key in the given collection.
     *
     * @param keys Collection of metric keys
     * @return A map containing the cardinality
     */
    public abstract Map<MetricCacheKey, Long> getCardinalities(Collection<MetricCacheKey> keys)
            throws Exception;

    /**
     * Gets any key from the given non-empty collection, validating that all other keys
     * in the collection are the same (except for the Range in the key).
     * <p>
     * In order to simplify the implementation of {@link MetricsReader#getCardinalities}, we are making a (safe) assumption
     * that the CacheKeys will all contain the same combination of schema/table/family/qualifier/authorizations.
     *
     * @param keys Non-empty collection of keys
     * @return Any key
     */
    public MetricCacheKey getAnyKey(Collection<MetricCacheKey> keys)
    {
        MetricCacheKey anyKey = keys.stream().findAny().get();
        keys.forEach(k -> {
            if (!k.schema.equals(anyKey.schema) || !k.table.equals(anyKey.table) || !k.family.equals(anyKey.family) || !k.qualifier.equals(anyKey.qualifier) || !k.auths.equals(anyKey.auths)) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "loadAll called with a non-homogeneous collection of cache keys");
            }
        });
        return anyKey;
    }

    /**
     * Gets the column family of the given cache key, accounting for the optional qualifier.
     * <p>
     * If qualifier is null, the key's column family is returned.  Else, the family is concatenated with the
     * qualifier, split by an underscore, i.e. &lt;family&gt;_&lt;qualifier&gt;
     *
     * @param key Metric cache key
     * @return Text object of the column family
     */
    public String getColumnFamily(MetricCacheKey key)
    {
        if (key.qualifier.isPresent()) {
            return new String(Indexer.getIndexColumnFamily(key.family.getBytes(UTF_8), key.qualifier.get().getBytes(UTF_8)).array(), UTF_8);
        }
        else {
            return key.family;
        }
    }
}
