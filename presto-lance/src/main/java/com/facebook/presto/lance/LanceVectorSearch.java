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
package com.facebook.presto.lance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Parameters for an approximate-nearest-neighbor (ANN) vector search pushed down to
 * a Lance dataset's pre-built vector index. Produced by {@link LanceVectorSearchPushdown}
 * when it recognizes an {@code ORDER BY <distance-fn>(col, q) ... LIMIT k} pattern, carried
 * on {@link LanceTableHandle}, and consumed by the split manager (segment-aligned splitting)
 * and the page source (building the Lance {@code nearest} query).
 *
 * <p>{@code metric} is one of {@code l2}, {@code cosine}, {@code dot} and maps to a Lance
 * {@code DistanceType} in {@link LanceFragmentPageSource}. {@code queryVector} is stored as a
 * list of doubles for JSON portability and narrowed to {@code float[]} at scan time.
 */
public class LanceVectorSearch
{
    public static final String METRIC_L2 = "l2";
    public static final String METRIC_COSINE = "cosine";
    public static final String METRIC_DOT = "dot";

    /**
     * Name of the distance column Lance appends to every batch of a {@code nearest} scan.
     * Smaller is always nearer, for every metric.
     */
    public static final String DISTANCE_COLUMN_NAME = "_distance";

    private final String column;
    private final List<Double> queryVector;
    private final int k;
    private final String metric;
    private final Optional<Integer> nprobes;
    private final Optional<Integer> ef;
    private final Optional<Integer> refineFactor;
    private final boolean requiresGlobalTopK;

    public LanceVectorSearch(
            String column,
            List<Double> queryVector,
            int k,
            String metric,
            Optional<Integer> nprobes,
            Optional<Integer> ef,
            Optional<Integer> refineFactor)
    {
        this(column, queryVector, k, metric, nprobes, ef, refineFactor, false);
    }

    @JsonCreator
    public LanceVectorSearch(
            @JsonProperty("column") String column,
            @JsonProperty("queryVector") List<Double> queryVector,
            @JsonProperty("k") int k,
            @JsonProperty("metric") String metric,
            @JsonProperty("nprobes") Optional<Integer> nprobes,
            @JsonProperty("ef") Optional<Integer> ef,
            @JsonProperty("refineFactor") Optional<Integer> refineFactor,
            @JsonProperty("requiresGlobalTopK") boolean requiresGlobalTopK)
    {
        this.column = requireNonNull(column, "column is null");
        this.queryVector = ImmutableList.copyOf(requireNonNull(queryVector, "queryVector is null"));
        this.k = k;
        this.metric = requireNonNull(metric, "metric is null");
        this.nprobes = requireNonNull(nprobes, "nprobes is null");
        this.ef = requireNonNull(ef, "ef is null");
        this.refineFactor = requireNonNull(refineFactor, "refineFactor is null");
        this.requiresGlobalTopK = requiresGlobalTopK;
    }

    @JsonProperty
    public String getColumn()
    {
        return column;
    }

    @JsonProperty
    public List<Double> getQueryVector()
    {
        return queryVector;
    }

    @JsonProperty
    public int getK()
    {
        return k;
    }

    @JsonProperty
    public String getMetric()
    {
        return metric;
    }

    @JsonProperty
    public Optional<Integer> getNprobes()
    {
        return nprobes;
    }

    @JsonProperty
    public Optional<Integer> getEf()
    {
        return ef;
    }

    @JsonProperty
    public Optional<Integer> getRefineFactor()
    {
        return refineFactor;
    }

    /**
     * When true, no TopN above this scan merges per-split results yet. Set by the
     * {@code vector_search} table function; {@code LanceVectorSearchTopNRewriter} inserts a
     * global-merge TopN over the scan and clears the flag. If it is still set at split
     * generation (connector optimization disabled), the split manager runs the scan as a
     * single split over all fragments so one Lance {@code nearest} query computes the exact
     * global top-k.
     */
    @JsonProperty
    public boolean isRequiresGlobalTopK()
    {
        return requiresGlobalTopK;
    }

    public float[] getQueryVectorAsFloats()
    {
        float[] key = new float[queryVector.size()];
        for (int i = 0; i < key.length; i++) {
            key[i] = queryVector.get(i).floatValue();
        }
        return key;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LanceVectorSearch that = (LanceVectorSearch) o;
        return k == that.k &&
                requiresGlobalTopK == that.requiresGlobalTopK &&
                Objects.equals(column, that.column) &&
                Objects.equals(queryVector, that.queryVector) &&
                Objects.equals(metric, that.metric) &&
                Objects.equals(nprobes, that.nprobes) &&
                Objects.equals(ef, that.ef) &&
                Objects.equals(refineFactor, that.refineFactor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, queryVector, k, metric, nprobes, ef, refineFactor, requiresGlobalTopK);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("k", k)
                .add("metric", metric)
                .add("dimension", queryVector.size())
                .add("nprobes", nprobes)
                .add("ef", ef)
                .add("refineFactor", refineFactor)
                .add("requiresGlobalTopK", requiresGlobalTopK)
                .toString();
    }
}
