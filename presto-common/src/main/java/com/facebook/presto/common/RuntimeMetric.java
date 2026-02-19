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
package com.facebook.presto.common;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.Objects.requireNonNull;

/**
 * A metric exposed by a presto operator or connector. It will be aggregated at the query level.
 * Optionally supports percentile tracking (p90, p95, p99) when explicitly enabled.
 */
@ThriftStruct
public class RuntimeMetric
{
    /*
     * Percentile Calculation Algorithm: Fixed-Width Histogram with Auto-Configuration
     *
     * This implementation uses a fixed-width histogram to approximate percentiles with bounded memory.
     * Both the number of buckets and bucket width can be configured, with auto-configured defaults.
     *
     * Auto-Configuration Strategy (with default 1000 buckets):
     * - NANO (latency): 1000 buckets × 1ms (1000μs) = 0-1 second range
     * - BYTE (data size): 1000 buckets × 1KB (1024 bytes) = 0-1MB range
     * - NONE (counts): 1000 buckets × 1000 = 0-1 million range
     *
     * Algorithm Overview:
     * 1. Divide the value range into numBuckets fixed-width bins
     * 2. Each value is mapped to a bucket: bucketIndex = value / bucketWidth
     * 3. To compute percentile P (e.g., 0.90 for p90):
     *    - Calculate target count: targetCount = ceil(P × totalCount)
     *    - Iterate through buckets, accumulating counts
     *    - When accumulated count >= targetCount, return bucket midpoint
     *
     * Example: For 100 latency values with p90 = 0.90:
     *   - targetCount = ceil(0.90 × 100) = 90
     *   - If buckets [0..89] have 85 values and bucket 90 has 10 values
     *   - Accumulated count reaches 95 at bucket 90, so return bucket 90's midpoint
     *
     * Accuracy: Within ±(bucketWidth/2) of true percentile
     *   - For NANO: ±0.5ms precision (default)
     *   - For BYTE: ±512 bytes precision (default)
     *   - For NONE: ±500 units precision (default)
     *
     * Memory: O(numBuckets) = 8KB (default 1000 buckets × 8 bytes per AtomicLong)
     *
     * Configurability:
     *   ✓ Number of buckets is configurable (default 1000)
     *   ✓ Bucket width is configurable (auto-determined by RuntimeUnit if not specified)
     *
     * References:
     *   - Prometheus Histogram: https://prometheus.io/docs/concepts/metric_types/#histogram
     *   - HdrHistogram paper: https://www.azul.com/files/HdrHistogram.pdf
     *
     * Trade-offs:
     *   ✓ Bounded memory (O(1) space complexity)
     *   ✓ Fast updates (O(1) atomic increment)
     *   ✓ Mergeable across distributed nodes (histogram addition)
     *   ✓ Auto-configured for different metric types
     *   ✓ Configurable number of buckets and bucket width for precision tuning
     *   ✗ Approximate results (accuracy = bucket width)
     *   ✗ Fixed range (values beyond range go to overflow bucket)
     */
    private static final int DEFAULT_NUM_BUCKETS = 1000;

    // Number of histogram buckets (configurable, defaults to 1000)
    private final int numBuckets;

    // Bucket width is determined by the RuntimeUnit or explicitly configured
    private final long bucketWidth;

    private final String name;
    private final RuntimeUnit unit;
    private final AtomicLong sum = new AtomicLong();
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);

    // Optional percentile tracking - only allocated when percentileTrackingEnabled is true
    private volatile boolean percentileTrackingEnabled;
    private volatile AtomicLongArray histogramBuckets;

    // Cached percentile values (computed and serialized to JSON, histogram is NOT serialized)
    private volatile Long p90;
    private volatile Long p95;
    private volatile Long p99;

    /**
     * Creates a new empty RuntimeMetric without percentile tracking.
     *
     * @param name Name of this metric. If used in the presto core code base, this should be a value defined in {@link RuntimeMetricName}. But connectors could use arbitrary names.
     * @param unit Unit of this metric. Available units are defined in {@link RuntimeUnit}.
     */
    public RuntimeMetric(String name, RuntimeUnit unit)
    {
        this(name, unit, false);
    }

    /**
     * Creates a new empty RuntimeMetric with optional percentile tracking.
     *
     * @param name Name of this metric.
     * @param unit Unit of this metric.
     * @param trackPercentiles If true, enables percentile tracking (p90, p95, p99) for all addValue calls. Allocates ~8KB histogram with default 1000 buckets.
     */
    public RuntimeMetric(String name, RuntimeUnit unit, boolean trackPercentiles)
    {
        this(name, unit, trackPercentiles, -1, -1);
    }

    /**
     * Creates a new empty RuntimeMetric with optional percentile tracking and custom bucket width.
     * Use this constructor when you need finer or coarser granularity than the auto-configured defaults.
     *
     * @param name Name of this metric.
     * @param unit Unit of this metric.
     * @param trackPercentiles If true, enables percentile tracking (p90, p95, p99) for all addValue calls. Allocates histogram.
     * @param bucketWidth Custom bucket width for histogram. If <= 0, uses auto-configured value based on unit type.
     * Examples:
     * - For sub-millisecond latency: 100 (100μs per bucket, ±50μs precision)
     * - For multi-second latency: 10000 (10ms per bucket, ±5ms precision)
     * - For small byte counts: 100 (100 bytes per bucket, ±50 bytes precision)
     * - For large row counts: 10000 (10k per bucket, ±5k precision)
     */
    public RuntimeMetric(String name, RuntimeUnit unit, boolean trackPercentiles, long bucketWidth)
    {
        this(name, unit, trackPercentiles, bucketWidth, -1);
    }

    /**
     * Creates a new empty RuntimeMetric with optional percentile tracking, custom bucket width, and custom number of buckets.
     * Use this constructor when you need complete control over histogram configuration.
     *
     * @param name Name of this metric.
     * @param unit Unit of this metric.
     * @param trackPercentiles If true, enables percentile tracking (p90, p95, p99) for all addValue calls. Allocates histogram.
     * @param bucketWidth Custom bucket width for histogram. If <= 0, uses auto-configured value based on unit type.
     * Examples:
     * - For sub-millisecond latency: 100 (100μs per bucket, ±50μs precision)
     * - For multi-second latency: 10000 (10ms per bucket, ±5ms precision)
     * - For small byte counts: 100 (100 bytes per bucket, ±50 bytes precision)
     * - For large row counts: 10000 (10k per bucket, ±5k precision)
     * @param numBuckets Number of buckets for histogram. If <= 0, uses default (1000).
     * Memory usage: numBuckets × 8 bytes (e.g., 1000 buckets = 8KB)
     * Examples:
     * - For high precision: 10000 buckets (80KB memory)
     * - For low memory: 100 buckets (800 bytes memory)
     * - Default: 1000 buckets (8KB memory)
     */
    public RuntimeMetric(String name, RuntimeUnit unit, boolean trackPercentiles, long bucketWidth, int numBuckets)
    {
        this.name = requireNonNull(name, "name is null");
        this.unit = unit == null ? NONE : unit;
        this.numBuckets = (numBuckets > 0) ? numBuckets : DEFAULT_NUM_BUCKETS;
        this.bucketWidth = (bucketWidth > 0) ? bucketWidth : determineBucketWidth(this.unit);
        this.percentileTrackingEnabled = trackPercentiles;
        if (trackPercentiles) {
            this.histogramBuckets = new AtomicLongArray(this.numBuckets);
        }
    }

    /**
     * Determines the appropriate bucket width based on the metric's unit type.
     * This auto-configures the histogram for optimal accuracy based on expected value ranges.
     * <p>
     * Auto-Configuration Strategy (with default 1000 buckets):
     * - NANO: 1ms per bucket for typical latencies (0-1 second range, ±0.5ms precision)
     * - BYTE: 1KB per bucket for typical data sizes (0-1MB range, ±512 bytes precision)
     * - NONE: 1000 per bucket for typical counts (0-1 million range, ±500 units precision)
     * <p>
     * Override with custom bucket width and/or numBuckets in constructor for:
     * - Sub-millisecond precision: Use 100,000 ns per bucket (100μs, 0-100ms range with 1000 buckets, ±50μs precision)
     * - Multi-second latencies: Use 10,000,000 ns per bucket (10ms, 0-10s range with 1000 buckets, ±5ms precision)
     * - Small byte counts: Use 100 bytes per bucket (0-100KB range with 1000 buckets, ±50 bytes precision)
     * - Large counts: Use 10,000 per bucket (0-10M range with 1000 buckets, ±5k precision)
     * - High precision: Use more buckets (e.g., 10000 buckets for finer granularity)
     * - Low memory: Use fewer buckets (e.g., 100 buckets to reduce memory footprint)
     *
     * @param unit The RuntimeUnit for this metric
     * @return The bucket width in the unit's native scale
     */
    private static long determineBucketWidth(RuntimeUnit unit)
    {
        switch (unit) {
            case NANO:
                // For nanosecond timing metrics (latency, CPU time, etc.)
                // Default: 1000 buckets × 1,000,000 ns (1ms) = 0-1 second range
                // Precision: ±0.5ms
                // Override bucketWidth and/or numBuckets for different precision or range
                return 1_000_000; // 1 millisecond in nanoseconds
            case BYTE:
                // For byte count metrics (memory, network, disk I/O)
                // Default: 1000 buckets × 1024 bytes (1KB) = 0-1MB range
                // Precision: ±512 bytes
                // Override bucketWidth and/or numBuckets for different precision or range
                return 1024;
            case NONE:
                // For dimensionless counts (rows, operations, events)
                // Default: 1000 buckets × 1000 = 0-1 million range
                // Precision: ±500 units
                // Override bucketWidth and/or numBuckets for different precision or range
                return 1000;
            default:
                // Fallback for any future unit types
                return 1000;
        }
    }

    public static RuntimeMetric copyOf(RuntimeMetric metric)
    {
        return copyOf(metric, metric.getName());
    }

    public static RuntimeMetric copyOf(RuntimeMetric metric, String newName)
    {
        requireNonNull(metric, "metric is null");
        requireNonNull(newName, "newName is null");
        RuntimeMetric copy = new RuntimeMetric(newName, metric.getUnit(),
                metric.percentileTrackingEnabled, metric.bucketWidth, metric.numBuckets);
        copy.set(metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin());
        if (metric.histogramBuckets != null) {
            for (int i = 0; i < metric.numBuckets; i++) {
                copy.histogramBuckets.set(i, metric.histogramBuckets.get(i));
            }
        }
        copy.p90 = metric.p90;
        copy.p95 = metric.p95;
        copy.p99 = metric.p99;
        return copy;
    }

    @ThriftConstructor
    public RuntimeMetric(String name, RuntimeUnit unit, long sum, long count, long max, long min)
    {
        this.name = requireNonNull(name, "name is null");
        this.unit = unit == null ? NONE : unit;
        this.numBuckets = DEFAULT_NUM_BUCKETS;
        this.bucketWidth = determineBucketWidth(this.unit);
        set(sum, count, max, min);
        // No percentile tracking for this constructor (backward compatibility)
        this.percentileTrackingEnabled = false;
    }

    @JsonCreator
    public RuntimeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("unit") RuntimeUnit unit,
            @JsonProperty("sum") long sum,
            @JsonProperty("count") long count,
            @JsonProperty("max") long max,
            @JsonProperty("min") long min,
            @JsonProperty("numBuckets") Integer numBuckets,
            @JsonProperty("bucketWidth") Long bucketWidth,
            @JsonProperty("p90") Long p90,
            @JsonProperty("p95") Long p95,
            @JsonProperty("p99") Long p99)
    {
        this.name = requireNonNull(name, "name is null");
        this.unit = unit == null ? NONE : unit;
        // Use provided numBuckets or default if null (for backward compatibility with old JSON)
        this.numBuckets = (numBuckets != null && numBuckets > 0) ? numBuckets : DEFAULT_NUM_BUCKETS;
        // Use provided bucketWidth or auto-configured value if null (for backward compatibility)
        this.bucketWidth = (bucketWidth != null && bucketWidth > 0) ? bucketWidth : determineBucketWidth(this.unit);
        set(sum, count, max, min);

        // Deserialized metrics provide read-only percentile snapshots
        this.p90 = p90;
        this.p95 = p95;
        this.p99 = p99;
        this.percentileTrackingEnabled = false;
    }

    private void set(long sum, long count, long max, long min)
    {
        this.sum.set(sum);
        this.count.set(count);
        this.max.set(max);
        this.min.set(min);
    }

    public void set(RuntimeMetric metric)
    {
        requireNonNull(metric, "metric is null");
        checkState(unit == metric.getUnit(), "The metric must have the same unit type as the current one.");
        checkState(bucketWidth == metric.bucketWidth, "The metric to be merged must have the same bucket width as the current one.");
        checkState(numBuckets == metric.numBuckets, "The metric to be merged must have the same bucket number as the current one.");

        set(metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin());

        // Copy percentile tracking state
        this.percentileTrackingEnabled = metric.percentileTrackingEnabled;
        if (metric.histogramBuckets != null) {
            if (this.histogramBuckets == null) {
                this.histogramBuckets = new AtomicLongArray(this.numBuckets);
            }
            for (int i = 0; i < this.numBuckets; i++) {
                this.histogramBuckets.set(i, metric.histogramBuckets.get(i));
            }
        }
        this.p90 = metric.p90;
        this.p95 = metric.p95;
        this.p99 = metric.p99;
    }

    /**
     * Check if percentile tracking is enabled for this metric.
     */
    public boolean isPercentileTrackingEnabled()
    {
        return percentileTrackingEnabled;
    }

    @JsonProperty
    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    /**
     * Add a value to this metric.
     * If percentile tracking is enabled (via constructor), the value is added to the histogram.
     *
     * @param value The value to add
     */
    public void addValue(long value)
    {
        sum.addAndGet(value);
        count.incrementAndGet();
        max.accumulateAndGet(value, Math::max);
        min.accumulateAndGet(value, Math::min);

        // Update histogram if percentile tracking is enabled
        if (percentileTrackingEnabled && histogramBuckets != null) {
            int bucketIndex = getBucketIndex(value);
            histogramBuckets.incrementAndGet(bucketIndex);
        }
    }

    /**
     * Maps a value to its histogram bucket index.
     * <p>
     * Algorithm: bucketIndex = floor(value / bucketWidth)
     * <p>
     * Examples with auto-configured bucket widths and default 1000 buckets:
     * <p>
     * For NANO (latency) with bucketWidth = 1000μs:
     * - value = 500μs   → bucket 0  (0-999μs)
     * - value = 1500μs  → bucket 1  (1000-1999μs)
     * - value = 999000μs → bucket 999 (999000-999999μs, ~1 second)
     * - value = 2000000μs → bucket 999 (overflow, clamped to last bucket)
     * <p>
     * For BYTE (data size) with bucketWidth = 1024 bytes:
     * - value = 512 bytes → bucket 0 (0-1023 bytes)
     * - value = 2048 bytes → bucket 2 (2048-3071 bytes, ~2KB)
     * - value = 1023KB → bucket 999 (1022KB-1023KB, ~1MB max)
     * <p>
     * For NONE (counts) with bucketWidth = 1000:
     * - value = 500 → bucket 0 (0-999)
     * - value = 50000 → bucket 50 (50000-50999)
     * - value = 999000 → bucket 999 (999000-999999, ~1 million max)
     * <p>
     * Overflow handling: Values beyond (numBuckets × bucketWidth) go to the last bucket.
     * This means very large outliers are grouped together but still counted.
     */
    private int getBucketIndex(long value)
    {
        if (value < 0) {
            return 0;
        }
        long bucketIndex = value / bucketWidth;
        if (bucketIndex >= numBuckets) {
            return numBuckets - 1; // Overflow bucket for very large values
        }
        return (int) bucketIndex;
    }

    /**
     * Merges {@code metric1} and {@code metric2} and returns the result. The input parameters are not updated.
     */
    public static RuntimeMetric merge(RuntimeMetric metric1, RuntimeMetric metric2)
    {
        if (metric1 == null) {
            return metric2;
        }
        if (metric2 == null) {
            return metric1;
        }
        checkState(metric1.getUnit() == metric2.getUnit(), "Two metrics to be merged must have the same unit type.");

        RuntimeMetric mergedMetric = copyOf(metric1);
        mergedMetric.mergeWith(metric2);
        return mergedMetric;
    }

    /**
     * Merges {@code metric} into this object.
     */
    public void mergeWith(RuntimeMetric metric)
    {
        if (metric == null) {
            return;
        }
        checkState(unit == metric.getUnit(), "The metric to be merged must have the same unit type as the current one.");
        sum.addAndGet(metric.getSum());
        count.addAndGet(metric.getCount());
        max.accumulateAndGet(metric.getMax(), Math::max);
        min.accumulateAndGet(metric.getMin(), Math::min);

        // Merge histogram data if both have percentile tracking enabled
        if (percentileTrackingEnabled && metric.percentileTrackingEnabled &&
                histogramBuckets != null && metric.histogramBuckets != null) {
            // Validate that both metrics have the same bucket configuration
            checkState(bucketWidth == metric.bucketWidth, "The metric to be merged must have the same bucket width as the current one.");
            checkState(numBuckets == metric.numBuckets, "The metric to be merged must have the same bucket number as the current one.");

            for (int i = 0; i < numBuckets; i++) {
                histogramBuckets.addAndGet(i, metric.histogramBuckets.get(i));
            }
        }
        // Invalidate cached percentiles after merge
        p90 = null;
        p95 = null;
        p99 = null;
    }

    @JsonProperty
    @ThriftField(2)
    public long getSum()
    {
        return sum.get();
    }

    @JsonProperty
    @ThriftField(3)
    public long getCount()
    {
        return count.get();
    }

    @JsonProperty
    @ThriftField(4)
    public long getMax()
    {
        return max.get();
    }

    @JsonProperty
    @ThriftField(5)
    public long getMin()
    {
        return min.get();
    }

    @JsonProperty
    @ThriftField(6)
    public RuntimeUnit getUnit()
    {
        return unit;
    }

    /**
     * Get the number of histogram buckets configured for this metric.
     * Only relevant when percentile tracking is enabled.
     * Note: This is only available via JSON, not Thrift.
     *
     * @return The number of buckets, or default value if not explicitly configured
     */
    @JsonProperty
    public Integer getNumBuckets()
    {
        if (!percentileTrackingEnabled) {
            return null;
        }
        return numBuckets;
    }

    /**
     * Get the bucket width (in native units) configured for this metric.
     * Only relevant when percentile tracking is enabled.
     * Note: This is only available via JSON, not Thrift.
     *
     * @return The bucket width in the metric's native unit scale
     */
    @JsonProperty
    public Long getBucketWidth()
    {
        if (!percentileTrackingEnabled) {
            return null;
        }
        return bucketWidth;
    }

    /**
     * Get the 90th percentile (null if percentile tracking not enabled).
     * Note: This is only available via JSON, not Thrift.
     * For JSON-deserialized metrics, returns the cached snapshot value.
     */
    @JsonProperty
    public Long getP90()
    {
        if (!percentileTrackingEnabled) {
            // For JSON-deserialized metrics, return the cached read-only snapshot
            return p90;
        }
        // For live tracking, compute and cache
        if (p90 == null) {
            long computed = computePercentile(0.90);
            p90 = computed >= 0 ? computed : null;
        }
        return p90;
    }

    /**
     * Get the 95th percentile (null if percentile tracking not enabled).
     * Note: This is only available via JSON, not Thrift.
     * For JSON-deserialized metrics, returns the cached snapshot value.
     */
    @JsonProperty
    public Long getP95()
    {
        if (!percentileTrackingEnabled) {
            // For JSON-deserialized metrics, return the cached read-only snapshot
            return p95;
        }
        // For live tracking, compute and cache
        if (p95 == null) {
            long computed = computePercentile(0.95);
            p95 = computed >= 0 ? computed : null;
        }
        return p95;
    }

    /**
     * Get the 99th percentile (null if percentile tracking not enabled).
     * Note: This is only available via JSON, not Thrift.
     * For JSON-deserialized metrics, returns the cached snapshot value.
     */
    @JsonProperty
    public Long getP99()
    {
        if (!percentileTrackingEnabled) {
            // For JSON-deserialized metrics, return the cached read-only snapshot
            return p99;
        }
        // For live tracking, compute and cache
        if (p99 == null) {
            long computed = computePercentile(0.99);
            p99 = computed >= 0 ? computed : null;
        }
        return p99;
    }

    /**
     * Calculate percentile value from histogram using cumulative distribution.
     * <p>
     * Algorithm (based on "Nearest Rank" method):
     * 1. targetCount = ceil(percentile × totalCount)
     * 2. Iterate through buckets, accumulating counts
     * 3. When accumulated >= targetCount, return bucket midpoint
     * <p>
     * Example: p90 with 100 values
     * - targetCount = ceil(0.90 × 100) = 90
     * - If bucket distribution is:
     * bucket[0..88]: 89 values (accumulated = 89)
     * bucket[89]:    5 values  (accumulated = 94) ← 94 >= 90, so return this bucket
     * - Return: bucket[89] midpoint = 89 × 1000 + 500 = 89,500μs
     * <p>
     * Why return bucket midpoint?
     * - We don't know exact value distribution within the bucket
     * - Midpoint is the unbiased estimator (minimizes expected error)
     * - Alternative: bucket start (pessimistic) or bucket end (optimistic)
     * <p>
     * Accuracy Analysis:
     * - True percentile is somewhere in the bucket: [i×bucketWidth, (i+1)×bucketWidth)
     * - Midpoint: i×bucketWidth + bucketWidth/2
     * - Maximum error: ±bucketWidth/2
     * - For NANO (1ms buckets): ±0.5ms error
     * - For BYTE (1KB buckets): ±512 bytes error
     * - For NONE (1000 unit buckets): ±500 units error
     * <p>
     * Why ceil() for targetCount?
     * - ceil(0.90 × 100) = 90 means "at least 90 values must be <= p90"
     * - This matches Excel PERCENTILE.INC function behavior
     * - Alternative: floor() would give PERCENTILE.EXC behavior
     * <p>
     * Auto-Configuration:
     * - Bucket width automatically determined by RuntimeUnit
     * - Ensures appropriate precision for different metric types
     * - No configuration needed from caller
     * <p>
     * References:
     * - NIST: https://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
     * - Numpy percentile: https://numpy.org/doc/stable/reference/generated/numpy.percentile.html
     *
     * @param percentile The percentile to calculate (0.0 to 1.0, e.g., 0.90 for p90)
     * @return The approximate percentile value, or -1 if tracking is disabled
     */
    private long computePercentile(double percentile)
    {
        if (!percentileTrackingEnabled || histogramBuckets == null) {
            return -1;
        }

        if (percentile < 0.0 || percentile > 1.0) {
            throw new IllegalArgumentException("Percentile must be between 0.0 and 1.0, got: " + percentile);
        }

        long totalCount = count.get();
        if (totalCount == 0) {
            return 0;
        }

        // Find the target count for the percentile (using "Nearest Rank" method)
        long targetCount = (long) Math.ceil(percentile * totalCount);

        // Accumulate counts through buckets - no synchronization needed with AtomicLongArray
        long accumulatedCount = 0;
        for (int i = 0; i < numBuckets; i++) {
            accumulatedCount += histogramBuckets.get(i);
            if (accumulatedCount >= targetCount) {
                // Calculate the midpoint of the bucket as the percentile estimate
                long percentileValue = i * bucketWidth + bucketWidth / 2;

                // Clamp to [min, max] range to ensure accuracy with small samples or large bucket widths
                // Example: If only value 100,000 is in bucket 0 (0-999,999), midpoint 500,000 > max 100,000
                // Clamping ensures the percentile never exceeds actual data bounds
                return Math.max(min.get(), Math.min(percentileValue, max.get()));
            }
        }

        // If we reach here (all buckets processed), return the max value
        // Race condition: Even with thread-safe AtomicLongArray, this can happen due to update ordering.
        // In addValue(), count is incremented BEFORE the histogram bucket is updated. This creates
        // a tiny window where count.get() returns N, but histogram only contains N-1 values.
        // Example: Thread A reads totalCount=100, Thread B increments count to 101 but hasn't yet
        // updated histogram, Thread A iterates and only finds 100 values, missing targetCount=99.
        // Returning max.get() is a safe approximation for high percentiles in this rare edge case.
        return max.get();
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("RuntimeMetric{");
        sb.append("name='").append(name).append('\'');
        sb.append(", unit=").append(unit);
        sb.append(", count=").append(count.get());
        sb.append(", sum=").append(sum.get());
        sb.append(", min=").append(min.get());
        sb.append(", max=").append(max.get());

        // Check if we have percentile values (either from live tracking or JSON deserialization)
        Long p90Value = this.p90;
        Long p95Value = this.p95;
        Long p99Value = this.p99;
        boolean hasPercentileValues = (p90Value != null || p95Value != null || p99Value != null);

        if (percentileTrackingEnabled) {
            sb.append(", percentileTracking=enabled");
            sb.append(", numBuckets=").append(numBuckets);
            sb.append(", bucketWidth=").append(bucketWidth);
        }
        else if (hasPercentileValues) {
            // Read-only percentile snapshot (from JSON deserialization)
            sb.append(", percentileTracking=disabled(read-only snapshot)");
            sb.append(", numBuckets=").append(numBuckets);
            sb.append(", bucketWidth=").append(bucketWidth);
        }
        else {
            sb.append(", percentileTracking=disabled");
        }

        // Show percentile values if available (either from live tracking or JSON)
        if (hasPercentileValues) {
            sb.append(", percentiles={");
            if (p90Value != null) {
                sb.append("p90=").append(p90Value);
            }
            if (p95Value != null) {
                if (p90Value != null) {
                    sb.append(", ");
                }
                sb.append("p95=").append(p95Value);
            }
            if (p99Value != null) {
                if (p90Value != null || p95Value != null) {
                    sb.append(", ");
                }
                sb.append("p99=").append(p99Value);
            }
            sb.append('}');
        }

        sb.append('}');
        return sb.toString();
    }
}
