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
package com.facebook.presto.ducklake.split.pruning;

import com.google.common.collect.ImmutableSet;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Parses one {@code ducklake_partition_column.transform} string (spec &sect;4.5) and knows which
 * DuckLake source types each kind can be pruned on, and how to evaluate the calendar kinds.
 *
 * <p>DuckDB's calendar functions ({@code year}, {@code month}, {@code day}, {@code hour}) operate
 * on the naive calendar fields of the value, so they are only prunable on {@code date} and the
 * zoneless {@code timestamp}/{@code timestamp_s}/{@code timestamp_ms}/{@code timestamp_ns} types;
 * a {@code timestamptz} source is evaluated by DuckDB in the session time zone, which this
 * connector cannot know, so it is never prunable. {@code bucket(N)} is never prunable in v1
 * (spec &sect;4.5). The {@code epoch_year}/{@code epoch_month}/{@code epoch_day}/{@code
 * epoch_hour} transforms are Iceberg-style ordinals ({@code date_diff} from 1970-01-01) and are
 * likewise never prunable in v1; unlike the calendar kinds they are monotonic, so a future version
 * could prune them the same way {@code year} is pruned here.
 */
public final class PartitionTransform
{
    private static final Set<String> IDENTITY_PRUNABLE_TYPES = ImmutableSet.of(
            "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "varchar", "date", "boolean");
    private static final Set<String> CALENDAR_PRUNABLE_TYPES = ImmutableSet.of(
            "date", "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns");
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\s*(?:\\(\\s*(\\d+)\\s*\\))?");

    public enum Kind
    {
        IDENTITY, YEAR, MONTH, DAY, HOUR, BUCKET, EPOCH, UNKNOWN
    }

    private final Kind kind;
    private final OptionalInt bucketCount;

    private PartitionTransform(Kind kind, OptionalInt bucketCount)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    /**
     * Parses a raw {@code ducklake_partition_column.transform} string. Case-insensitive, and
     * tolerant of surrounding whitespace. Anything not recognised (including a malformed {@code
     * bucket(...)}) parses to {@link Kind#UNKNOWN} rather than failing, since an unrecognised
     * transform must simply be treated as not prunable (spec &sect;4.5), never as an error.
     */
    public static PartitionTransform parse(String transform)
    {
        requireNonNull(transform, "transform is null");
        String normalized = transform.trim().toLowerCase(Locale.ENGLISH);
        switch (normalized) {
            case "identity":
                return new PartitionTransform(Kind.IDENTITY, OptionalInt.empty());
            case "year":
                return new PartitionTransform(Kind.YEAR, OptionalInt.empty());
            case "month":
                return new PartitionTransform(Kind.MONTH, OptionalInt.empty());
            case "day":
                return new PartitionTransform(Kind.DAY, OptionalInt.empty());
            case "hour":
                return new PartitionTransform(Kind.HOUR, OptionalInt.empty());
            case "epoch_year":
            case "epoch_month":
            case "epoch_day":
            case "epoch_hour":
                return new PartitionTransform(Kind.EPOCH, OptionalInt.empty());
            default:
                Matcher bucketMatcher = BUCKET_PATTERN.matcher(normalized);
                if (bucketMatcher.matches()) {
                    OptionalInt bucketCount = bucketMatcher.group(1) == null
                            ? OptionalInt.empty()
                            : OptionalInt.of(Integer.parseInt(bucketMatcher.group(1)));
                    return new PartitionTransform(Kind.BUCKET, bucketCount);
                }
                return new PartitionTransform(Kind.UNKNOWN, OptionalInt.empty());
        }
    }

    public Kind getKind()
    {
        return kind;
    }

    /**
     * The {@code N} captured from {@code bucket(N)}, or empty for a bare {@code bucket} or any
     * non-bucket kind.
     */
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    /**
     * Whether this transform can be used to prune files whose partitioned source column has the
     * given raw DuckLake type (for example {@code int32}, {@code timestamp_ns}).
     */
    public boolean isPrunable(String sourceDuckLakeType)
    {
        requireNonNull(sourceDuckLakeType, "sourceDuckLakeType is null");
        String normalizedType = sourceDuckLakeType.trim().toLowerCase(Locale.ENGLISH);
        switch (kind) {
            case IDENTITY:
                return IDENTITY_PRUNABLE_TYPES.contains(normalizedType);
            case YEAR:
            case MONTH:
            case DAY:
            case HOUR:
                return CALENDAR_PRUNABLE_TYPES.contains(normalizedType);
            case BUCKET:
            case EPOCH:
            case UNKNOWN:
            default:
                return false;
        }
    }

    /**
     * Evaluates one of the four calendar kinds ({@code year}/{@code month}/{@code day}/{@code
     * hour}) on the naive local value. A {@code date} source must be converted with {@code
     * LocalDate.atStartOfDay()} first, matching DuckDB evaluating {@code hour(date_value)} as 0.
     *
     * @throws IllegalStateException if this transform is not one of the four calendar kinds
     */
    public long apply(LocalDateTime value)
    {
        requireNonNull(value, "value is null");
        switch (kind) {
            case YEAR:
                return value.getYear();
            case MONTH:
                return value.getMonthValue();
            case DAY:
                return value.getDayOfMonth();
            case HOUR:
                return value.getHour();
            default:
                throw new IllegalStateException("apply() only supports calendar transforms, was " + kind);
        }
    }

    @Override
    public String toString()
    {
        return bucketCount.isPresent() ? kind + "(" + bucketCount.getAsInt() + ")" : kind.toString();
    }
}
