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
package com.facebook.presto.delta;

import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Parses the {@code add.stats} JSON field from the Delta transaction log for a single data file.
 * Keys in minValues/maxValues/nullCount are physical column names for column-mapping tables.
 * All lookups are case-insensitive.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeltaJsonFileStatistics
        implements DeltaFileStatistics
{
    private static final Logger log = Logger.get(DeltaJsonFileStatistics.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread-safe after construction

    private final Long numRecords;
    private final Map<String, Object> minValues;
    private final Map<String, Object> maxValues;
    private final Map<String, Object> nullCount;

    /** Parses a stats JSON string. Returns empty on null, blank, or malformed input — never throws. */
    public static Optional<DeltaJsonFileStatistics> create(String statsJson)
    {
        if (isNullOrEmpty(statsJson) || statsJson.trim().equals("null")) {
            return Optional.empty();
        }
        try {
            DeltaJsonFileStatistics parsed = OBJECT_MAPPER.readValue(statsJson, DeltaJsonFileStatistics.class);
            if (parsed == null) {
                return Optional.empty();
            }
            return Optional.of(parsed);
        }
        catch (JsonProcessingException e) {
            log.debug("Cannot parse Delta file statistics JSON, skipping statistics: %s", e.getMessage());
            return Optional.empty();
        }
    }

    @JsonCreator
    public DeltaJsonFileStatistics(
            @JsonProperty("numRecords") Long numRecords,
            @JsonProperty("minValues") Map<String, Object> minValues,
            @JsonProperty("maxValues") Map<String, Object> maxValues,
            @JsonProperty("nullCount") Map<String, Object> nullCount)
    {
        this.numRecords = numRecords;
        this.minValues = toLowerCaseMap(minValues);
        this.maxValues = toLowerCaseMap(maxValues);
        this.nullCount = toLowerCaseMap(nullCount);
    }

    private static Map<String, Object> toLowerCaseMap(Map<String, Object> map)
    {
        if (map == null) {
            return null;
        }
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                builder.put(entry.getKey().toLowerCase(Locale.ENGLISH), entry.getValue());
            }
        }
        return builder.build();
    }

    @Override
    public Optional<Long> getNumRecords()
    {
        return Optional.ofNullable(numRecords);
    }

    @Override
    public Optional<Long> getNullCount(String physicalColumnName)
    {
        return getStat(physicalColumnName, nullCount).map(o -> {
            if (o instanceof Number) {
                return ((Number) o).longValue();
            }
            try {
                return Long.parseLong(o.toString());
            }
            catch (NumberFormatException e) {
                log.debug("Cannot parse nullCount value '%s' for column '%s', treating as unknown", o, physicalColumnName);
                return null;
            }
        }).filter(Objects::nonNull);
    }

    @Override
    public Optional<Object> getMinColumnValue(DeltaColumnHandle columnHandle)
    {
        String physicalName = columnHandle.getPhysicalName() != null
                ? columnHandle.getPhysicalName()
                : columnHandle.getLogicalName();
        return getStat(physicalName, minValues);
    }

    @Override
    public Optional<Object> getMaxColumnValue(DeltaColumnHandle columnHandle)
    {
        String physicalName = columnHandle.getPhysicalName() != null
                ? columnHandle.getPhysicalName()
                : columnHandle.getLogicalName();
        return getStat(physicalName, maxValues);
    }

    private Optional<Object> getStat(String columnName, Map<String, Object> stats)
    {
        if (stats == null || columnName == null) {
            return Optional.empty();
        }
        Object value = stats.get(columnName.toLowerCase(Locale.ENGLISH));
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof List || value instanceof Map) {
            log.debug("Skipping statistics for column '%s': complex value type", columnName);
            return Optional.empty();
        }
        return Optional.of(value);
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
        DeltaJsonFileStatistics that = (DeltaJsonFileStatistics) o;
        return Objects.equals(numRecords, that.numRecords) &&
                Objects.equals(minValues, that.minValues) &&
                Objects.equals(maxValues, that.maxValues) &&
                Objects.equals(nullCount, that.nullCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(numRecords, minValues, maxValues, nullCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numRecords", numRecords)
                .add("minValues", minValues)
                .add("maxValues", maxValues)
                .add("nullCount", nullCount)
                .toString();
    }
}
