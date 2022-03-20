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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.ORDER_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.PARTITION_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.PRIMARY_KEY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.SAMPLE_BY_PROPERTY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ClickHouseTableProperties
        implements TablePropertiesProvider
{
    public static final String ENGINE_PROPERTY = "engine";

    public static final ClickHouseEngineType DEFAULT_TABLE_ENGINE = ClickHouseEngineType.LOG;

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ClickHouseTableProperties()
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        ENGINE_PROPERTY,
                        "ClickHouse Table Engine, defaults to Log",
                        ClickHouseEngineType.class,
                        DEFAULT_TABLE_ENGINE,
                        false),
                new PropertyMetadata<>(
                        ORDER_BY_PROPERTY,
                        "columns to be the sorting key, it's required for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        PARTITION_BY_PROPERTY,
                        "columns to be the partition key. it's optional for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        PRIMARY_KEY_PROPERTY,
                        "columns to be the primary key. it's optional for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                stringProperty(
                        SAMPLE_BY_PROPERTY,
                        "An expression for sampling. it's optional for table MergeTree engine family",
                        null,
                        false));
    }

    public static ClickHouseEngineType getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (ClickHouseEngineType) tableProperties.get(ENGINE_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getOrderBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(ORDER_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PARTITION_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY);
    }

    public static Optional<String> getSampleBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");

        return Optional.ofNullable(tableProperties.get(SAMPLE_BY_PROPERTY)).map(String.class::cast);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, boolean hidden)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, value -> {}, hidden);
    }

    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, Consumer<T> validation, boolean hidden)
    {
        String allValues = EnumSet.allOf(type).stream()
                .map(Enum::name)
                .collect(joining(", ", "[", "]"));
        return new PropertyMetadata<>(
                name,
                format("%s. Possible values: %s", descriptionPrefix, allValues),
                VARCHAR,
                type,
                defaultValue,
                hidden,
                value -> {
                    T enumValue;
                    try {
                        enumValue = Enum.valueOf(type, ((String) value).toUpperCase(ENGLISH));
                    }
                    catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(format("Invalid value [%s]. Valid values: %s", value, allValues), e);
                    }
                    validation.accept(enumValue);
                    return enumValue;
                },
                Enum::name);
    }
    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, boolean hidden)
    {
        return stringProperty(name, description, defaultValue, value -> {}, hidden);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, Consumer<String> validation, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                String.class,
                defaultValue,
                hidden,
                object -> {
                    String value = (String) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }
}
