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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.raptorx.util.PropertyUtil.lowerCaseStringListProperty;
import static com.facebook.presto.raptorx.util.PropertyUtil.lowerCaseStringProperty;
import static com.facebook.presto.raptorx.util.PropertyUtil.stringList;
import static com.facebook.presto.raptorx.util.PropertyUtil.upperCaseEnumProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public class RaptorTableProperties
{
    public static final String COMPRESSION_TYPE_PROPERTY = "compression_type";
    public static final String ORDERING_PROPERTY = "ordering";
    public static final String TEMPORAL_COLUMN_PROPERTY = "temporal_column";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";
    public static final String BUCKETED_ON_PROPERTY = "bucketed_on";
    public static final String DISTRIBUTION_NAME_PROPERTY = "distribution_name";
    public static final String ORGANIZED_PROPERTY = "organized";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public RaptorTableProperties(TypeManager typeManager)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(upperCaseEnumProperty(
                        CompressionType.class,
                        COMPRESSION_TYPE_PROPERTY,
                        "Type of compression to use for table data",
                        CompressionType.ZSTD))
                .add(lowerCaseStringListProperty(
                        typeManager,
                        ORDERING_PROPERTY,
                        "Sort order for each chunk of the table"))
                .add(lowerCaseStringProperty(
                        TEMPORAL_COLUMN_PROPERTY,
                        "Temporal column of the table"))
                .add(integerProperty(
                        BUCKET_COUNT_PROPERTY,
                        "Number of buckets into which to divide the table",
                        null,
                        false))
                .add(lowerCaseStringListProperty(
                        typeManager,
                        BUCKETED_ON_PROPERTY,
                        "Table columns on which to bucket the table"))
                .add(lowerCaseStringProperty(
                        DISTRIBUTION_NAME_PROPERTY,
                        "Name of shared distribution for colocated tables"))
                .add(booleanProperty(
                        ORGANIZED_PROPERTY,
                        "Keep the table organized using the sort order",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static List<String> getSortColumns(Map<String, Object> tableProperties)
    {
        return stringList(tableProperties.get(ORDERING_PROPERTY));
    }

    public static String getTemporalColumn(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(TEMPORAL_COLUMN_PROPERTY);
    }

    public static OptionalInt getBucketCount(Map<String, Object> tableProperties)
    {
        Integer value = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        return (value != null) ? OptionalInt.of(value) : OptionalInt.empty();
    }

    public static List<String> getBucketColumns(Map<String, Object> tableProperties)
    {
        return stringList(tableProperties.get(BUCKETED_ON_PROPERTY));
    }

    public static String getDistributionName(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(DISTRIBUTION_NAME_PROPERTY);
    }

    public static CompressionType getCompressionType(Map<String, Object> tableProperties)
    {
        return (CompressionType) tableProperties.get(COMPRESSION_TYPE_PROPERTY);
    }

    public static boolean isOrganized(Map<String, Object> tableProperties)
    {
        Boolean value = (Boolean) tableProperties.get(ORGANIZED_PROPERTY);
        return (value == null) ? false : value;
    }
}
