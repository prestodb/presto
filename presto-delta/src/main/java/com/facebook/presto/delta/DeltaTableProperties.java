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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Locale.ENGLISH;

public class DeltaTableProperties
{
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String CLUSTERED_BY_PROPERTY = "clustered_by";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DeltaTableProperties(TypeManager typeManager, HiveClientConfig config)
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        EXTERNAL_LOCATION_PROPERTY,
                        "File system location URI for external table",
                        null,
                        false),
                new PropertyMetadata<>(
                        STORAGE_FORMAT_PROPERTY,
                        "Hive storage format for the table",
                        createUnboundedVarcharType(),
                        HiveStorageFormat.class,
                        config.getHiveStorageFormat(),
                        false,
                        value -> HiveStorageFormat.valueOf(((String) value).toUpperCase(ENGLISH)),
                        HiveStorageFormat::toString),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        CLUSTERED_BY_PROPERTY,
                        "Cluster columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static boolean isExternalTable(Map<String, Object> tableProperties)
    {
        return tableProperties.get(EXTERNAL_LOCATION_PROPERTY) != null;
    }

    public static HiveStorageFormat getTableStorageFormat(Map<String, Object> tableProperties)
    {
        return (HiveStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    // No getters for "partitioned_by" and "clustered_by" table properties as they are not exposed to the user
    // Only computed after the table snapshots, can be obtained through the "SHOW CREATE TABLE" command
}
