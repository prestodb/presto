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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HiveTableProperties
{
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc_bloom_filter_fpp";
    public static final String AVRO_SCHEMA_URL = "avro_schema_url";
    public static final String PREFERRED_ORDERING_COLUMNS = "preferred_ordering_columns";
    public static final String ENCRYPT_COLUMNS = "encrypt_columns";
    public static final String ENCRYPT_TABLE = "encrypt_table";
    public static final String DWRF_ENCRYPTION_ALGORITHM = "dwrf_encryption_algorithm";
    public static final String DWRF_ENCRYPTION_PROVIDER = "dwrf_encryption_provider";
    public static final String CSV_SEPARATOR = "csv_separator";
    public static final String CSV_QUOTE = "csv_quote";
    public static final String CSV_ESCAPE = "csv_escape";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HiveTableProperties(TypeManager typeManager, HiveClientConfig config)
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
                        BUCKETED_BY_PROPERTY,
                        "Bucketing columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Bucket sorting columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(SortingColumn::sortingColumnFromString)
                                .collect(toImmutableList()),
                        value -> ((Collection<?>) value).stream()
                                .map(SortingColumn.class::cast)
                                .map(SortingColumn::sortingColumnToString)
                                .collect(toImmutableList())),
                new PropertyMetadata<>(
                        ORC_BLOOM_FILTER_COLUMNS,
                        "ORC Bloom filter index columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                doubleProperty(
                        ORC_BLOOM_FILTER_FPP,
                        "ORC Bloom filter false positive probability",
                        config.getOrcDefaultBloomFilterFpp(),
                        false),
                integerProperty(BUCKET_COUNT_PROPERTY, "Number of buckets", 0, false),
                stringProperty(AVRO_SCHEMA_URL, "URI pointing to Avro schema for the table", null, false),
                new PropertyMetadata<>(
                        PREFERRED_ORDERING_COLUMNS,
                        "Preferred ordering columns for unbucketed table",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(SortingColumn::sortingColumnFromString)
                                .collect(toImmutableList()),
                        value -> ((Collection<?>) value).stream()
                                .map(SortingColumn.class::cast)
                                .map(SortingColumn::sortingColumnToString)
                                .collect(toImmutableList())),
                stringProperty(ENCRYPT_TABLE, "Key reference for encrypting the whole table", null, false),
                stringProperty(DWRF_ENCRYPTION_ALGORITHM, "Algorithm used for encryption data in DWRF", null, false),
                stringProperty(DWRF_ENCRYPTION_PROVIDER, "Provider for encryption keys in provider", null, false),
                stringProperty(CSV_SEPARATOR, "CSV separator character", null, false),
                stringProperty(CSV_QUOTE, "CSV quote character", null, false),
                stringProperty(CSV_ESCAPE, "CSV escape character", null, false),
                new PropertyMetadata<>(
                        ENCRYPT_COLUMNS,
                        "List of key references and columns being encrypted. Example: ARRAY['key1:col1,col2', 'key2:col3,col4']",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        ColumnEncryptionInformation.class,
                        null,
                        false,
                        ColumnEncryptionInformation::fromTableProperty,
                        ColumnEncryptionInformation::toTableProperty));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getExternalLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(EXTERNAL_LOCATION_PROPERTY);
    }

    public static boolean isExternalTable(Map<String, Object> tableProperties)
    {
        return tableProperties.get(EXTERNAL_LOCATION_PROPERTY) != null;
    }

    public static String getAvroSchemaUrl(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(AVRO_SCHEMA_URL);
    }

    public static HiveStorageFormat getHiveStorageFormat(Map<String, Object> tableProperties)
    {
        return (HiveStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    public static Optional<HiveBucketProperty> getBucketProperty(Map<String, Object> tableProperties)
    {
        List<String> bucketedBy = getBucketedBy(tableProperties);
        List<SortingColumn> sortedBy = getSortedBy(tableProperties);
        int bucketCount = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        if ((bucketedBy.isEmpty()) && (bucketCount == 0)) {
            if (!sortedBy.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s may be specified only when %s is specified", SORTED_BY_PROPERTY, BUCKETED_BY_PROPERTY));
            }
            return Optional.empty();
        }
        if (bucketCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be greater than zero", BUCKET_COUNT_PROPERTY));
        }
        if (bucketCount > 1_000_000) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s should be no more than 1000000", BUCKET_COUNT_PROPERTY));
        }
        if (bucketedBy.isEmpty() || bucketCount == 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s and %s must be specified together", BUCKETED_BY_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        return Optional.of(new HiveBucketProperty(bucketedBy, bucketCount, sortedBy, HIVE_COMPATIBLE, Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    private static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    private static List<SortingColumn> getSortedBy(Map<String, Object> tableProperties)
    {
        return (List<SortingColumn>) tableProperties.get(SORTED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getOrcBloomFilterColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(ORC_BLOOM_FILTER_COLUMNS);
    }

    public static Double getOrcBloomFilterFpp(Map<String, Object> tableProperties)
    {
        return (Double) tableProperties.get(ORC_BLOOM_FILTER_FPP);
    }

    public static Optional<Character> getCsvProperty(Map<String, Object> tableProperties, String key)
    {
        Object value = tableProperties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        String csvValue = (String) value;
        if (csvValue.length() != 1) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be a single character string, but was: '%s'", key, csvValue));
        }
        return Optional.of(csvValue.charAt(0));
    }

    @SuppressWarnings("unchecked")
    public static List<SortingColumn> getPreferredOrderingColumns(Map<String, Object> tableProperties)
    {
        List<SortingColumn> preferredOrderingColumns = (List<SortingColumn>) tableProperties.get(PREFERRED_ORDERING_COLUMNS);
        if (preferredOrderingColumns == null) {
            return ImmutableList.of();
        }
        if (!preferredOrderingColumns.isEmpty() && getBucketProperty(tableProperties).isPresent()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must not be specified when %s is specified", PREFERRED_ORDERING_COLUMNS, BUCKETED_BY_PROPERTY));
        }
        return preferredOrderingColumns;
    }

    public static String getEncryptTable(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(ENCRYPT_TABLE);
    }

    public static String getDwrfEncryptionAlgorithm(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(DWRF_ENCRYPTION_ALGORITHM);
    }

    public static String getDwrfEncryptionProvider(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(DWRF_ENCRYPTION_PROVIDER);
    }

    public static ColumnEncryptionInformation getEncryptColumns(Map<String, Object> tableProperties)
    {
        return tableProperties.containsKey(ENCRYPT_COLUMNS) ? (ColumnEncryptionInformation) tableProperties.get(ENCRYPT_COLUMNS) :
                ColumnEncryptionInformation.fromMap(ImmutableMap.of());
    }
}
