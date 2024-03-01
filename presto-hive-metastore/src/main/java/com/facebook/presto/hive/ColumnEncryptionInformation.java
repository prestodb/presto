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

import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class ColumnEncryptionInformation
{
    private static final char HIVE_PROPERTY_COLUMN_DELIMITER = ',';
    private static final char HIVE_PROPERTY_KEY_JOINER = ':';
    private static final char HIVE_PROPERTY_ENTRY_DELIMITER = ';';

    private final Map<ColumnWithStructSubfield, String> columnToKeyReference;

    private ColumnEncryptionInformation(Map<ColumnWithStructSubfield, String> columnToKeyReference)
    {
        this.columnToKeyReference = ImmutableMap.copyOf(requireNonNull(columnToKeyReference, "columnToKeyReference is null"));
    }

    public Map<ColumnWithStructSubfield, String> getColumnToKeyReference()
    {
        return columnToKeyReference;
    }

    public boolean hasEntries()
    {
        return !columnToKeyReference.isEmpty();
    }

    private Map<String, List<String>> getKeyReferenceToColumns()
    {
        Map<String, List<String>> keyReferenceToColumns = new HashMap<>();
        columnToKeyReference.forEach((column, keyReference) -> keyReferenceToColumns.computeIfAbsent(keyReference, unused -> new ArrayList<>()).add(column.toString()));
        return keyReferenceToColumns;
    }

    public List<String> toTableProperty()
    {
        return getKeyReferenceToColumns()
                .entrySet()
                .stream()
                .map(entry -> format("%s%s%s", entry.getKey(), HIVE_PROPERTY_KEY_JOINER, Joiner.on(HIVE_PROPERTY_COLUMN_DELIMITER).join(entry.getValue())))
                .collect(toImmutableList());
    }

    public String toHiveProperty()
    {
        return Joiner.on(HIVE_PROPERTY_ENTRY_DELIMITER).join(toTableProperty());
    }

    @Override
    public int hashCode()
    {
        return hash(columnToKeyReference);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }

        ColumnEncryptionInformation otherObj = (ColumnEncryptionInformation) obj;
        return Objects.equals(this.columnToKeyReference, otherObj.columnToKeyReference);
    }

    public static ColumnEncryptionInformation fromHiveProperty(String value)
    {
        if (value == null) {
            return new ColumnEncryptionInformation(ImmutableMap.of());
        }

        List<String> keyReferenceWithColumns = Splitter.on(HIVE_PROPERTY_ENTRY_DELIMITER).trimResults().splitToList(value);
        return fromTableProperty(keyReferenceWithColumns);
    }

    public static ColumnEncryptionInformation fromTableProperty(Object value)
    {
        if (value == null) {
            return new ColumnEncryptionInformation(ImmutableMap.of());
        }

        List<?> data = (List<?>) value;

        Map<ColumnWithStructSubfield, String> columnToKeyReference = new HashMap<>();
        for (Object entry : data) {
            if (entry == null) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Encrypted columns property cannot have null value");
            }

            String keyEntry = (String) entry;

            List<String> keyEntries = Splitter.on(HIVE_PROPERTY_KEY_JOINER).splitToList(keyEntry);

            if (keyEntries.size() != 2) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Encrypted column entry needs to be in the format 'key1:col1,col2'. Received: %s", keyEntry));
            }

            String keyReference = keyEntries.get(0);
            String columns = keyEntries.get(1);

            List<String> columnList = Splitter.on(HIVE_PROPERTY_COLUMN_DELIMITER).trimResults().splitToList(columns);

            columnList.forEach(column -> {
                String previousReferenceKey = columnToKeyReference.put(ColumnWithStructSubfield.valueOf(column), keyReference);
                if (previousReferenceKey != null) {
                    throw new PrestoException(
                            INVALID_TABLE_PROPERTY,
                            format("Column %s has been assigned 2 key references (%s and %s). Only 1 is allowed", column, keyReference, previousReferenceKey));
                }
            });
        }
        return new ColumnEncryptionInformation(columnToKeyReference);
    }

    public static ColumnEncryptionInformation fromMap(Map<String, String> columnToKeyReference)
    {
        return new ColumnEncryptionInformation(
                columnToKeyReference
                        .entrySet()
                        .stream()
                        .collect(toImmutableMap(entry -> ColumnWithStructSubfield.valueOf(entry.getKey()), Map.Entry::getValue)));
    }

    public static final class ColumnWithStructSubfield
    {
        private final String columnName;
        private final Optional<String> subfieldPath;

        private ColumnWithStructSubfield(String columnName, Optional<String> subfieldPath)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.subfieldPath = requireNonNull(subfieldPath, "subfieldPath is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<String> getSubfieldPath()
        {
            return subfieldPath;
        }

        public Optional<ColumnWithStructSubfield> getChildField()
        {
            return subfieldPath.map(ColumnWithStructSubfield::valueOf);
        }

        public static ColumnWithStructSubfield valueOf(String columnExpression)
        {
            if (columnExpression == null) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot provide null column name for encryption columns");
            }

            List<String> splitParts = Splitter.on('.').limit(2).splitToList(columnExpression);

            if (splitParts.size() == 1) {
                return new ColumnWithStructSubfield(splitParts.get(0), Optional.empty());
            }

            return new ColumnWithStructSubfield(splitParts.get(0), Optional.of(splitParts.get(1)));
        }

        @Override
        public String toString()
        {
            return columnName + subfieldPath.map(path -> "." + path).orElse("");
        }

        @Override
        public int hashCode()
        {
            return hash(columnName, subfieldPath);
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null) {
                return false;
            }

            if (!other.getClass().equals(this.getClass())) {
                return false;
            }

            ColumnWithStructSubfield otherObj = (ColumnWithStructSubfield) other;
            return Objects.equals(this.columnName, otherObj.columnName) &&
                    Objects.equals(this.subfieldPath, otherObj.subfieldPath);
        }
    }
}
