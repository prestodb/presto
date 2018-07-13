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

import com.facebook.presto.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;

class S3SelectRecordCursor<K, V extends Writable>
        extends GenericHiveRecordCursor
{
    private static final String THRIFT_STRUCT = "struct";
    private static final String START_STRUCT = "{";
    private static final String END_STRUCT = "}";
    private static final String FIELD_SEPARATOR = ",";
    private static final String EMPTY_STRING = "";
    private static final String SPACE = " ";

    S3SelectRecordCursor(
            Configuration configuration,
            Path path,
            RecordReader recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        super(configuration, path, recordReader, totalBytes, updateSplitSchema(splitSchema, columns), columns, hiveStorageTimeZone, typeManager);
    }

    // since s3select only returns the required column, not the whole columns
    // we need to update the split schema to include only the required columns
    // otherwise, Serde could not deserialize output from s3select to row data correctly
    static Properties updateSplitSchema(Properties splitSchema, List<HiveColumnHandle> columns)
    {
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        // clone split properties for update so as not to affect the original one,
        // which could be used by other logic
        Properties updatedSchema = new Properties();
        updatedSchema.putAll(splitSchema);
        updatedSchema.setProperty(LIST_COLUMNS, buildColumns(columns));
        updatedSchema.setProperty(LIST_COLUMN_TYPES, buildColumnTypes(columns));
        ThriftDDL thriftDDL = parseThriftDDL(splitSchema.getProperty(SERIALIZATION_DDL));
        thriftDDL = filterThriftDDL(thriftDDL, columns);
        updatedSchema.setProperty(SERIALIZATION_DDL, thriftDDLToString(thriftDDL));
        return updatedSchema;
    }

    private static String buildColumns(List<HiveColumnHandle> columns)
    {
        if (columns == null || columns.isEmpty()) {
            return EMPTY_STRING;
        }
        return columns.stream().map(HiveColumnHandle::getName).collect(Collectors.joining(","));
    }

    private static String buildColumnTypes(List<HiveColumnHandle> columns)
    {
        if (columns == null || columns.isEmpty()) {
            return EMPTY_STRING;
        }
        return columns
                .stream()
                .map(column -> column.getHiveType().getTypeInfo().getTypeName())
                .collect(Collectors.joining(","));
    }

    private static ThriftDDL parseThriftDDL(String ddl)
    {
        if (isNullOrEmpty(ddl)) {
            return null;
        }
        String[] parts = ddl.trim().split("\\s+");
        checkArgument(parts.length >= 5, "Invalid thrift DDL " + ddl);
        checkArgument(THRIFT_STRUCT.equals(parts[0]), "ThriftDDL should start with " + THRIFT_STRUCT);
        ThriftDDL thriftDDL = new ThriftDDL();
        thriftDDL.setTableName(parts[1]);
        checkArgument(START_STRUCT.equals(parts[2]), "Invalid thrift DDL " + ddl);
        checkArgument(parts[parts.length - 1].endsWith(END_STRUCT), "Invalid thrift DDL " + ddl);
        parts[parts.length - 1] = parts[parts.length - 1].substring(0, parts[parts.length - 1].length() - 1);
        List<ThriftField> fields = new ArrayList<>();
        for (int i = 3; i < parts.length - 1; i += 2) {
            ThriftField thriftField = new ThriftField();
            thriftField.setType(parts[i]);
            if (parts[i + 1].endsWith(FIELD_SEPARATOR)) {
                parts[i + 1] = parts[i + 1].substring(0, parts[i + 1].length() - 1);
            }
            thriftField.setName(parts[i + 1]);
            fields.add(thriftField);
        }
        thriftDDL.setFields(fields);

        return thriftDDL;
    }

    private static ThriftDDL filterThriftDDL(ThriftDDL thriftDDL, List<HiveColumnHandle> columns)
    {
        if (thriftDDL == null) {
            return null;
        }
        List<ThriftField> fields = thriftDDL.getFields();
        if (fields == null || fields.isEmpty()) {
            return thriftDDL;
        }
        Set<String> columnNames = columns.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableSet());
        List<ThriftField> filteredFields = fields.stream()
                .filter(field -> columnNames.contains(field.getName()))
                .collect(toList());
        thriftDDL.setFields(filteredFields);

        return thriftDDL;
    }

    private static String thriftDDLToString(ThriftDDL thriftDDL)
    {
        if (thriftDDL == null) {
            return EMPTY_STRING;
        }
        List<ThriftField> fields = thriftDDL.getFields();
        if (fields == null || fields.isEmpty()) {
            return EMPTY_STRING;
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(THRIFT_STRUCT)
                .append(SPACE)
                .append(thriftDDL.getTableName())
                .append(SPACE)
                .append(START_STRUCT);
        stringBuilder.append(fields.stream()
                .map(field -> SPACE + field.getType() + SPACE + field.getName())
                .collect(Collectors.joining(",")));
        stringBuilder.append(END_STRUCT);

        return stringBuilder.toString();
    }

    private static class ThriftField
    {
        private String type;
        private String name;

        private String getType()
        {
            return type;
        }

        private void setType(String type)
        {
            checkArgument(!isNullOrEmpty(type), "type is null or empty string");
            this.type = type;
        }

        private String getName()
        {
            return name;
        }

        private void setName(String name)
        {
            requireNonNull(name, "name is null");
            this.name = name;
        }
    }

    private static class ThriftDDL
    {
        private String tableName;
        private List<ThriftField> fields;

        private String getTableName()
        {
            return tableName;
        }

        private void setTableName(String tableName)
        {
            checkArgument(!isNullOrEmpty(tableName), "tableName is null or empty string");
            this.tableName = tableName;
        }

        private List<ThriftField> getFields()
        {
            return fields;
        }

        private void setFields(List<ThriftField> fields)
        {
            requireNonNull(fields, "fields is null");
            this.fields = fields;
        }
    }
}
