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

import com.facebook.hive.orc.OrcSerde;
import com.facebook.presto.hive.ColumnEncryptionInformation.ColumnWithStructSubfield;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.DwrfTableEncryptionProperties.fromHiveTableProperties;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public abstract class AbstractDwrfEncryptionInformationSource
        implements EncryptionInformationSource
{
    @Override
    public Optional<Map<String, EncryptionInformation>> getReadEncryptionInformation(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, Partition> partitions)
    {
        Optional<DwrfTableEncryptionProperties> encryptionProperties = getTableEncryptionProperties(table);
        if (!encryptionProperties.isPresent()) {
            return Optional.empty();
        }

        Optional<Map<String, String>> fieldToKeyReference = getFieldToKeyReference(encryptionProperties.get(), requestedColumns);
        if (!fieldToKeyReference.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(getReadEncryptionInformationInternal(session, table, requestedColumns, partitions, fieldToKeyReference.get(), encryptionProperties.get()));
    }

    protected abstract Map<String, EncryptionInformation> getReadEncryptionInformationInternal(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, Partition> partitions,
            Map<String, String> fieldToKeyReference,
            DwrfTableEncryptionProperties encryptionProperties);

    @Override
    public Optional<EncryptionInformation> getReadEncryptionInformation(ConnectorSession session, Table table, Optional<Set<HiveColumnHandle>> requestedColumns)
    {
        Optional<DwrfTableEncryptionProperties> encryptionProperties = getTableEncryptionProperties(table);
        if (!encryptionProperties.isPresent()) {
            return Optional.empty();
        }

        Optional<Map<String, String>> fieldToKeyReference = getFieldToKeyReference(encryptionProperties.get(), requestedColumns);
        if (!fieldToKeyReference.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(getReadEncryptionInformationInternal(session, table, requestedColumns, fieldToKeyReference.get(), encryptionProperties.get()));
    }

    protected abstract EncryptionInformation getReadEncryptionInformationInternal(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, String> fieldToKeyReference,
            DwrfTableEncryptionProperties encryptionProperties);

    @Override
    public Optional<EncryptionInformation> getWriteEncryptionInformation(ConnectorSession session, TableEncryptionProperties tableEncryptionProperties, String dbName, String tableName)
    {
        if (!(tableEncryptionProperties instanceof DwrfTableEncryptionProperties)) {
            return Optional.empty();
        }
        return Optional.of(getWriteEncryptionInformationInternal(session, (DwrfTableEncryptionProperties) tableEncryptionProperties, dbName, tableName));
    }

    protected abstract EncryptionInformation getWriteEncryptionInformationInternal(
            ConnectorSession session,
            DwrfTableEncryptionProperties tableEncryptionProperties,
            String dbName,
            String tableName);

    private static Optional<Map<String, String>> getFieldToKeyReference(DwrfTableEncryptionProperties encryptionProperties, Optional<Set<HiveColumnHandle>> requestedColumns)
    {
        Optional<ColumnEncryptionInformation> columnEncryptionInformation = encryptionProperties.getColumnEncryptionInformation();
        Optional<String> encryptTable = encryptionProperties.getEncryptTable();

        Map<String, String> fieldToKeyReference;
        if (encryptTable.isPresent()) {
            if (!requestedColumns.isPresent() || !requestedColumns.get().isEmpty()) {
                fieldToKeyReference = ImmutableMap.of(DwrfEncryptionMetadata.TABLE_IDENTIFIER, encryptTable.get());
            }
            else {
                fieldToKeyReference = ImmutableMap.of();
            }
        }
        else if (columnEncryptionInformation.isPresent()) {
            Map<ColumnWithStructSubfield, String> allFieldsToKeyReference = columnEncryptionInformation.get().getColumnToKeyReference();
            Optional<Set<String>> requestedColumnNames = requestedColumns.map(columns -> columns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet()));

            fieldToKeyReference = allFieldsToKeyReference.entrySet()
                    .stream()
                    .filter(entry -> requestedColumnNames.map(columns -> columns.contains(entry.getKey().getColumnName())).orElse(true))
                    .collect(toImmutableMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Neither of encryptColumn or encryptTable present. We should never hit this");
        }

        return Optional.of(fieldToKeyReference);
    }

    private static Optional<DwrfTableEncryptionProperties> getTableEncryptionProperties(Table table)
    {
        if (!OrcSerde.class.getName().equals(table.getStorage().getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        return fromHiveTableProperties(table.getParameters());
    }
}
