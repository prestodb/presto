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
package com.facebook.presto.kudu;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.DiscreteValues;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.kudu.properties.ColumnDesign;
import com.facebook.presto.kudu.properties.HashPartitionDefinition;
import com.facebook.presto.kudu.properties.KuduTableProperties;
import com.facebook.presto.kudu.properties.PartitionDesign;
import com.facebook.presto.kudu.properties.RangePartition;
import com.facebook.presto.kudu.properties.RangePartitionDefinition;
import com.facebook.presto.kudu.schema.SchemaEmulation;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.kudu.KuduUtil.reTryKerberos;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class KuduClientSession
{
    public static final String DEFAULT_SCHEMA = "default";
    private final Logger log = Logger.get(getClass());
    private final KuduConnectorId connectorId;
    private final KuduClient client;
    private final SchemaEmulation schemaEmulation;
    private final boolean kerberosAuthEnabled;

    public KuduClientSession(KuduConnectorId connectorId, KuduClient client, SchemaEmulation schemaEmulation, boolean kerberosAuthEnabled)
    {
        this.connectorId = connectorId;
        this.client = client;
        this.schemaEmulation = schemaEmulation;
        this.kerberosAuthEnabled = kerberosAuthEnabled;
    }

    public List<String> listSchemaNames()
    {
        reTryKerberos(kerberosAuthEnabled);
        return schemaEmulation.listSchemaNames(client);
    }

    private List<String> internalListTables(String prefix)
    {
        try {
            if (prefix.isEmpty()) {
                return client.getTablesList().getTablesList();
            }
            else {
                return client.getTablesList(prefix).getTablesList();
            }
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public List<SchemaTableName> listTables(Optional<String> optSchemaName)
    {
        reTryKerberos(kerberosAuthEnabled);
        if (optSchemaName.isPresent()) {
            return listTablesSingleSchema(optSchemaName.get());
        }

        List<SchemaTableName> all = new ArrayList<>();
        for (String schemaName : listSchemaNames()) {
            List<SchemaTableName> single = listTablesSingleSchema(schemaName);
            all.addAll(single);
        }
        return all;
    }

    private List<SchemaTableName> listTablesSingleSchema(String schemaName)
    {
        final String prefix = schemaEmulation.getPrefixForTablesOfSchema(schemaName);

        List<String> tables = internalListTables(prefix);
        return tables.stream()
                .map(schemaEmulation::fromRawName)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    public Schema getTableSchema(KuduTableHandle tableHandle)
    {
        reTryKerberos(kerberosAuthEnabled);
        KuduTable table = tableHandle.getTable(this);
        return table.getSchema();
    }

    public Map<String, Object> getTableProperties(KuduTableHandle tableHandle)
    {
        reTryKerberos(kerberosAuthEnabled);
        KuduTable table = tableHandle.getTable(this);
        return KuduTableProperties.toMap(table);
    }

    public List<KuduSplit> buildKuduSplits(KuduTableLayoutHandle layoutHandle)
    {
        reTryKerberos(kerberosAuthEnabled);
        KuduTableHandle tableHandle = layoutHandle.getTableHandle();
        KuduTable table = tableHandle.getTable(this);
        final int primaryKeyColumnCount = table.getSchema().getPrimaryKeyColumnCount();
        KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(table);

        TupleDomain<ColumnHandle> constraintSummary = layoutHandle.getConstraintSummary();
        if (!addConstraintPredicates(table, builder, constraintSummary)) {
            return ImmutableList.of();
        }

        Optional<Set<ColumnHandle>> desiredColumns = layoutHandle.getDesiredColumns();
        if (desiredColumns.isPresent()) {
            if (desiredColumns.get().contains(KuduColumnHandle.ROW_ID_HANDLE)) {
                List<Integer> columnIndexes = IntStream
                        .range(0, primaryKeyColumnCount)
                        .boxed().collect(Collectors.toList());
                for (ColumnHandle columnHandle : desiredColumns.get()) {
                    if (columnHandle instanceof KuduColumnHandle) {
                        KuduColumnHandle k = (KuduColumnHandle) columnHandle;
                        int index = k.getOrdinalPosition();
                        if (index >= primaryKeyColumnCount) {
                            columnIndexes.add(index);
                        }
                    }
                }
                builder.setProjectedColumnIndexes(columnIndexes);
            }
            else {
                List<Integer> columnIndexes = desiredColumns.get().stream()
                        .map(handle -> ((KuduColumnHandle) handle).getOrdinalPosition())
                        .collect(toImmutableList());
                builder.setProjectedColumnIndexes(columnIndexes);
            }
        }

        List<KuduScanToken> tokens = builder.build();
        return tokens.stream()
                .map(token -> toKuduSplit(tableHandle, token, primaryKeyColumnCount))
                .collect(toImmutableList());
    }

    public KuduScanner createScanner(KuduSplit kuduSplit)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            return KuduScanToken.deserializeIntoScanner(kuduSplit.getSerializedScanToken(), client);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KuduTable openTable(SchemaTableName schemaTableName)
    {
        reTryKerberos(kerberosAuthEnabled);
        String rawName = schemaEmulation.toRawName(schemaTableName);
        try {
            return client.openTable(rawName);
        }
        catch (KuduException e) {
            log.debug("Error on doOpenTable: " + e, e);
            if (!listSchemaNames().contains(schemaTableName.getSchemaName())) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName());
            }
            throw new TableNotFoundException(schemaTableName);
        }
    }

    public KuduSession newSession()
    {
        reTryKerberos(kerberosAuthEnabled);
        return client.newSession();
    }

    public void createSchema(String schemaName)
    {
        reTryKerberos(kerberosAuthEnabled);
        schemaEmulation.createSchema(client, schemaName);
    }

    public void dropSchema(String schemaName)
    {
        reTryKerberos(kerberosAuthEnabled);
        schemaEmulation.dropSchema(client, schemaName);
    }

    public void dropTable(SchemaTableName schemaTableName)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            client.deleteTable(rawName);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void renameTable(SchemaTableName schemaTableName, SchemaTableName newSchemaTableName)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            String newRawName = schemaEmulation.toRawName(newSchemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameTable(newRawName);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public KuduTable createTable(ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(tableMetadata.getTable());
            if (ignoreExisting) {
                if (client.tableExists(rawName)) {
                    return null;
                }
            }

            if (!schemaEmulation.existsSchema(client, tableMetadata.getTable().getSchemaName())) {
                throw new SchemaNotFoundException(tableMetadata.getTable().getSchemaName());
            }

            List<ColumnMetadata> columns = tableMetadata.getColumns();
            Map<String, Object> properties = tableMetadata.getProperties();

            Schema schema = buildSchema(columns, properties);
            CreateTableOptions options = buildCreateTableOptions(schema, properties);
            return client.createTable(rawName, schema, options);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata column)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            Type type = TypeHelper.toKuduClientType(column.getType());
            alterOptions.addNullableColumn(column.getName(), type);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void dropColumn(SchemaTableName schemaTableName, String name)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.dropColumn(name);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void renameColumn(SchemaTableName schemaTableName, String oldName, String newName)
    {
        reTryKerberos(kerberosAuthEnabled);
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameColumn(oldName, newName);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void addRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition)
    {
        reTryKerberos(kerberosAuthEnabled);
        changeRangePartition(schemaTableName, rangePartition, RangePartitionChange.ADD);
    }

    public void dropRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition)
    {
        reTryKerberos(kerberosAuthEnabled);
        changeRangePartition(schemaTableName, rangePartition, RangePartitionChange.DROP);
    }

    private void changeRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition,
            RangePartitionChange change)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            KuduTable table = client.openTable(rawName);
            Schema schema = table.getSchema();
            PartitionDesign design = KuduTableProperties.getPartitionDesign(table);
            RangePartitionDefinition definition = design.getRange();
            if (definition == null) {
                throw new PrestoException(QUERY_REJECTED, "Table " + schemaTableName + " has no range partition");
            }
            PartialRow lowerBound = KuduTableProperties.toRangeBoundToPartialRow(schema, definition, rangePartition.getLower());
            PartialRow upperBound = KuduTableProperties.toRangeBoundToPartialRow(schema, definition, rangePartition.getUpper());
            AlterTableOptions alterOptions = new AlterTableOptions();
            switch (change) {
                case ADD:
                    alterOptions.addRangePartition(lowerBound, upperBound);
                    break;
                case DROP:
                    alterOptions.dropRangePartition(lowerBound, upperBound);
                    break;
            }
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private Schema buildSchema(List<ColumnMetadata> columns, Map<String, Object> tableProperties)
    {
        List<ColumnSchema> kuduColumns = columns.stream()
                .map(this::toColumnSchema)
                .collect(ImmutableList.toImmutableList());
        return new Schema(kuduColumns);
    }

    private ColumnSchema toColumnSchema(ColumnMetadata columnMetadata)
    {
        String name = columnMetadata.getName();
        ColumnDesign design = KuduTableProperties.getColumnDesign(columnMetadata.getProperties());
        Type ktype = TypeHelper.toKuduClientType(columnMetadata.getType());
        ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, ktype);
        builder.key(design.isPrimaryKey()).nullable(design.isNullable());
        setEncoding(name, builder, design);
        setCompression(name, builder, design);
        setTypeAttributes(columnMetadata, builder);
        return builder.build();
    }

    private void setTypeAttributes(ColumnMetadata columnMetadata, ColumnSchema.ColumnSchemaBuilder builder)
    {
        if (columnMetadata.getType() instanceof DecimalType) {
            DecimalType type = (DecimalType) columnMetadata.getType();
            ColumnTypeAttributes attributes = new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                    .precision(type.getPrecision())
                    .scale(type.getScale()).build();
            builder.typeAttributes(attributes);
        }
    }

    private void setCompression(String name, ColumnSchema.ColumnSchemaBuilder builder, ColumnDesign design)
    {
        if (design.getCompression() != null) {
            try {
                ColumnSchema.CompressionAlgorithm algorithm = KuduTableProperties.lookupCompression(design.getCompression());
                builder.compressionAlgorithm(algorithm);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown compression algorithm " + design.getCompression() + " for column " + name);
            }
        }
    }

    private void setEncoding(String name, ColumnSchema.ColumnSchemaBuilder builder, ColumnDesign design)
    {
        if (design.getEncoding() != null) {
            try {
                ColumnSchema.Encoding encoding = KuduTableProperties.lookupEncoding(design.getEncoding());
                builder.encoding(encoding);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown encoding " + design.getEncoding() + " for column " + name);
            }
        }
    }

    private CreateTableOptions buildCreateTableOptions(Schema schema, Map<String, Object> properties)
    {
        CreateTableOptions options = new CreateTableOptions();

        RangePartitionDefinition rangePartitionDefinition = null;
        PartitionDesign partitionDesign = KuduTableProperties.getPartitionDesign(properties);
        if (partitionDesign.getHash() != null) {
            for (HashPartitionDefinition partition : partitionDesign.getHash()) {
                options.addHashPartitions(partition.getColumns(), partition.getBuckets());
            }
        }
        if (partitionDesign.getRange() != null) {
            rangePartitionDefinition = partitionDesign.getRange();
            options.setRangePartitionColumns(rangePartitionDefinition.getColumns());
        }

        List<RangePartition> rangePartitions = KuduTableProperties.getRangePartitions(properties);
        if (rangePartitionDefinition != null && !rangePartitions.isEmpty()) {
            for (RangePartition rangePartition : rangePartitions) {
                PartialRow lower = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getLower());
                PartialRow upper = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getUpper());
                options.addRangePartition(lower, upper);
            }
        }

        Optional<Integer> numReplicas = KuduTableProperties.getNumReplicas(properties);
        numReplicas.ifPresent(options::setNumReplicas);

        return options;
    }

    /**
     * translates TupleDomain to KuduPredicates.
     *
     * @return false if TupleDomain or one of its domains is none
     */
    private boolean addConstraintPredicates(KuduTable table, KuduScanToken.KuduScanTokenBuilder builder,
            TupleDomain<ColumnHandle> constraintSummary)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        else if (!constraintSummary.isAll()) {
            Schema schema = table.getSchema();
            for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : constraintSummary.getColumnDomains().get()) {
                int position = ((KuduColumnHandle) columnDomain.getColumn()).getOrdinalPosition();
                ColumnSchema columnSchema = schema.getColumnByIndex(position);
                Domain domain = columnDomain.getDomain();
                if (domain.isNone()) {
                    return false;
                }
                else if (domain.isAll()) {
                    // no restriction
                }
                else if (domain.isOnlyNull()) {
                    builder.addPredicate(KuduPredicate.newIsNullPredicate(columnSchema));
                }
                else if (domain.getValues().isAll() && domain.isNullAllowed()) {
                    builder.addPredicate(KuduPredicate.newIsNotNullPredicate(columnSchema));
                }
                else if (domain.isSingleValue()) {
                    KuduPredicate predicate = createEqualsPredicate(columnSchema, domain.getSingleValue());
                    builder.addPredicate(predicate);
                }
                else {
                    ValueSet valueSet = domain.getValues();
                    if (valueSet instanceof EquatableValueSet) {
                        DiscreteValues discreteValues = valueSet.getDiscreteValues();
                        KuduPredicate predicate = createInListPredicate(columnSchema, discreteValues);
                        builder.addPredicate(predicate);
                    }
                    else if (valueSet instanceof SortedRangeSet) {
                        Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                        Range span = ranges.getSpan();
                        Marker low = span.getLow();
                        if (!low.isLowerUnbounded()) {
                            KuduPredicate.ComparisonOp op = (low.getBound() == Marker.Bound.ABOVE)
                                    ? KuduPredicate.ComparisonOp.GREATER : KuduPredicate.ComparisonOp.GREATER_EQUAL;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, low.getValue());
                            builder.addPredicate(predicate);
                        }
                        Marker high = span.getHigh();
                        if (!high.isUpperUnbounded()) {
                            KuduPredicate.ComparisonOp op = (low.getBound() == Marker.Bound.BELOW)
                                    ? KuduPredicate.ComparisonOp.LESS : KuduPredicate.ComparisonOp.LESS_EQUAL;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, high.getValue());
                            builder.addPredicate(predicate);
                        }
                    }
                    else {
                        throw new IllegalStateException("Unexpected domain: " + domain);
                    }
                }
            }
        }
        return true;
    }

    private KuduPredicate createInListPredicate(ColumnSchema columnSchema, DiscreteValues discreteValues)
    {
        com.facebook.presto.common.type.Type type = TypeHelper.fromKuduColumn(columnSchema);
        List<Object> javaValues = discreteValues.getValues().stream().map(value -> TypeHelper.getJavaValue(type, value)).collect(toImmutableList());
        return KuduPredicate.newInListPredicate(columnSchema, javaValues);
    }

    private KuduPredicate createEqualsPredicate(ColumnSchema columnSchema, Object value)
    {
        return createComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, value);
    }

    private KuduPredicate createComparisonPredicate(ColumnSchema columnSchema,
            KuduPredicate.ComparisonOp op,
            Object value)
    {
        com.facebook.presto.common.type.Type type = TypeHelper.fromKuduColumn(columnSchema);
        Object javaValue = TypeHelper.getJavaValue(type, value);
        if (javaValue instanceof Long) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Long) javaValue);
        }
        else if (javaValue instanceof Integer) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Integer) javaValue);
        }
        else if (javaValue instanceof Short) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Short) javaValue);
        }
        else if (javaValue instanceof Byte) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Byte) javaValue);
        }
        else if (javaValue instanceof String) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (String) javaValue);
        }
        else if (javaValue instanceof Double) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Double) javaValue);
        }
        else if (javaValue instanceof Float) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Float) javaValue);
        }
        else if (javaValue instanceof Boolean) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Boolean) javaValue);
        }
        else if (javaValue instanceof byte[]) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (byte[]) javaValue);
        }
        else if (javaValue == null) {
            throw new IllegalStateException("Unexpected null java value for column " + columnSchema.getName());
        }
        else {
            throw new IllegalStateException("Unexpected java value for column "
                    + columnSchema.getName() + ": " + javaValue + "(" + javaValue.getClass() + ")");
        }
    }

    private KuduSplit toKuduSplit(KuduTableHandle tableHandle, KuduScanToken token,
            int primaryKeyColumnCount)
    {
        try {
            byte[] serializedScanToken = token.serialize();
            return new KuduSplit(tableHandle, primaryKeyColumnCount, serializedScanToken);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
