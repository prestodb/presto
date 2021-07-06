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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.iceberg.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class ManifestsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;

    public ManifestsTable(SchemaTableName tableName, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        tableMetadata = new ConnectorTableMetadata(
                tableName,
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("path", VARCHAR))
                        .add(new ColumnMetadata("length", BIGINT))
                        .add(new ColumnMetadata("partition_spec_id", INTEGER))
                        .add(new ColumnMetadata("added_snapshot_id", BIGINT))
                        .add(new ColumnMetadata("added_data_files_count", INTEGER))
                        .add(new ColumnMetadata("existing_data_files_count", INTEGER))
                        .add(new ColumnMetadata("deleted_data_files_count", INTEGER))
                        .add(new ColumnMetadata("partitions", new ArrayType(RowType.from(asList(
                                RowType.field("contains_null", BOOLEAN),
                                RowType.field("lower_bound", VARCHAR),
                                RowType.field("upper_bound", VARCHAR))))))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (!snapshotId.isPresent()) {
            return new FixedPageSource(ImmutableList.of());
        }
        return new FixedPageSource(buildPages(tableMetadata, icebergTable, snapshotId.get()));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable, long snapshotId)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        Snapshot snapshot = icebergTable.snapshot(snapshotId);
        if (snapshot == null) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, format("Snapshot ID [%s] does not exist for table: %s", snapshotId, icebergTable));
        }

        Map<Integer, PartitionSpec> partitionSpecsById = icebergTable.specs();

        snapshot.allManifests().forEach(file -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(file.path());
            pagesBuilder.appendBigint(file.length());
            pagesBuilder.appendInteger(file.partitionSpecId());
            pagesBuilder.appendBigint(file.snapshotId());
            pagesBuilder.appendInteger(file.addedFilesCount());
            pagesBuilder.appendInteger(file.existingFilesCount());
            pagesBuilder.appendInteger(file.deletedFilesCount());
            writePartitionSummaries(pagesBuilder.nextColumn(), file.partitions(), partitionSpecsById.get(file.partitionSpecId()));
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static void writePartitionSummaries(BlockBuilder arrayBlockBuilder, List<ManifestFile.PartitionFieldSummary> summaries, PartitionSpec partitionSpec)
    {
        BlockBuilder singleArrayWriter = arrayBlockBuilder.beginBlockEntry();
        for (int i = 0; i < summaries.size(); i++) {
            ManifestFile.PartitionFieldSummary summary = summaries.get(i);
            PartitionField field = partitionSpec.fields().get(i);
            Type nestedType = partitionSpec.partitionType().fields().get(i).type();

            BlockBuilder rowBuilder = singleArrayWriter.beginBlockEntry();
            BOOLEAN.writeBoolean(rowBuilder, summary.containsNull());
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.upperBound())));
            singleArrayWriter.closeEntry();
        }
        arrayBlockBuilder.closeEntry();
    }
}
