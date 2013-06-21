package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class AliasSystemTable
        implements SystemTable
{
    public static final SchemaTableName ALIAS_TABLE_NAME = new SchemaTableName("sys", "alias");

    public static final TableMetadata ALIAS_TABLE = tableMetadataBuilder(ALIAS_TABLE_NAME)
            .column("source_catalog", STRING)
            .column("table", STRING)
            .column("destination_catalog", STRING)
            .column("alias", STRING)
            .build();

    private final AliasDao aliasDao;

    @Inject
    public AliasSystemTable(AliasDao aliasDao)
    {
        this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
    }

    @Override
    public boolean isDistributed()
    {
        return false;
    }

    @Override
    public TableMetadata getTableMetadata()
    {
        return ALIAS_TABLE;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(ALIAS_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(ALIAS_TABLE);
        for (TableAlias tableAlias : aliasDao.getAliases()) {
            table.addRow(tableAlias.getSourceConnectorId(),
                    new SchemaTableName(tableAlias.getSourceSchemaName(), tableAlias.getSourceTableName()).toString(),
                    tableAlias.getDestinationConnectorId(),
                    new SchemaTableName(tableAlias.getDestinationSchemaName(), tableAlias.getDestinationTableName()).toString());
        }
        return table.build().cursor();
    }
}
