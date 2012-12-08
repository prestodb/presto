package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ShowColumns
{
    public static Query create(QualifiedName table)
    {
        checkNotNull(table, "table is null");
        List<String> parts = Lists.reverse(table.getParts());
        checkArgument(parts.size() <= 3, "too many parts in table name: %s", table);

        QualifiedName columnsTable = QualifiedName.of(INFORMATION_SCHEMA, TABLE_COLUMNS);
        String schemaName = Session.DEFAULT_SCHEMA; // TODO: use Session properly
        String tableName = parts.get(0);

        if (parts.size() > 2) {
            columnsTable = QualifiedName.of(parts.get(2), columnsTable);
        }
        if (parts.size() > 1) {
            schemaName = parts.get(1);
        }

        Expression where = logicalAnd(
                equal(nameReference("table_schema"), new StringLiteral(schemaName)),
                equal(nameReference("table_name"), new StringLiteral(tableName)));

        return new Query(
                selectList(
                        aliasedName("column_name", "Column"),
                        aliasedName("data_type", "Type"),
                        aliasedName("is_nullable", "Null")),
                table(columnsTable),
                where,
                ImmutableList.<Expression>of(),
                null,
                ImmutableList.of(ascending("ordinal_position")),
                null
        );
    }
}
