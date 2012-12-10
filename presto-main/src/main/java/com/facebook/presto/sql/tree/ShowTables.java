package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.google.common.base.Preconditions.checkArgument;

public class ShowTables
{
    public static Query create(QualifiedName schema)
    {
        String catalogName = null;
        String schemaName = Session.DEFAULT_SCHEMA; // TODO: use Session properly

        if (schema != null) {
            List<String> parts = schema.getParts();
            checkArgument(parts.size() <= 2, "too many parts in schema name: %s", schema);
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.getSuffix();
        }

        QualifiedName tableName = QualifiedName.of(INFORMATION_SCHEMA, TABLE_TABLES);
        if (catalogName != null) {
            tableName = QualifiedName.of(catalogName, tableName);
        }

        Expression where = null;
        if (schemaName != null) {
            where = equal(nameReference("table_schema"), new StringLiteral(schemaName));
        }

        return new Query(
                selectList(aliasedName("table_name", "Table")),
                table(tableName),
                where,
                ImmutableList.<Expression>of(),
                null,
                ImmutableList.of(ascending("table_name")),
                null
        );
    }
}
