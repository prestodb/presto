package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type;
import static com.google.common.base.Preconditions.checkArgument;

public class ShowTables
{
    public static Query create(QualifiedName schema)
    {
        String catalogName = null;
        String schemaName = null;

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
            tableName = prepend(catalogName, tableName);
        }

        Expression where = null;
        if (schemaName != null) {
            where = new ComparisonExpression(Type.EQUAL, nameReference("table_schema"), new StringLiteral(schemaName));
        }

        return new Query(
                new Select(false, ImmutableList.<Expression>of(nameReference("table_name"))),
                ImmutableList.<Relation>of(new Table(tableName)),
                where,
                ImmutableList.<Expression>of(),
                null,
                ImmutableList.<SortItem>of(),
                null
        );
    }

    private static QualifiedNameReference nameReference(String name)
    {
        return new QualifiedNameReference(QualifiedName.of(name));
    }

    private static QualifiedName prepend(String prefix, QualifiedName suffix)
    {
        return QualifiedName.of(ImmutableList.<String>builder()
                .add(prefix)
                .addAll(suffix.getParts())
                .build());
    }
}
