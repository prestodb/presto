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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * The query of a view in a MySQL catalog, rewritten so that MySQL resolves each table to the
 * one Presto resolved when it analyzed the view. MySQL knows nothing of Presto catalogs, so the
 * catalog is removed from a name such as mysql.web.orders, and a table in any other catalog is
 * rejected, since a MySQL view cannot read it. A name without a schema is resolved by MySQL
 * against the default database of the connection, which {@link #getDefaultSchema} names.
 *
 * <p>Only the table names are touched. The query arrives as CreateViewTask formats it, and the
 * rest of that text is what MySQL receives either way, so it is not formatted a second time.
 */
final class MySqlViewQuery
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder()
            .setDecimalLiteralTreatment(AS_DECIMAL)
            .build();

    private final String sql;
    private final Optional<String> defaultSchema;

    private MySqlViewQuery(String sql, Optional<String> defaultSchema)
    {
        this.sql = requireNonNull(sql, "sql is null");
        this.defaultSchema = requireNonNull(defaultSchema, "defaultSchema is null");
    }

    /**
     * @param sql the view query, in the form CreateViewTask records it
     * @param catalog the catalog the view is created in
     * @param sessionCatalog the session catalog the query was analyzed with
     * @param sessionSchema the session schema the query was analyzed with
     */
    static MySqlViewQuery rewrite(String sql, String catalog, Optional<String> sessionCatalog, Optional<String> sessionSchema)
    {
        TableCollector tables = new TableCollector();
        tables.process(SQL_PARSER.createStatement(sql, PARSING_OPTIONS), null);

        List<Table> catalogQualified = new ArrayList<>();
        boolean hasUnqualified = false;
        for (Table table : tables.getTables()) {
            List<String> parts = table.getName().getParts();
            if (parts.size() == 1 && tables.getWithQueryNames().contains(parts.get(0))) {
                continue;
            }
            // the analyzer resolved a name missing its catalog against the session catalog
            String tableCatalog = parts.size() == 3 ? parts.get(0) : sessionCatalog.map(name -> name.toLowerCase(ENGLISH)).orElse(null);
            if (!catalog.equals(tableCatalog)) {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "A view in MySQL catalog '%s' can only read tables in that catalog, but its query reads '%s' from catalog '%s'",
                        catalog,
                        table.getName(),
                        tableCatalog));
            }
            if (parts.size() == 3) {
                catalogQualified.add(table);
            }
            else if (parts.size() == 1) {
                hasUnqualified = true;
            }
        }

        // splice from the end, so the offsets of the earlier names stay valid
        catalogQualified.sort(comparingInt((Table table) -> offset(sql, table)).reversed());
        StringBuilder rewritten = new StringBuilder(sql);
        for (Table table : catalogQualified) {
            int start = offset(sql, table);
            String prefix = formatIdentifier(table.getName().getOriginalParts().get(0)) + ".";
            if (!sql.startsWith(prefix, start)) {
                throw new PrestoException(NOT_SUPPORTED, format("Cannot remove the catalog from table name '%s' in view query: %s", table.getName(), sql));
            }
            rewritten.delete(start, start + prefix.length());
        }

        return new MySqlViewQuery(rewritten.toString(), hasUnqualified ? sessionSchema : Optional.empty());
    }

    String getSql()
    {
        return sql;
    }

    /**
     * The schema MySQL has to resolve the tables named without one against, present only when the
     * query names such a table.
     */
    Optional<String> getDefaultSchema()
    {
        return defaultSchema;
    }

    private static int offset(String sql, Table table)
    {
        NodeLocation location = table.getLocation()
                .orElseThrow(() -> new IllegalArgumentException("No location for table " + table.getName()));
        int offset = 0;
        for (int line = 1; line < location.getLineNumber(); line++) {
            offset = sql.indexOf('\n', offset) + 1;
        }
        return offset + location.getColumnNumber() - 1;
    }

    // the same quoting SqlFormatter applies to each part of a table name
    private static String formatIdentifier(Identifier identifier)
    {
        String delimiter = identifier.isDelimited() ? "\"" : "";
        return delimiter + identifier.getValue().replace("\"", "\"\"") + delimiter;
    }

    private static class TableCollector
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final ImmutableList.Builder<Table> tables = ImmutableList.builder();
        private final Set<String> withQueryNames = new HashSet<>();

        @Override
        protected Void visitTable(Table node, Void context)
        {
            tables.add(node);
            return null;
        }

        // a WITH query name is matched anywhere in the statement rather than only within its
        // scope, so a table of the same name outside that scope is not checked
        @Override
        protected Void visitWithQuery(WithQuery node, Void context)
        {
            withQueryNames.add(node.getName().getValue().toLowerCase(ENGLISH));
            return super.visitWithQuery(node, context);
        }

        List<Table> getTables()
        {
            return tables.build();
        }

        Set<String> getWithQueryNames()
        {
            return withQueryNames;
        }
    }
}
