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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.framework.PrestoAction.ResultSetConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryStage.REWRITE;
import static com.facebook.presto.verifier.framework.QueryType.Category.DATA_PRODUCING;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class QueryRewriter
{
    private final SqlParser sqlParser;
    private final PrestoAction prestoAction;
    private final List<Property> tablePropertyOverrides;
    private final Map<ClusterType, QualifiedName> prefixes;

    public QueryRewriter(SqlParser sqlParser, PrestoAction prestoAction, List<Property> tablePropertyOverrides, VerifierConfig verifierConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.tablePropertyOverrides = requireNonNull(tablePropertyOverrides, "tablePropertyOverrides is null");
        this.prefixes = ImmutableMap.of(
                CONTROL, verifierConfig.getControlTablePrefix(),
                TEST, verifierConfig.getTestTablePrefix());
    }

    public QueryBundle rewriteQuery(@Language("SQL") String query, ClusterType cluster)
    {
        Statement statement = sqlParser.createStatement(query, PARSING_OPTIONS);
        if (QueryType.of(statement).getCategory() != DATA_PRODUCING) {
            return new QueryBundle(Optional.empty(), ImmutableList.of(), statement, ImmutableList.of(), cluster);
        }

        QualifiedName prefix = prefixes.get(cluster);
        if (statement instanceof CreateTableAsSelect) {
            CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
            QualifiedName temporaryTableName = generateTemporaryTableName(Optional.of(createTableAsSelect.getName()), prefix);
            return new QueryBundle(
                    Optional.of(temporaryTableName),
                    ImmutableList.of(),
                    new CreateTableAsSelect(
                            temporaryTableName,
                            createTableAsSelect.getQuery(),
                            createTableAsSelect.isNotExists(),
                            applyPropertyOverride(createTableAsSelect.getProperties(), tablePropertyOverrides),
                            createTableAsSelect.isWithData(),
                            createTableAsSelect.getColumnAliases(),
                            createTableAsSelect.getComment()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    cluster);
        }
        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            QualifiedName originalTableName = insert.getTarget();
            QualifiedName temporaryTableName = generateTemporaryTableName(Optional.of(originalTableName), prefix);
            return new QueryBundle(
                    Optional.of(temporaryTableName),
                    ImmutableList.of(
                            new CreateTable(
                                    temporaryTableName,
                                    ImmutableList.of(new LikeClause(originalTableName, Optional.of(INCLUDING))),
                                    false,
                                    tablePropertyOverrides,
                                    Optional.empty())),
                    new Insert(
                            temporaryTableName,
                            insert.getColumns(),
                            insert.getQuery()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    cluster);
        }
        if (statement instanceof Query) {
            QualifiedName temporaryTableName = generateTemporaryTableName(Optional.empty(), prefix);
            return new QueryBundle(
                    Optional.of(temporaryTableName),
                    ImmutableList.of(),
                    new CreateTableAsSelect(
                            temporaryTableName,
                            (Query) statement,
                            false,
                            tablePropertyOverrides,
                            true,
                            Optional.of(generateStorageColumnAliases((Query) statement)),
                            Optional.empty()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    cluster);
        }

        throw new IllegalStateException(format("Unsupported query type: %s", statement.getClass()));
    }

    private QualifiedName generateTemporaryTableName(Optional<QualifiedName> originalName, QualifiedName prefix)
    {
        List<String> parts = new ArrayList<>();
        int originalSize = originalName.map(QualifiedName::getOriginalParts).map(List::size).orElse(0);
        int prefixSize = prefix.getOriginalParts().size();
        if (originalName.isPresent() && originalSize > prefixSize) {
            parts.addAll(originalName.get().getOriginalParts().subList(0, originalSize - prefixSize));
        }
        parts.addAll(prefix.getOriginalParts());
        parts.set(parts.size() - 1, prefix.getSuffix() + "_" + randomUUID().toString().replace("-", ""));
        return QualifiedName.of(parts);
    }

    private List<Identifier> generateStorageColumnAliases(Query query)
    {
        ImmutableList.Builder<Identifier> aliases = ImmutableList.builder();
        Set<String> usedAliases = new HashSet<>();

        for (String columnName : getColumnNames(query)) {
            columnName = sanitizeColumnName(columnName);
            String alias = columnName;
            int postfix = 1;
            while (usedAliases.contains(alias)) {
                alias = format("%s__%s", columnName, postfix);
                postfix++;
            }
            aliases.add(new Identifier(alias, true));
            usedAliases.add(alias);
        }
        return aliases.build();
    }

    private List<String> getColumnNames(Query query)
    {
        Query zeroRowQuery;
        if (query.getQueryBody() instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            zeroRowQuery = new Query(
                    query.getWith(),
                    new QuerySpecification(
                            querySpecification.getSelect(),
                            querySpecification.getFrom(),
                            querySpecification.getWhere(),
                            querySpecification.getGroupBy(),
                            querySpecification.getHaving(),
                            querySpecification.getOrderBy(),
                            Optional.of("0")),
                    Optional.empty(),
                    Optional.empty());
        }
        else {
            zeroRowQuery = new Query(query.getWith(), query.getQueryBody(), Optional.empty(), Optional.of("0"));
        }
        return prestoAction.execute(zeroRowQuery, REWRITE, ResultSetConverter.DEFAULT).getColumnNames();
    }

    private static String sanitizeColumnName(String columnName)
    {
        return columnName.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase(ENGLISH);
    }

    private static List<Property> applyPropertyOverride(List<Property> properties, List<Property> overrides)
    {
        Map<String, Expression> propertyMap = properties.stream()
                .collect(toImmutableMap(property -> property.getName().getValue().toLowerCase(ENGLISH), Property::getValue));
        Map<String, Expression> overrideMap = overrides.stream()
                .collect(toImmutableMap(property -> property.getName().getValue().toLowerCase(ENGLISH), Property::getValue));
        return Stream.concat(propertyMap.entrySet().stream(), overrideMap.entrySet().stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (original, override) -> override))
                .entrySet()
                .stream()
                .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                .collect(toImmutableList());
    }
}
