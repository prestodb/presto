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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.framework.ClusterType;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RowType.Field;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static com.facebook.presto.sql.tree.ShowCreate.Type.VIEW;
import static com.facebook.presto.verifier.framework.CreateViewVerification.SHOW_CREATE_VIEW_CONVERTER;
import static com.facebook.presto.verifier.framework.QueryStage.REWRITE;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.framework.VerifierUtil.getColumnNames;
import static com.facebook.presto.verifier.framework.VerifierUtil.getColumnTypes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class QueryRewriter
{
    private final SqlParser sqlParser;
    private final TypeManager typeManager;
    private final PrestoAction prestoAction;
    private final Map<ClusterType, QualifiedName> prefixes;
    private final Map<ClusterType, List<Property>> tableProperties;

    public QueryRewriter(
            SqlParser sqlParser,
            TypeManager typeManager,
            PrestoAction prestoAction,
            Map<ClusterType, QualifiedName> tablePrefixes,
            Map<ClusterType, List<Property>> tableProperties)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.prefixes = ImmutableMap.copyOf(tablePrefixes);
        this.tableProperties = ImmutableMap.copyOf(tableProperties);
    }

    public QueryObjectBundle rewriteQuery(@Language("SQL") String query, ClusterType clusterType)
    {
        checkState(prefixes.containsKey(clusterType), "Unsupported cluster type: %s", clusterType);
        Statement statement = sqlParser.createStatement(query, PARSING_OPTIONS);

        QualifiedName prefix = prefixes.get(clusterType);
        List<Property> properties = tableProperties.get(clusterType);
        if (statement instanceof CreateTableAsSelect) {
            CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
            QualifiedName temporaryTableName = generateTemporaryName(Optional.of(createTableAsSelect.getName()), prefix);
            return new QueryObjectBundle(
                    temporaryTableName,
                    ImmutableList.of(),
                    new CreateTableAsSelect(
                            temporaryTableName,
                            createTableAsSelect.getQuery(),
                            createTableAsSelect.isNotExists(),
                            applyPropertyOverride(createTableAsSelect.getProperties(), properties),
                            createTableAsSelect.isWithData(),
                            createTableAsSelect.getColumnAliases(),
                            createTableAsSelect.getComment()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    clusterType);
        }
        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            QualifiedName originalTableName = insert.getTarget();
            QualifiedName temporaryTableName = generateTemporaryName(Optional.of(originalTableName), prefix);
            return new QueryObjectBundle(
                    temporaryTableName,
                    ImmutableList.of(
                            new CreateTable(
                                    temporaryTableName,
                                    ImmutableList.of(new LikeClause(originalTableName, Optional.of(INCLUDING))),
                                    false,
                                    properties,
                                    Optional.empty())),
                    new Insert(
                            temporaryTableName,
                            insert.getColumns(),
                            insert.getQuery()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    clusterType);
        }
        if (statement instanceof Query) {
            QualifiedName temporaryTableName = generateTemporaryName(Optional.empty(), prefix);
            ResultSetMetaData metadata = getResultMetadata((Query) statement);
            List<Identifier> columnAliases = generateStorageColumnAliases(metadata);
            Query rewrite = rewriteNonStorableColumns((Query) statement, metadata);
            return new QueryObjectBundle(
                    temporaryTableName,
                    ImmutableList.of(),
                    new CreateTableAsSelect(
                            temporaryTableName,
                            rewrite,
                            false,
                            properties,
                            true,
                            Optional.of(columnAliases),
                            Optional.empty()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    clusterType);
        }
        if (statement instanceof CreateView) {
            CreateView createView = (CreateView) statement;
            QualifiedName temporaryViewName = generateTemporaryName(Optional.empty(), prefix);
            ImmutableList.Builder<Statement> setupQueries = ImmutableList.builder();

            // Check to see if there is an existing view with the specified view name.
            // If view exists, create a temporary view that are has the same definition as the existing view.
            // Otherwise, do not pre-create temporary view.
            try {
                String createExistingViewQuery = getOnlyElement(prestoAction.execute(
                        new ShowCreate(VIEW, createView.getName()),
                        REWRITE,
                        SHOW_CREATE_VIEW_CONVERTER).getResults());
                CreateView createExistingView = (CreateView) sqlParser.createStatement(createExistingViewQuery, PARSING_OPTIONS);
                setupQueries.add(new CreateView(
                        temporaryViewName,
                        createExistingView.getQuery(),
                        false,
                        createExistingView.getSecurity()));
            }
            catch (QueryException e) {
                // no-op
            }
            return new QueryObjectBundle(
                    temporaryViewName,
                    setupQueries.build(),
                    new CreateView(
                            temporaryViewName,
                            createView.getQuery(),
                            createView.isReplace(),
                            createView.getSecurity()),
                    ImmutableList.of(new DropView(temporaryViewName, true)),
                    clusterType);
        }
        if (statement instanceof CreateTable) {
            CreateTable createTable = (CreateTable) statement;
            QualifiedName temporaryTableName = generateTemporaryName(Optional.empty(), prefix);
            return new QueryObjectBundle(
                    temporaryTableName,
                    ImmutableList.of(),
                    new CreateTable(
                            temporaryTableName,
                            createTable.getElements(),
                            createTable.isNotExists(),
                            applyPropertyOverride(createTable.getProperties(), properties),
                            createTable.getComment()),
                    ImmutableList.of(new DropTable(temporaryTableName, true)),
                    clusterType);
        }

        throw new IllegalStateException(format("Unsupported query type: %s", statement.getClass()));
    }

    private QualifiedName generateTemporaryName(Optional<QualifiedName> originalName, QualifiedName prefix)
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

    private List<Identifier> generateStorageColumnAliases(ResultSetMetaData metadata)
    {
        ImmutableList.Builder<Identifier> aliases = ImmutableList.builder();
        Set<String> usedAliases = new HashSet<>();

        for (String columnName : getColumnNames(metadata)) {
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

    private ResultSetMetaData getResultMetadata(Query query)
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
        return prestoAction.execute(zeroRowQuery, REWRITE, ResultSetConverter.DEFAULT).getMetadata();
    }

    private Query rewriteNonStorableColumns(Query query, ResultSetMetaData metadata)
    {
        // Skip if all columns are storable
        List<Type> columnTypes = getColumnTypes(typeManager, metadata);
        if (columnTypes.stream().noneMatch(type -> getColumnTypeRewrite(type).isPresent())) {
            return query;
        }

        // Cannot handle SELECT query with top-level SetOperation
        if (!(query.getQueryBody() instanceof QuerySpecification)) {
            return query;
        }

        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
        // Cannot handle SELECT *
        if (selectItems.stream().anyMatch(AllColumns.class::isInstance)) {
            return query;
        }

        List<SelectItem> newItems = new ArrayList<>();
        checkState(selectItems.size() == columnTypes.size(), "SelectItem count (%s) mismatches column count (%s)", selectItems.size(), columnTypes.size());
        for (int i = 0; i < selectItems.size(); i++) {
            SingleColumn singleColumn = (SingleColumn) selectItems.get(i);
            Optional<Type> columnTypeRewrite = getColumnTypeRewrite(columnTypes.get(i));
            if (columnTypeRewrite.isPresent()) {
                newItems.add(new SingleColumn(new Cast(singleColumn.getExpression(), columnTypeRewrite.get().getTypeSignature().toString()), singleColumn.getAlias()));
            }
            else {
                newItems.add(singleColumn);
            }
        }

        return new Query(
                query.getWith(),
                new QuerySpecification(
                        new Select(querySpecification.getSelect().isDistinct(), newItems),
                        querySpecification.getFrom(),
                        querySpecification.getWhere(),
                        querySpecification.getGroupBy(),
                        querySpecification.getHaving(),
                        querySpecification.getOrderBy(),
                        querySpecification.getLimit()),
                query.getOrderBy(),
                query.getLimit());
    }

    private Optional<Type> getColumnTypeRewrite(Type type)
    {
        if (type.equals(DATE) || type.equals(TIME)) {
            return Optional.of(TIMESTAMP);
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Optional.of(VARCHAR);
        }
        if (type.equals(UNKNOWN)) {
            return Optional.of(BIGINT);
        }
        if (type instanceof DecimalType) {
            return Optional.of(DOUBLE);
        }
        if (type instanceof ArrayType) {
            return getColumnTypeRewrite(((ArrayType) type).getElementType()).map(ArrayType::new);
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            Optional<Type> keyTypeRewrite = getColumnTypeRewrite(keyType);
            Optional<Type> valueTypeRewrite = getColumnTypeRewrite(valueType);
            if (keyTypeRewrite.isPresent() || valueTypeRewrite.isPresent()) {
                return Optional.of(typeManager.getType(new TypeSignature(
                        MAP,
                        TypeSignatureParameter.of(keyTypeRewrite.orElse(keyType).getTypeSignature()),
                        TypeSignatureParameter.of(valueTypeRewrite.orElse(valueType).getTypeSignature()))));
            }
            return Optional.empty();
        }
        if (type instanceof RowType) {
            List<Field> fields = ((RowType) type).getFields();
            List<Field> fieldsRewrite = new ArrayList<>();
            boolean rewrite = false;
            for (Field field : fields) {
                Optional<Type> fieldTypeRewrite = getColumnTypeRewrite(field.getType());
                rewrite = rewrite || fieldTypeRewrite.isPresent();
                fieldsRewrite.add(new Field(field.getName(), fieldTypeRewrite.orElse(field.getType())));
            }
            return rewrite ? Optional.of(RowType.from(fieldsRewrite)) : Optional.empty();
        }
        return Optional.empty();
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
