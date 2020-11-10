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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager.SessionPropertyValue;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowCreateFunction;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowRoleGrants;
import com.facebook.presto.sql.tree.ShowRoles;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.Values;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.Primitives;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_ENABLED_ROLES;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_ROLES;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLE_PRIVILEGES;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.metadata.MetadataListing.listCatalogs;
import static com.facebook.presto.metadata.MetadataListing.listSchemas;
import static com.facebook.presto.metadata.MetadataUtil.createCatalogSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedName;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.aliasedName;
import static com.facebook.presto.sql.QueryUtil.aliasedNullToEmpty;
import static com.facebook.presto.sql.QueryUtil.ascending;
import static com.facebook.presto.sql.QueryUtil.descending;
import static com.facebook.presto.sql.QueryUtil.equal;
import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.sql.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.QueryUtil.ordering;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectAll;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.singleValueQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Language;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause;
import static com.facebook.presto.sql.tree.ShowCreate.Type.TABLE;
import static com.facebook.presto.sql.tree.ShowCreate.Type.VIEW;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

final class ShowQueriesRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        return (Statement) new Visitor(metadata, parser, session, parameters, accessControl, queryExplainer, warningCollector).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        final List<Expression> parameters;
        private final AccessControl accessControl;
        private Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters, AccessControl accessControl, Optional<QueryExplainer> queryExplainer, WarningCollector warningCollector)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement(), null);
            return new Explain(
                    node.getLocation().get(),
                    node.isAnalyze(),
                    node.isVerbose(),
                    statement,
                    node.getOptions());
        }

        @Override
        protected Node visitShowTables(ShowTables showTables, Void context)
        {
            CatalogSchemaName schema = createCatalogSchemaName(session, showTables, showTables.getSchema());

            accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), schema);

            if (!metadata.catalogExists(session, schema.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, showTables, "Catalog '%s' does not exist", schema.getCatalogName());
            }

            if (!metadata.schemaExists(session, schema)) {
                throw new SemanticException(MISSING_SCHEMA, showTables, "Schema '%s' does not exist", schema.getSchemaName());
            }

            Expression predicate = equal(identifier("table_schema"), new StringLiteral(schema.getSchemaName()));

            Optional<String> likePattern = showTables.getLikePattern();
            if (likePattern.isPresent()) {
                Expression likePredicate = new LikePredicate(
                        identifier("table_name"),
                        new StringLiteral(likePattern.get()),
                        showTables.getEscape().map(StringLiteral::new));
                predicate = logicalAnd(predicate, likePredicate);
            }

            return simpleQuery(
                    selectList(aliasedName("table_name", "Table")),
                    from(schema.getCatalogName(), TABLE_TABLES),
                    predicate,
                    ordering(ascending("table_name")));
        }

        @Override
        protected Node visitShowGrants(ShowGrants showGrants, Void context)
        {
            String catalogName = session.getCatalog().orElse(null);
            Optional<Expression> predicate = Optional.empty();

            Optional<QualifiedName> tableName = showGrants.getTableName();
            if (tableName.isPresent()) {
                QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, showGrants, tableName.get());

                if (!metadata.getView(session, qualifiedTableName).isPresent() &&
                        !metadata.getTableHandle(session, qualifiedTableName).isPresent()) {
                    throw new SemanticException(MISSING_TABLE, showGrants, "Table '%s' does not exist", tableName);
                }

                catalogName = qualifiedTableName.getCatalogName();

                accessControl.checkCanShowTablesMetadata(
                        session.getRequiredTransactionId(),
                        session.getIdentity(),
                        session.getAccessControlContext(),
                        new CatalogSchemaName(catalogName, qualifiedTableName.getSchemaName()));

                predicate = Optional.of(combineConjuncts(
                        equal(identifier("table_schema"), new StringLiteral(qualifiedTableName.getSchemaName())),
                        equal(identifier("table_name"), new StringLiteral(qualifiedTableName.getObjectName()))));
            }
            else {
                if (catalogName == null) {
                    throw new SemanticException(CATALOG_NOT_SPECIFIED, showGrants, "Catalog must be specified when session catalog is not set");
                }

                Set<String> allowedSchemas = listSchemas(session, metadata, accessControl, catalogName);
                for (String schema : allowedSchemas) {
                    accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), new CatalogSchemaName(catalogName, schema));
                }
            }

            return simpleQuery(
                    selectList(
                            aliasedName("grantor", "Grantor"),
                            aliasedName("grantor_type", "Grantor Type"),
                            aliasedName("grantee", "Grantee"),
                            aliasedName("grantee_type", "Grantee Type"),
                            aliasedName("table_catalog", "Catalog"),
                            aliasedName("table_schema", "Schema"),
                            aliasedName("table_name", "Table"),
                            aliasedName("privilege_type", "Privilege"),
                            aliasedName("is_grantable", "Grantable"),
                            aliasedName("with_hierarchy", "With Hierarchy")),
                    from(catalogName, TABLE_TABLE_PRIVILEGES),
                    predicate,
                    Optional.empty());
        }

        @Override
        protected Node visitShowRoles(ShowRoles node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());

            if (node.isCurrent()) {
                accessControl.checkCanShowCurrentRoles(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ENABLED_ROLES));
            }
            else {
                accessControl.checkCanShowRoles(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ROLES));
            }
        }

        @Override
        protected Node visitShowRoleGrants(ShowRoleGrants node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());
            PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, session.getUser());

            accessControl.checkCanShowRoleGrants(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), catalog);
            List<Expression> rows = metadata.listRoleGrants(session, catalog, principal).stream()
                    .map(roleGrant -> row(new StringLiteral(roleGrant.getRoleName())))
                    .collect(toList());

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "role_grants", ImmutableList.of("Role Grants")),
                    ordering(ascending("Role Grants")));
        }

        @Override
        protected Node visitShowSchemas(ShowSchemas node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(Identifier::getValue).orElseGet(() -> session.getCatalog().get());
            accessControl.checkCanShowSchemas(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), catalog);

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(
                        identifier("schema_name"),
                        new StringLiteral(likePattern.get()),
                        node.getEscape().map(StringLiteral::new)));
            }

            return simpleQuery(
                    selectList(aliasedName("schema_name", "Schema")),
                    from(catalog, TABLE_SCHEMATA),
                    predicate,
                    Optional.of(ordering(ascending("schema_name"))));
        }

        @Override
        protected Node visitShowCatalogs(ShowCatalogs node, Void context)
        {
            List<Expression> rows = listCatalogs(session, metadata, accessControl).keySet().stream()
                    .map(name -> row(new StringLiteral(name)))
                    .collect(toList());

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(identifier("Catalog"), new StringLiteral(likePattern.get()), Optional.empty()));
            }

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "catalogs", ImmutableList.of("Catalog")),
                    predicate,
                    Optional.of(ordering(ascending("Catalog"))));
        }

        @Override
        protected Node visitShowColumns(ShowColumns showColumns, Void context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, showColumns, showColumns.getTable());

            if (!metadata.getView(session, tableName).isPresent() &&
                    !metadata.getTableHandle(session, tableName).isPresent()) {
                throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
            }

            return simpleQuery(
                    selectList(
                            aliasedName("column_name", "Column"),
                            aliasedName("data_type", "Type"),
                            aliasedNullToEmpty("extra_info", "Extra"),
                            aliasedNullToEmpty("comment", "Comment")),
                    from(tableName.getCatalogName(), TABLE_COLUMNS),
                    logicalAnd(
                            equal(identifier("table_schema"), new StringLiteral(tableName.getSchemaName())),
                            equal(identifier("table_name"), new StringLiteral(tableName.getObjectName()))),
                    ordering(ascending("ordinal_position")));
        }

        private static <T> Expression getExpression(PropertyMetadata<T> property, Object value)
                throws PrestoException
        {
            return toExpression(property.encode(property.getJavaType().cast(value)));
        }

        private static Expression toExpression(Object value)
                throws PrestoException
        {
            if (value instanceof String) {
                return new StringLiteral(value.toString());
            }

            if (value instanceof Boolean) {
                return new BooleanLiteral(value.toString());
            }

            if (value instanceof Long || value instanceof Integer) {
                return new LongLiteral(value.toString());
            }

            if (value instanceof Double) {
                return new DoubleLiteral(value.toString());
            }

            if (value instanceof List) {
                List<?> list = (List<?>) value;
                return new ArrayConstructor(list.stream()
                        .map(Visitor::toExpression)
                        .collect(toList()));
            }

            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Failed to convert object of type %s to expression: %s", value.getClass().getName(), value));
        }

        @Override
        protected Node visitShowCreate(ShowCreate node, Void context)
        {
            QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());
            Optional<ViewDefinition> viewDefinition = metadata.getView(session, objectName);

            if (node.getType() == VIEW) {
                if (!viewDefinition.isPresent()) {
                    if (metadata.getTableHandle(session, objectName).isPresent()) {
                        throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a table, not a view", objectName);
                    }
                    throw new SemanticException(MISSING_TABLE, node, "View '%s' does not exist", objectName);
                }

                Query query = parseView(viewDefinition.get().getOriginalSql(), objectName, node);
                String sql = formatSql(new CreateView(createQualifiedName(objectName), query, false, Optional.empty()), Optional.of(parameters)).trim();
                return singleValueQuery("Create View", sql);
            }

            if (node.getType() == TABLE) {
                if (viewDefinition.isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a view, not a table", objectName);
                }

                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, objectName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, node, "Table '%s' does not exist", objectName);
                }

                ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();

                Map<String, PropertyMetadata<?>> allColumnProperties = metadata.getColumnPropertyManager().getAllProperties().get(tableHandle.get().getConnectorId());

                List<TableElement> columns = connectorTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .map(column -> {
                            List<Property> propertyNodes = buildProperties(objectName, Optional.of(column.getName()), INVALID_COLUMN_PROPERTY, column.getProperties(), allColumnProperties);
                            return new ColumnDefinition(new Identifier(column.getName()), column.getType().getDisplayName(), column.isNullable(), propertyNodes, Optional.ofNullable(column.getComment()));
                        })
                        .collect(toImmutableList());

                Map<String, Object> properties = connectorTableMetadata.getProperties();
                Map<String, PropertyMetadata<?>> allTableProperties = metadata.getTablePropertyManager().getAllProperties().get(tableHandle.get().getConnectorId());
                List<Property> propertyNodes = buildProperties(objectName, Optional.empty(), INVALID_TABLE_PROPERTY, properties, allTableProperties);

                CreateTable createTable = new CreateTable(
                        QualifiedName.of(objectName.getCatalogName(), objectName.getSchemaName(), objectName.getObjectName()),
                        columns,
                        false,
                        propertyNodes,
                        connectorTableMetadata.getComment());
                return singleValueQuery("Create Table", formatSql(createTable, Optional.of(parameters)).trim());
            }

            throw new UnsupportedOperationException("SHOW CREATE only supported for tables and views");
        }

        @Override
        protected Node visitShowCreateFunction(ShowCreateFunction node, Void context)
        {
            QualifiedObjectName functionName = qualifyObjectName(node.getName());
            Collection<? extends SqlFunction> functions = metadata.getFunctionAndTypeManager().getFunctions(session.getTransactionId(), functionName);
            if (node.getParameterTypes().isPresent()) {
                List<TypeSignature> parameterTypes = node.getParameterTypes().get().stream()
                        .map(TypeSignature::parseTypeSignature)
                        .collect(toImmutableList());
                functions = functions.stream()
                        .filter(function -> function.getSignature().getArgumentTypes().equals(parameterTypes))
                        .collect(toImmutableList());
            }
            if (functions.isEmpty()) {
                String types = node.getParameterTypes().map(parameterTypes -> format("(%s)", Joiner.on(", ").join(parameterTypes))).orElse("");
                throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s%s", functionName, types));
            }

            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (SqlFunction function : functions) {
                if (!(function instanceof SqlInvokedFunction)) {
                    throw new PrestoException(GENERIC_USER_ERROR, "SHOW CREATE FUNCTION is only supported for SQL functions");
                }

                SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
                CreateFunction createFunction = new CreateFunction(
                        node.getName(),
                        false,
                        sqlFunction.getParameters().stream()
                                .map(parameter -> new SqlParameterDeclaration(new Identifier(parameter.getName()), parameter.getType().toString()))
                                .collect(toImmutableList()),
                        sqlFunction.getSignature().getReturnType().toString(),
                        Optional.of(sqlFunction.getDescription()),
                        new RoutineCharacteristics(
                                new Language(sqlFunction.getRoutineCharacteristics().getLanguage().getLanguage()),
                                Determinism.valueOf(sqlFunction.getRoutineCharacteristics().getDeterminism().name()),
                                NullCallClause.valueOf(sqlFunction.getRoutineCharacteristics().getNullCallClause().name())),
                        sqlParser.createReturn(sqlFunction.getBody(), createParsingOptions(session, warningCollector)));
                rows.add(row(
                        new StringLiteral(formatSql(createFunction, Optional.empty())),
                        new StringLiteral(function.getSignature().getArgumentTypes().stream()
                                .map(TypeSignature::toString)
                                .collect(joining(", ")))));
            }

            Map<String, String> columns = ImmutableMap.<String, String>builder()
                    .put("create_function", "Create Function")
                    .put("argument_types", "Argument Types")
                    .build();

            return simpleQuery(
                    selectAll(columns.entrySet().stream()
                            .map(entry -> aliasedName(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList())),
                    aliased(new Values(rows.build()), "functions", ImmutableList.copyOf(columns.keySet())),
                    ordering(ascending("argument_types")));
        }

        private List<Property> buildProperties(
                Object objectName,
                Optional<String> columnName,
                StandardErrorCode errorCode,
                Map<String, Object> properties,
                Map<String, PropertyMetadata<?>> allProperties)
        {
            if (properties.isEmpty()) {
                return Collections.emptyList();
            }

            ImmutableSortedMap.Builder<String, Expression> sqlProperties = ImmutableSortedMap.naturalOrder();

            for (Map.Entry<String, Object> propertyEntry : properties.entrySet()) {
                String propertyName = propertyEntry.getKey();
                Object value = propertyEntry.getValue();
                if (value == null) {
                    throw new PrestoException(errorCode, format("Property %s for %s cannot have a null value", propertyName, toQualifedName(objectName, columnName)));
                }

                PropertyMetadata<?> property = allProperties.get(propertyName);
                if (!Primitives.wrap(property.getJavaType()).isInstance(value)) {
                    throw new PrestoException(errorCode, format(
                            "Property %s for %s should have value of type %s, not %s",
                            propertyName,
                            toQualifedName(objectName, columnName),
                            property.getJavaType().getName(),
                            value.getClass().getName()));
                }

                Expression sqlExpression = getExpression(property, value);
                sqlProperties.put(propertyName, sqlExpression);
            }

            return sqlProperties.build().entrySet().stream()
                    .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                    .collect(toImmutableList());
        }

        private static String toQualifedName(Object objectName, Optional<String> columnName)
        {
            return columnName.map(s -> format("column %s of table %s", s, objectName))
                    .orElseGet(() -> "table " + objectName);
        }

        @Override
        protected Node visitShowFunctions(ShowFunctions node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (SqlFunction function : metadata.listFunctions(session)) {
                Signature signature = function.getSignature();
                boolean builtIn = signature.getName().getCatalogSchemaName().equals(DEFAULT_NAMESPACE);
                rows.add(row(
                        builtIn ? new StringLiteral(signature.getNameSuffix()) : new StringLiteral(signature.getName().toString()),
                        new StringLiteral(signature.getReturnType().toString()),
                        new StringLiteral(Joiner.on(", ").join(signature.getArgumentTypes())),
                        new StringLiteral(getFunctionType(function)),
                        function.isDeterministic() ? TRUE_LITERAL : FALSE_LITERAL,
                        new StringLiteral(nullToEmpty(function.getDescription())),
                        signature.isVariableArity() ? TRUE_LITERAL : FALSE_LITERAL,
                        builtIn ? TRUE_LITERAL : FALSE_LITERAL,
                        function instanceof SqlInvokedFunction
                                ? new StringLiteral(((SqlInvokedFunction) function).getRoutineCharacteristics().getLanguage().getLanguage().toLowerCase(ENGLISH))
                                : new StringLiteral("")));
            }

            Map<String, String> columns = ImmutableMap.<String, String>builder()
                    .put("function_name", "Function")
                    .put("return_type", "Return Type")
                    .put("argument_types", "Argument Types")
                    .put("function_type", "Function Type")
                    .put("deterministic", "Deterministic")
                    .put("description", "Description")
                    .put("variable_arity", "Variable Arity")
                    .put("built_in", "Built In")
                    .put("language", "Language")
                    .build();

            return simpleQuery(
                    selectAll(columns.entrySet().stream()
                            .map(entry -> aliasedName(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList())),
                    aliased(new Values(rows.build()), "functions", ImmutableList.copyOf(columns.keySet())),
                    node.getLikePattern().map(pattern -> new LikePredicate(
                            identifier("function_name"),
                            new StringLiteral(pattern),
                            node.getEscape().map(StringLiteral::new))),
                    Optional.of(ordering(
                            descending("built_in"),
                            new SortItem(
                                    functionCall("lower", identifier("function_name")),
                                    SortItem.Ordering.ASCENDING,
                                    SortItem.NullOrdering.UNDEFINED),
                            ascending("return_type"),
                            ascending("argument_types"),
                            ascending("function_type"))));
        }

        private static String getFunctionType(SqlFunction function)
        {
            FunctionKind kind = function.getSignature().getKind();
            switch (kind) {
                case AGGREGATE:
                    return "aggregate";
                case WINDOW:
                    return "window";
                case SCALAR:
                    return "scalar";
            }
            throw new IllegalArgumentException("Unsupported function kind: " + kind);
        }

        @Override

        protected Node visitShowSession(ShowSession node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            SortedMap<String, ConnectorId> catalogNames = listCatalogs(session, metadata, accessControl);
            List<SessionPropertyValue> sessionProperties = metadata.getSessionPropertyManager().getAllSessionProperties(session, catalogNames);
            for (SessionPropertyValue sessionProperty : sessionProperties) {
                if (sessionProperty.isHidden()) {
                    continue;
                }

                String value = sessionProperty.getValue();
                String defaultValue = sessionProperty.getDefaultValue();
                rows.add(row(
                        new StringLiteral(sessionProperty.getFullyQualifiedName()),
                        new StringLiteral(nullToEmpty(value)),
                        new StringLiteral(nullToEmpty(defaultValue)),
                        new StringLiteral(sessionProperty.getType()),
                        new StringLiteral(sessionProperty.getDescription()),
                        TRUE_LITERAL));
            }

            // add bogus row so we can support empty sessions
            rows.add(row(new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));

            return simpleQuery(
                    selectList(
                            aliasedName("name", "Name"),
                            aliasedName("value", "Value"),
                            aliasedName("default", "Default"),
                            aliasedName("type", "Type"),
                            aliasedName("description", "Description")),
                    aliased(
                            new Values(rows.build()),
                            "session",
                            ImmutableList.of("name", "value", "default", "type", "description", "include")),
                    identifier("include"));
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                Statement statement = sqlParser.createStatement(view, createParsingOptions(session, warningCollector));
                return (Query) statement;
            }
            catch (ParsingException e) {
                throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        private static Relation from(String catalog, SchemaTableName table)
        {
            return table(QualifiedName.of(catalog, table.getSchemaName(), table.getTableName()));
        }

        private static Optional<OrderBy> orderBy(List<SortItem> sortItems)
        {
            requireNonNull(sortItems, "sortItems is null");
            if (sortItems.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new OrderBy(sortItems));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
