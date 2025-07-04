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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AlterColumnNotNull;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.AlterRoutineCharacteristics;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ConstraintSpecification;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropConstraint;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropRole;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExternalBodyReference;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.GrantorSpecification;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.MergeCase;
import com.facebook.presto.sql.tree.MergeInsert;
import com.facebook.presto.sql.tree.MergeUpdate;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.PrincipalSpecification;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.RenameView;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
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
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.TruncateTable;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.UpdateAssignment;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionFormatter.formatGroupBy;
import static com.facebook.presto.sql.ExpressionFormatter.formatOrderBy;
import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType.UNIQUE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class SqlFormatter
{
    private static final String INDENT = "   ";
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private SqlFormatter() {}

    public static String formatSql(Node root, Optional<List<Expression>> parameters)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, parameters).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;
        private final Optional<List<Expression>> parameters;

        public Formatter(StringBuilder builder, Optional<List<Expression>> parameters)
        {
            this.builder = builder;
            this.parameters = parameters;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, parameters));
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(expression -> formatExpression(expression, parameters))
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent)
        {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitPrepare(Prepare node, Integer indent)
        {
            append(indent, "PREPARE ");
            builder.append(node.getName());
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent)
        {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent)
        {
            append(indent, "EXECUTE ");
            builder.append(node.getName());
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                Joiner.on(", ").appendTo(builder, parameters);
            }
            return null;
        }

        @Override
        protected Void visitDescribeOutput(DescribeOutput node, Integer indent)
        {
            append(indent, "DESCRIBE OUTPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitDescribeInput(DescribeInput node, Integer indent)
        {
            append(indent, "DESCRIBE INPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatExpression(query.getName(), parameters));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get(), parameters))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get(), parameters))
                        .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitOffset(Offset node, Integer indent)
        {
            append(indent, "OFFSET " + node.getRowCount() + " ROWS")
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, formatOrderBy(node, parameters))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression(), parameters));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(formatExpression(node.getAlias().get(), parameters));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
        {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(formatName(node.getName()));
            if (node.getTableVersionExpression().isPresent()) {
                builder.append(' ');
                process(node.getTableVersionExpression().get(), indent);
            }

            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                            .append(formatExpression(on.getExpression(), parameters));
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatExpression(node.getAlias(), parameters));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row, parameters));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    node.isDistinct().map(distinct -> distinct ? builder.append("DISTINCT ") : builder.append("ALL "));
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            node.isDistinct().map(distinct -> distinct ? builder.append("DISTINCT ") : builder.append("ALL "));

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    node.isDistinct().map(distinct -> distinct ? builder.append("DISTINCT ") : builder.append("ALL "));
                }
            }

            return null;
        }

        @Override
        protected Void visitMerge(Merge node, Integer indent)
        {
            builder.append("MERGE INTO ")
                    .append(node.getTargetTable().getName());

            node.getTargetAlias().ifPresent(value -> builder
                    .append(' ')
                    .append(value));
            builder.append("\n");

            append(indent + 1, "USING ");

            processRelation(node.getSource(), indent + 2);

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(formatExpression(node.getPredicate(), parameters));

            for (MergeCase mergeCase : node.getMergeCases()) {
                builder.append("\n");
                process(mergeCase, indent);
            }

            return null;
        }

        @Override
        protected Void visitMergeInsert(MergeInsert node, Integer indent)
        {
            appendMergeCaseWhen(false);
            append(indent + 1, "INSERT ");

            if (!node.getColumns().isEmpty()) {
                builder.append(node.getColumns().stream()
                        .map((Identifier expression) -> ExpressionFormatter.formatExpression(expression, parameters))
                        .collect(joining(", ", "(", ")")));
            }

            builder.append("\n");
            append(indent + 1, "VALUES ");
            builder.append(node.getValues().stream()
                    .map((Expression expression) -> ExpressionFormatter.formatExpression(expression, parameters))
                    .collect(joining(", ", "(", ")")));

            return null;
        }

        @Override
        protected Void visitMergeUpdate(MergeUpdate node, Integer indent)
        {
            appendMergeCaseWhen(true);
            append(indent + 1, "UPDATE SET");

            boolean first = true;
            for (MergeUpdate.Assignment assignment : node.getAssignments()) {
                builder.append("\n");
                append(indent + 2, first ? "  " : ", ");
                builder.append(assignment.getTarget())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue(), parameters));
                first = false;
            }

            return null;
        }

        private void appendMergeCaseWhen(boolean matched)
        {
            builder.append(matched ? "WHEN MATCHED" : "WHEN NOT MATCHED").append(" THEN\n");
        }

        @Override
        protected Void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(formatName(node.getName()));

            node.getSecurity().ifPresent(security ->
                    builder.append(" SECURITY ")
                            .append(security.toString()));

            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitCreateMaterializedView(CreateMaterializedView node, Integer indent)
        {
            builder.append("CREATE MATERIALIZED VIEW ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitCreateFunction(CreateFunction node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isTemporary()) {
                builder.append("TEMPORARY ");
            }
            builder.append("FUNCTION ")
                    .append(formatName(node.getFunctionName()))
                    .append(" ")
                    .append(formatSqlParameterDeclarations(node.getParameters()))
                    .append("\nRETURNS ")
                    .append(node.getReturnType());
            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT ")
                        .append(formatStringLiteral(node.getComment().get()));
            }
            builder.append("\n")
                    .append(formatRoutineCharacteristics(node.getCharacteristics()))
                    .append("\n");

            process(node.getBody(), 0);

            return null;
        }

        @Override
        protected Void visitAlterFunction(AlterFunction node, Integer indent)
        {
            builder.append("ALTER FUNCTION ")
                    .append(formatName(node.getFunctionName()));
            node.getParameterTypes().map(Formatter::formatTypeList).ifPresent(builder::append);
            builder.append("\n")
                    .append(formatAlterRoutineCharacteristics(node.getCharacteristics()));

            return null;
        }

        @Override
        protected Void visitDropFunction(DropFunction node, Integer indent)
        {
            builder.append("DROP ");
            if (node.isTemporary()) {
                builder.append("TEMPORARY ");
            }
            builder.append("FUNCTION ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getFunctionName()));
            node.getParameterTypes().map(Formatter::formatTypeList).ifPresent(builder::append);

            return null;
        }

        @Override
        protected Void visitReturn(Return node, Integer indent)
        {
            append(indent, "RETURN ");
            builder.append(formatExpression(node.getExpression(), parameters));

            return null;
        }

        @Override
        protected Void visitExternalBodyReference(ExternalBodyReference node, Integer indent)
        {
            append(indent, "EXTERNAL");
            if (node.getIdentifier().isPresent()) {
                builder.append(" NAME ");
                builder.append(node.getIdentifier().get().toString());
            }

            return null;
        }

        @Override
        protected Void visitRenameView(RenameView node, Integer context)
        {
            builder.append("ALTER VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitDropView(DropView node, Integer context)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitDropMaterializedView(DropMaterializedView node, Integer context)
        {
            builder.append("DROP MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitRefreshMaterializedView(RefreshMaterializedView node, Integer context)
        {
            builder.append("REFRESH MATERIALIZED VIEW ")
                    .append(formatName(node.getTarget().getName()))
                    .append(" WHERE ")
                    .append(formatExpression(node.getWhere(), parameters));

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");
            if (node.isAnalyze()) {
                builder.append("ANALYZE ");
            }

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context)
        {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value ->
                    builder.append(" FROM ")
                            .append(formatName(value)));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        protected Void visitSetProperties(SetProperties node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()));
            builder.append(" SET PROPERTIES ( ");
            builder.append(joinProperties(node.getProperties()));
            builder.append(" )");
            return null;
        }

        private String joinProperties(List<Property> properties)
        {
            return properties.stream()
                    .map(element -> formatExpression(element.getName(), Optional.empty()) + " = " +
                            formatExpression(element.getValue(), Optional.empty()))
                    .collect(joining(", "));
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer context)
        {
            if (node.getType() == ShowCreate.Type.TABLE) {
                builder.append("SHOW CREATE TABLE ")
                        .append(formatName(node.getName()));
            }
            else if (node.getType() == ShowCreate.Type.SCHEMA) {
                builder.append("SHOW CREATE SCHEMA ")
                        .append(formatName(node.getName()));
            }
            else if (node.getType() == ShowCreate.Type.VIEW) {
                builder.append("SHOW CREATE VIEW ")
                        .append(formatName(node.getName()));
            }
            else if (node.getType() == ShowCreate.Type.MATERIALIZED_VIEW) {
                builder.append("SHOW CREATE MATERIALIZED VIEW ")
                        .append(formatName(node.getName()));
            }

            return null;
        }

        @Override
        protected Void visitShowCreateFunction(ShowCreateFunction node, Integer context)
        {
            builder.append("SHOW CREATE FUNCTION ")
                    .append(formatName(node.getName()));
            node.getParameterTypes().map(Formatter::formatTypeList).ifPresent(builder::append);

            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable()));

            return null;
        }

        @Override
        protected Void visitShowStats(ShowStats node, Integer context)
        {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);
            builder.append("");
            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context)
        {
            builder.append("SHOW FUNCTIONS");

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitUse(Use node, Integer context)
        {
            builder.append("USE ");
            if (node.getCatalog().isPresent()) {
                builder.append(formatExpression(node.getCatalog().get(), Optional.empty()))
                        .append(".");
            }
            builder.append(formatExpression(node.getSchema(), Optional.empty()));

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer context)
        {
            builder.append("SHOW SESSION");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitDelete(Delete node, Integer context)
        {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get(), parameters));
            }

            return null;
        }

        @Override
        protected Void visitTruncateTable(TruncateTable node, Integer indent)
        {
            builder.append("TRUNCATE TABLE ");
            builder.append(formatName(node.getTableName()));

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer context)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer context)
        {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        @Override
        protected Void visitRenameSchema(RenameSchema node, Integer context)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatExpression(node.getTarget(), parameters));

            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            if (node.getColumnAliases().isPresent()) {
                String columnList = node.getColumnAliases().get().stream().map(element -> formatExpression(element, parameters)).collect(joining(", "));
                builder.append(format("( %s )", columnList));
            }

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName());
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition) {
                            ColumnDefinition column = (ColumnDefinition) element;
                            return elementIndent + formatColumnDefinition(column);
                        }
                        if (element instanceof LikeClause) {
                            LikeClause likeClause = (LikeClause) element;
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName()));
                            if (likeClause.getPropertiesOption().isPresent()) {
                                builder.append(" ")
                                        .append(likeClause.getPropertiesOption().get().name())
                                        .append(" PROPERTIES");
                            }
                            return builder.toString();
                        }
                        if (element instanceof ConstraintSpecification) {
                            ConstraintSpecification constraint = (ConstraintSpecification) element;
                            return elementIndent + processConstraintDefinition(constraint);
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        private String formatPropertiesMultiLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            formatExpression(element.getName(), parameters) + " = " +
                            formatExpression(element.getValue(), parameters))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatPropertiesSingleLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> formatExpression(element.getName(), parameters) + " = " +
                            formatExpression(element.getValue(), parameters))
                    .collect(joining(", "));

            return " WITH ( " + propertyList + " )";
        }

        private String formatSqlParameterDeclarations(List<SqlParameterDeclaration> parameters)
        {
            if (parameters.isEmpty()) {
                return "()";
            }
            return parameters.stream()
                    .map(parameter -> format(
                            "%s%s %s",
                            INDENT,
                            formatExpression(parameter.getName(), this.parameters),
                            parameter.getType()))
                    .collect(joining(",\n", "(\n", "\n)"));
        }

        private static String formatTypeList(List<String> types)
        {
            return format("(%s)", Joiner.on(", ").join(types));
        }

        private String formatRoutineCharacteristics(RoutineCharacteristics characteristics)
        {
            return Joiner.on("\n").join(ImmutableList.of(
                    "LANGUAGE " + characteristics.getLanguage(),
                    formatRoutineCharacteristicName(characteristics.getDeterminism()),
                    formatRoutineCharacteristicName(characteristics.getNullCallClause())));
        }

        private String formatAlterRoutineCharacteristics(AlterRoutineCharacteristics characteristics)
        {
            StringBuilder formatted = new StringBuilder();
            if (characteristics.getNullCallClause().isPresent()) {
                formatted.append(formatRoutineCharacteristicName(characteristics.getNullCallClause().get()));
            }
            return formatted.toString();
        }

        private String formatRoutineCharacteristicName(Enum characteristic)
        {
            return characteristic.name().replace("_", " ");
        }

        private static String formatName(String name)
        {
            if (NAME_PATTERN.matcher(name).matches()) {
                return name;
            }
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }

        private static String formatName(QualifiedName name)
        {
            return name.getOriginalParts().stream()
                    .map(Formatter::formatName)
                    .collect(joining("."));
        }

        private String formatColumnDefinition(ColumnDefinition column)
        {
            StringBuilder sb = new StringBuilder()
                    .append(formatExpression(column.getName(), parameters))
                    .append(" ").append(column.getType());
            if (!column.isNullable()) {
                sb.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment ->
                    sb.append(" COMMENT ").append(formatStringLiteral(comment)));
            sb.append(formatPropertiesSingleLine(column.getProperties()));
            return sb.toString();
        }

        private static String formatGrantor(GrantorSpecification grantor)
        {
            GrantorSpecification.Type type = grantor.getType();
            switch (type) {
                case CURRENT_ROLE:
                case CURRENT_USER:
                    return type.name();
                case PRINCIPAL:
                    return formatPrincipal(grantor.getPrincipal().get());
                default:
                    throw new IllegalArgumentException("Unsupported principal type: " + type);
            }
        }

        private static String formatPrincipal(PrincipalSpecification principal)
        {
            PrincipalSpecification.Type type = principal.getType();
            switch (type) {
                case UNSPECIFIED:
                    return principal.getName().toString();
                case USER:
                case ROLE:
                    return format("%s %s", type.name(), principal.getName().toString());
                default:
                    throw new IllegalArgumentException("Unsupported principal type: " + type);
            }
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitRenameColumn(RenameColumn node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTable())
                    .append(" RENAME COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitDropColumn(DropColumn node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" DROP COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatExpression(node.getColumn(), parameters));

            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Integer context)
        {
            builder.append("ANALYZE ")
                    .append(formatName(node.getTableName()));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            return null;
        }

        @Override
        protected Void visitAddColumn(AddColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName())
                    .append(" ADD COLUMN ");
            if (node.isColumnNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatColumnDefinition(node.getColumn()));

            return null;
        }

        @Override
        protected Void visitInsert(Insert node, Integer indent)
        {
            builder.append("INSERT INTO ")
                    .append(node.getTarget());

            if (node.getColumns().isPresent()) {
                builder.append(" (")
                        .append(Joiner.on(", ").join(node.getColumns().get()))
                        .append(")");
            }

            builder.append("\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitUpdate(Update node, Integer indent)
        {
            builder.append("UPDATE ")
                    .append(node.getTable().getName())
                    .append(" SET");
            int setCounter = node.getAssignments().size() - 1;
            for (UpdateAssignment assignment : node.getAssignments()) {
                builder.append("\n")
                        .append(indentString(indent + 1))
                        .append(assignment.getName().getValue())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue(), parameters));
                if (setCounter > 0) {
                    builder.append(",");
                }
                setCounter--;
            }
            if (node.getWhere().isPresent()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append("WHERE ").append(formatExpression(node.getWhere().get(), parameters));
            }
            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer context)
        {
            builder.append("SET SESSION ")
                    .append(node.getName())
                    .append(" = ")
                    .append(formatExpression(node.getValue(), parameters));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer context)
        {
            builder.append("RESET SESSION ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent)
        {
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" => ");
            }
            builder.append(formatExpression(node.getValue(), parameters));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(node.getName())
                    .append("(");

            Iterator<CallArgument> arguments = node.getArguments().iterator();
            while (arguments.hasNext()) {
                process(arguments.next(), indent);
                if (arguments.hasNext()) {
                    builder.append(", ");
                }
            }

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitStartTransaction(StartTransaction node, Integer indent)
        {
            builder.append("START TRANSACTION");

            Iterator<TransactionMode> iterator = node.getTransactionModes().iterator();
            while (iterator.hasNext()) {
                builder.append(" ");
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            return null;
        }

        @Override
        protected Void visitIsolationLevel(Isolation node, Integer indent)
        {
            builder.append("ISOLATION LEVEL ").append(node.getLevel().getText());
            return null;
        }

        @Override
        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer context)
        {
            builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
            return null;
        }

        @Override
        protected Void visitCommit(Commit node, Integer context)
        {
            builder.append("COMMIT");
            return null;
        }

        @Override
        protected Void visitRollback(Rollback node, Integer context)
        {
            builder.append("ROLLBACK");
            return null;
        }

        @Override
        protected Void visitCreateRole(CreateRole node, Integer context)
        {
            builder.append("CREATE ROLE ").append(node.getName());
            if (node.getGrantor().isPresent()) {
                builder.append(" WITH ADMIN ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitDropRole(DropRole node, Integer context)
        {
            builder.append("DROP ROLE ").append(node.getName());
            return null;
        }

        @Override
        protected Void visitGrantRoles(GrantRoles node, Integer context)
        {
            builder.append("GRANT ");
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" TO ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            if (node.isWithAdminOption()) {
                builder.append(" WITH ADMIN OPTION");
            }
            if (node.getGrantor().isPresent()) {
                builder.append(" GRANTED BY ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitRevokeRoles(RevokeRoles node, Integer context)
        {
            builder.append("REVOKE ");
            if (node.isAdminOptionFor()) {
                builder.append("ADMIN OPTION FOR ");
            }
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" FROM ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            if (node.getGrantor().isPresent()) {
                builder.append(" GRANTED BY ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitSetRole(SetRole node, Integer context)
        {
            builder.append("SET ROLE ");
            SetRole.Type type = node.getType();
            switch (type) {
                case ALL:
                case NONE:
                    builder.append(type.toString());
                    break;
                case ROLE:
                    builder.append(node.getRole().get());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
            return null;
        }

        @Override
        public Void visitGrant(Grant node, Integer indent)
        {
            builder.append("GRANT ");

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.isTable()) {
                builder.append("TABLE ");
            }
            builder.append(node.getTableName())
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }

            return null;
        }

        @Override
        public Void visitRevoke(Revoke node, Integer indent)
        {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.isTable()) {
                builder.append("TABLE ");
            }
            builder.append(node.getTableName())
                    .append(" FROM ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitShowGrants(ShowGrants node, Integer indent)
        {
            builder.append("SHOW GRANTS ");

            if (node.getTableName().isPresent()) {
                builder.append("ON ");

                if (node.getTable()) {
                    builder.append("TABLE ");
                }
                builder.append(node.getTableName().get());
            }

            return null;
        }

        @Override
        protected Void visitShowRoles(ShowRoles node, Integer context)
        {
            builder.append("SHOW ");
            if (node.isCurrent()) {
                builder.append("CURRENT ");
            }
            builder.append("ROLES");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowRoleGrants(ShowRoleGrants node, Integer context)
        {
            builder.append("SHOW ROLE GRANTS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitDropConstraint(DropConstraint node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()))
                    .append(" DROP CONSTRAINT ");
            if (node.isConstraintExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getConstraintName());

            return null;
        }

        @Override
        protected Void visitAddConstraint(AddConstraint node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTableName().toString());
            builder.append(" ADD ");
            builder.append(processConstraintDefinition(node.getConstraintSpecification()));

            return null;
        }

        @Override
        protected Void visitAlterColumnNotNull(AlterColumnNotNull node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTable().toString());
            builder.append(" ALTER COLUMN ");
            builder.append(node.getColumn());
            builder.append(" ");
            if (node.isDropConstraint()) {
                builder.append("DROP");
            }
            else {
                builder.append("SET");
            }
            builder.append(" NOT NULL");
            return null;
        }

        private String processConstraintDefinition(ConstraintSpecification node)
        {
            StringBuilder sb = new StringBuilder();
            if (node.getConstraintName().isPresent()) {
                sb.append("CONSTRAINT ");
                sb.append(node.getConstraintName().get());
                sb.append(" ");
            }
            sb.append(node.getConstraintType() == UNIQUE ? "UNIQUE " : "PRIMARY KEY ");
            sb.append("(");
            Iterator<String> columns = node.getColumns().iterator();
            while (columns.hasNext()) {
                sb.append(columns.next());
                if (columns.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            if (!node.isEnabled()) {
                sb.append(" DISABLED");
            }
            if (!node.isRely()) {
                sb.append(" NOT RELY");
            }
            if (!node.isEnforced()) {
                sb.append(" NOT ENFORCED");
            }
            return sb.toString();
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(name -> formatExpression(name, Optional.empty()))
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
