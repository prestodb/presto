package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.compiler.Field.nameGetter;
import static java.lang.String.format;

public class SemanticAnalyzer
{
    private final Metadata metadata;
    private final SymbolTable symbols;

    public SemanticAnalyzer(Metadata metadata, SymbolTable symbols)
    {
        this.metadata = metadata;
        this.symbols = symbols;
    }

    public SemanticAnalyzer(Metadata metadata)
    {
        this(metadata, new SymbolTable());
    }

    public AnalysisResult analyze(Node node)
    {
        Visitor analyzer = new Visitor();
        analyzer.process(node);

        Schema schema = analyzer.getTypes().get(node);

        return new AnalysisResult(schema, analyzer.getResolvedNames(), analyzer.getTypes());
    }

    private Schema inferTypes(Select select)
    {
        return new Schema(ImmutableList.copyOf(Iterables.transform(select.getSelectItems(), new Function<Expression, Field>()
        {
            @Override
            public Field apply(Expression input)
            {
                QualifiedName name = null;

                if (input instanceof AliasedExpression) {
                    name = QualifiedName.of(((AliasedExpression) input).getAlias());
                }
                else if (input instanceof QualifiedNameReference) {
                    name = ((QualifiedNameReference) input).getSuffix();
                }

                return new Field(name, null);
            }
        })));
    }

    private class Visitor
            extends AstVisitor<Void, Void>
    {
        private final IdentityHashMap<Node, Schema> types = new IdentityHashMap<>();
        private final IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNames = new IdentityHashMap<>();

        public IdentityHashMap<Node, Schema> getTypes()
        {
            return types;
        }

        public IdentityHashMap<QualifiedNameReference, QualifiedName> getResolvedNames()
        {
            return resolvedNames;
        }

        @Override
        protected Void visitQuery(Query query, Void context)
        {
            // analyze FROM clause
            Set<QualifiedName> exported = new HashSet<>();
            for (Relation relation : query.getFrom()) {
                SemanticAnalyzer analyzer = new SemanticAnalyzer(metadata, symbols);
                AnalysisResult analysis = analyzer.analyze(relation);

                resolvedNames.putAll(analysis.getResolvedNames());
                types.putAll(analysis.getTypes());

                Iterables.addAll(exported, Iterables.transform(analysis.getType().getFields(), nameGetter()));
            }

            SymbolTable newSymbols = new SymbolTable(symbols, exported);

            // resolve unqualified references in all expressions except for nested queries (e.g, IN clause)
            ReferenceResolver resolver = new ReferenceResolver(newSymbols);

            if (query.getWhere() != null) {
                resolver.resolve(query.getWhere());
            }
            resolver.resolve(query.getSelect());

            for (Expression expression : query.getGroupBy()) {
                resolver.resolve(expression);
            }

            if (query.getHaving() != null) {
                resolver.resolve(query.getHaving());
            }

            for (SortItem sortItem : query.getOrderBy()) {
                resolver.resolve(sortItem);
            }
            resolvedNames.putAll(resolver.getResolvedNames());

            // analyze nested queries
            if (query.getWhere() != null) {
                NestedQueryExtractor extractor = new NestedQueryExtractor();
                for (Query nested : extractor.extract(query.getWhere())) {
                    SemanticAnalyzer analyzer = new SemanticAnalyzer(metadata, newSymbols);
                    AnalysisResult analysis = analyzer.analyze(nested);
                    resolvedNames.putAll(analysis.getResolvedNames());
                    types.putAll(analysis.getTypes());
                }
            }

            types.put(query, inferTypes(query.getSelect()));

            // TODO: infer types

            return null;
        }


        @Override
        protected Void visitSubquery(Subquery subquery, Void context)
        {
            process(subquery.getQuery());

            types.put(subquery, types.get(subquery.getQuery()));
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression subquery, Void context)
        {
            process(subquery.getQuery());

            types.put(subquery, types.get(subquery.getQuery()));
            return null;
        }

        @Override
        protected Void visitTable(Table table, Void context)
        {
            TableMetadata tableMetadata = metadata.getTable(table.getName());

            if (tableMetadata == null) {
                throw new SemanticException(format("Cannot resolve table '%s'", table.getName()), table);
            }

            ImmutableList.Builder<Field> names = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                names.add(new Field(QualifiedName.of(tableMetadata.getName(), column.getName()), null));
            }

            types.put(table, new Schema(names.build()));
            return null;
        }

        @Override
        protected Void visitAliasedRelation(final AliasedRelation relation, Void context)
        {
            process(relation.getRelation());

            Schema relationSchema = types.get(relation.getRelation());

            ImmutableList.Builder<Field> builder = ImmutableList.builder();
            int count = 0;
            for (Field field : relationSchema.getFields()) {
                QualifiedName name;
                if (field.getName() == null) {
                    name = QualifiedName.of(relation.getAlias(), "$" + count);
                }
                else {
                    name = QualifiedName.of(relation.getAlias(), Iterables.getLast(field.getName().getParts()));
                }
                builder.add(new Field(name, field.getType()));
                count++;
            }

            types.put(relation, new Schema(builder.build()));
            return null;
        }
    }
}
