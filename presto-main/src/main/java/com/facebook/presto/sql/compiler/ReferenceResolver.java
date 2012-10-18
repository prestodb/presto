package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

class ReferenceResolver
{
    private final SymbolTable symbols;

    private final IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNames = new IdentityHashMap<>();

    public ReferenceResolver(SymbolTable symbols)
    {
        this.symbols = symbols;
    }

    public void resolve(Node node)
    {
        AstVisitor<Void, Void> visitor = new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
            {
                List<QualifiedName> matches = symbols.resolve(node.getName());
                if (matches.isEmpty()) {
                    throw new SemanticException(format("Attribute '%s' cannot be resolved", node.getName()), node);
                }
                else if (matches.size() > 1) {
                    throw new SemanticException(format("Attribute '%s' is ambiguous. Possible targets: %s", node.getName(), matches), node);
                }

                resolvedNames.put(node, Iterables.getOnlyElement(matches));
                return null;
            }

            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
            {
                // don't recurse. This will be taken care of in another phase that analyzes nested queries
                return null;
            }
        };

        visitor.process(node, null);
    }

    public Map<QualifiedNameReference, QualifiedName> getResolvedNames()
    {
        return Collections.unmodifiableMap(resolvedNames);
    }
}
