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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ProjectionPushDown
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        /**
         * Convert a plan of the shape ... -> Project -> Union -> ... to ... -> Union -> Project -> ...
         */
        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            // If we have a Project on a Union, push the Project through the Union
            PlanNode source = context.rewrite(node.getSource());

            if (!(source instanceof UnionNode)) {
                return context.replaceChildren(node, ImmutableList.of(source));
            }

            UnionNode unionNode = (UnionNode) source;

            // OutputLayout of the resultant Union, will be same as the layout of the Project
            List<Symbol> outputLayout = node.getOutputSymbols();

            // Mapping from the output symbol to ordered list of symbols from each of the sources
            ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();

            // sources for the resultant UnionNode
            ImmutableList.Builder<PlanNode> outputSources = ImmutableList.builder();

            for (int i = 0; i < unionNode.getSources().size(); i++) {
                Map<Symbol, QualifiedNameReference> outputToInput = unionNode.sourceSymbolMap(i);   // Map: output of union -> input of this source to the union
                ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();      // assignments for the new ProjectNode

                // mapping from current ProjectNode to new ProjectNode, used to identify the output layout
                Map<Symbol, Symbol> projectSymbolMapping = new HashMap<>();

                // Translate the assignments in the ProjectNode using symbols of the source of the UnionNode
                for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                    Expression translatedExpression = translateExpression(entry.getValue(), outputToInput);
                    Type type = symbolAllocator.getTypes().get(entry.getKey());
                    Symbol symbol = symbolAllocator.newSymbol(translatedExpression, type);
                    assignments.put(symbol, translatedExpression);
                    projectSymbolMapping.put(entry.getKey(), symbol);
                }
                outputSources.add(new ProjectNode(idAllocator.getNextId(), unionNode.getSources().get(i), assignments.build()));
                outputLayout.forEach(symbol -> mappings.put(symbol, projectSymbolMapping.get(symbol)));
            }

            return new UnionNode(node.getId(), outputSources.build(), mappings.build());
        }
    }

    private static Expression translateExpression(Expression inputExpression, Map<Symbol, QualifiedNameReference> symbolMapping)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                QualifiedNameReference qualifiedNameReference = symbolMapping.get(Symbol.fromQualifiedName(node.getName()));
                checkState(qualifiedNameReference != null, "Cannot resolve symbol %s", node.getName());

                return qualifiedNameReference;
            }
        }, inputExpression);
    }
}
