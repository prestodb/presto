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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.json.ir.IrAbsMethod;
import com.facebook.presto.json.ir.IrArithmeticBinary;
import com.facebook.presto.json.ir.IrArithmeticBinary.Operator;
import com.facebook.presto.json.ir.IrArithmeticUnary;
import com.facebook.presto.json.ir.IrArrayAccessor;
import com.facebook.presto.json.ir.IrArrayAccessor.Subscript;
import com.facebook.presto.json.ir.IrCeilingMethod;
import com.facebook.presto.json.ir.IrComparisonPredicate;
import com.facebook.presto.json.ir.IrConjunctionPredicate;
import com.facebook.presto.json.ir.IrContextVariable;
import com.facebook.presto.json.ir.IrDisjunctionPredicate;
import com.facebook.presto.json.ir.IrDoubleMethod;
import com.facebook.presto.json.ir.IrExistsPredicate;
import com.facebook.presto.json.ir.IrFilter;
import com.facebook.presto.json.ir.IrFloorMethod;
import com.facebook.presto.json.ir.IrIsUnknownPredicate;
import com.facebook.presto.json.ir.IrJsonNull;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.json.ir.IrKeyValueMethod;
import com.facebook.presto.json.ir.IrLastIndexVariable;
import com.facebook.presto.json.ir.IrLiteral;
import com.facebook.presto.json.ir.IrMemberAccessor;
import com.facebook.presto.json.ir.IrNamedJsonVariable;
import com.facebook.presto.json.ir.IrNamedValueVariable;
import com.facebook.presto.json.ir.IrNegationPredicate;
import com.facebook.presto.json.ir.IrPathNode;
import com.facebook.presto.json.ir.IrPredicate;
import com.facebook.presto.json.ir.IrPredicateCurrentItemVariable;
import com.facebook.presto.json.ir.IrSizeMethod;
import com.facebook.presto.json.ir.IrStartsWithPredicate;
import com.facebook.presto.json.ir.IrTypeMethod;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.sql.analyzer.JsonPathAnalyzer;
import com.facebook.presto.sql.jsonpath.PathNodeRef;
import com.facebook.presto.sql.jsonpath.tree.AbsMethod;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor;
import com.facebook.presto.sql.jsonpath.tree.CeilingMethod;
import com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate;
import com.facebook.presto.sql.jsonpath.tree.ConjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.ContextVariable;
import com.facebook.presto.sql.jsonpath.tree.DatetimeMethod;
import com.facebook.presto.sql.jsonpath.tree.DisjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.DoubleMethod;
import com.facebook.presto.sql.jsonpath.tree.ExistsPredicate;
import com.facebook.presto.sql.jsonpath.tree.Filter;
import com.facebook.presto.sql.jsonpath.tree.FloorMethod;
import com.facebook.presto.sql.jsonpath.tree.IsUnknownPredicate;
import com.facebook.presto.sql.jsonpath.tree.JsonNullLiteral;
import com.facebook.presto.sql.jsonpath.tree.JsonPathTreeVisitor;
import com.facebook.presto.sql.jsonpath.tree.KeyValueMethod;
import com.facebook.presto.sql.jsonpath.tree.LastIndexVariable;
import com.facebook.presto.sql.jsonpath.tree.LikeRegexPredicate;
import com.facebook.presto.sql.jsonpath.tree.MemberAccessor;
import com.facebook.presto.sql.jsonpath.tree.NamedVariable;
import com.facebook.presto.sql.jsonpath.tree.NegationPredicate;
import com.facebook.presto.sql.jsonpath.tree.PathNode;
import com.facebook.presto.sql.jsonpath.tree.PredicateCurrentItemVariable;
import com.facebook.presto.sql.jsonpath.tree.SizeMethod;
import com.facebook.presto.sql.jsonpath.tree.SqlValueLiteral;
import com.facebook.presto.sql.jsonpath.tree.StartsWithPredicate;
import com.facebook.presto.sql.jsonpath.tree.TypeMethod;
import com.facebook.presto.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.ADD;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.DIVIDE;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.MODULUS;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.MULTIPLY;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.SUBTRACT;
import static com.facebook.presto.json.ir.IrArithmeticUnary.Sign.MINUS;
import static com.facebook.presto.json.ir.IrArithmeticUnary.Sign.PLUS;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.EQUAL;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.GREATER_THAN;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.LESS_THAN;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.json.ir.IrComparisonPredicate.Operator.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonPathTranslator
{
    private final Session session;
    private final FunctionAndTypeManager functionAndTypeManager;

    public JsonPathTranslator(Session session, FunctionAndTypeManager functionAndTypeManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public IrJsonPath rewriteToIr(JsonPathAnalyzer.JsonPathAnalysis pathAnalysis)
    {
        PathNode root = pathAnalysis.getPath().getRoot();
        IrPathNode rewritten = new Rewriter(session, functionAndTypeManager, pathAnalysis.getTypes(), pathAnalysis.getJsonParameters()).process(root);

        return new IrJsonPath(pathAnalysis.getPath().isLax(), rewritten);
    }

    private static class Rewriter
            extends JsonPathTreeVisitor<IrPathNode, Void>
    {
        private final Map<PathNodeRef<PathNode>, Type> types;
        private final Set<PathNodeRef<PathNode>> jsonParameters;
        private final Session session;
        private final FunctionAndTypeManager functionAndTypeManager;

        public Rewriter(Session session, FunctionAndTypeManager functionAndTypeManager, Map<PathNodeRef<PathNode>, Type> types, Set<PathNodeRef<PathNode>> jsonParameters)
        {
            this.session = requireNonNull(session, "session is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            requireNonNull(types, "types is null");
            requireNonNull(jsonParameters, "jsonParameters is null");
            requireNonNull(jsonParameters, "jsonParameters is null");

            this.types = types;
            this.jsonParameters = jsonParameters;
        }

        @Override
        protected IrPathNode visitPathNode(PathNode node, Void context)
        {
            throw new UnsupportedOperationException("rewrite not implemented for " + node.getClass().getSimpleName());
        }

        @Override
        protected IrPathNode visitAbsMethod(AbsMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrAbsMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitArithmeticBinary(ArithmeticBinary node, Void context)
        {
            IrPathNode left = process(node.getLeft());
            IrPathNode right = process(node.getRight());
            return new IrArithmeticBinary(binaryOperator(node.getOperator()), left, right, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        private Operator binaryOperator(ArithmeticBinary.Operator operator)
        {
            switch (operator) {
                case ADD:
                    return ADD;
                case SUBTRACT:
                    return SUBTRACT;
                case MULTIPLY:
                    return MULTIPLY;
                case DIVIDE:
                    return DIVIDE;
                case MODULUS:
                    return MODULUS;
            }
            throw new UnsupportedOperationException("Unexpected operator: " + operator);
        }

        @Override
        protected IrPathNode visitArithmeticUnary(ArithmeticUnary node, Void context)
        {
            IrPathNode base = process(node.getBase());
            IrArithmeticUnary.Sign sign;
            switch (node.getSign()) {
                case PLUS:
                    sign = PLUS;
                    break;
                case MINUS:
                    sign = MINUS;
                    break;
                default:
                    throw new UnsupportedOperationException("Unexpected sign: " + node.getSign());
            }
            return new IrArithmeticUnary(sign, base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitArrayAccessor(ArrayAccessor node, Void context)
        {
            IrPathNode base = process(node.getBase());
            List<Subscript> subscripts = node.getSubscripts().stream()
                    .map(subscript -> {
                        IrPathNode from = process(subscript.getFrom());
                        Optional<IrPathNode> to = subscript.getTo().map(this::process);
                        return new Subscript(from, to);
                    })
                    .collect(toImmutableList());
            return new IrArrayAccessor(base, subscripts, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitCeilingMethod(CeilingMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrCeilingMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitContextVariable(ContextVariable node, Void context)
        {
            return new IrContextVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitDatetimeMethod(DatetimeMethod node, Void context)
        {
            // TODO
            throw new IllegalStateException("datetime method is not yet supported. The query should have failed in JsonPathAnalyzer.");

//            IrPathNode base = process(node.getBase());
//            return new IrDatetimeMethod(base, /*parsed format*/, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitDoubleMethod(DoubleMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrDoubleMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitFilter(Filter node, Void context)
        {
            IrPathNode base = process(node.getBase());
            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrFilter(base, predicate, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitFloorMethod(FloorMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrFloorMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitJsonNullLiteral(JsonNullLiteral node, Void context)
        {
            return new IrJsonNull();
        }

        @Override
        protected IrPathNode visitKeyValueMethod(KeyValueMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrKeyValueMethod(base);
        }

        @Override
        protected IrPathNode visitLastIndexVariable(LastIndexVariable node, Void context)
        {
            return new IrLastIndexVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitMemberAccessor(MemberAccessor node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrMemberAccessor(base, node.getKey(), Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitNamedVariable(NamedVariable node, Void context)
        {
            if (jsonParameters.contains(PathNodeRef.of(node))) {
                return new IrNamedJsonVariable(node.getName(), Optional.ofNullable(types.get(PathNodeRef.of(node))));
            }
            return new IrNamedValueVariable(node.getName(), Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitPredicateCurrentItemVariable(PredicateCurrentItemVariable node, Void context)
        {
            return new IrPredicateCurrentItemVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitSizeMethod(SizeMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrSizeMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitSqlValueLiteral(SqlValueLiteral node, Void context)
        {
            Expression value = node.getValue();
            return new IrLiteral(types.get(PathNodeRef.of(node)), LiteralInterpreter.evaluate(functionAndTypeManager, session.toConnectorSession(), value));
        }

        @Override
        protected IrPathNode visitTypeMethod(TypeMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrTypeMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        // predicate

        @Override
        protected IrPathNode visitComparisonPredicate(ComparisonPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode left = process(node.getLeft());
            IrPathNode right = process(node.getRight());
            IrComparisonPredicate.Operator operator = comparisonOperator(node.getOperator());
            return new IrComparisonPredicate(operator, left, right);
        }

        private IrComparisonPredicate.Operator comparisonOperator(ComparisonPredicate.Operator operator)
        {
            switch (operator) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return LESS_THAN;
                case GREATER_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
            }
            throw new UnsupportedOperationException("Unexpected comparison operator: " + operator);
        }

        @Override
        protected IrPathNode visitConjunctionPredicate(ConjunctionPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate left = (IrPredicate) process(node.getLeft());
            IrPredicate right = (IrPredicate) process(node.getRight());
            return new IrConjunctionPredicate(left, right);
        }

        @Override
        protected IrPathNode visitDisjunctionPredicate(DisjunctionPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate left = (IrPredicate) process(node.getLeft());
            IrPredicate right = (IrPredicate) process(node.getRight());
            return new IrDisjunctionPredicate(left, right);
        }

        @Override
        protected IrPathNode visitExistsPredicate(ExistsPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode path = process(node.getPath());
            return new IrExistsPredicate(path);
        }

        @Override
        protected IrPathNode visitIsUnknownPredicate(IsUnknownPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrIsUnknownPredicate(predicate);
        }

        @Override
        protected IrPathNode visitLikeRegexPredicate(LikeRegexPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            // TODO
            throw new IllegalStateException("like_regex predicate is not yet supported. The query should have failed in JsonPathAnalyzer.");
        }

        @Override
        protected IrPathNode visitNegationPredicate(NegationPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrNegationPredicate(predicate);
        }

        @Override
        protected IrPathNode visitStartsWithPredicate(StartsWithPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode whole = process(node.getWhole());
            IrPathNode initial = process(node.getInitial());
            return new IrStartsWithPredicate(whole, initial);
        }
    }
}
