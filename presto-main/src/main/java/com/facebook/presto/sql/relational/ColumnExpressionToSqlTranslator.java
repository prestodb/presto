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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnExpressionVisitor;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.ConstantExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionRegistry.isOperator;
import static com.facebook.presto.metadata.FunctionRegistry.unmangleOperator;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.likePatternSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ColumnExpressionToSqlTranslator
{
    private ColumnExpressionToSqlTranslator()
    {
    }

    public static Optional<Expression> translate(ColumnExpression columnExpression, List<Symbol> inputs, Map<ColumnHandle, String> columns, LiteralEncoder literalEncoder,
            FunctionRegistry functionRegistry)
    {
        return columnExpression.accept(new Visitor(inputs, columns, literalEncoder, functionRegistry), null);
    }

    public static class Visitor
            implements ColumnExpressionVisitor<Optional<Expression>, Void>
    {
        private final List<Symbol> inputs;
        private final Map<ColumnHandle, String> columns;
        private final LiteralEncoder literalEncoder;
        private final FunctionRegistry functionRegistry;

        public Visitor(List<Symbol> inputs, Map<ColumnHandle, String> columns, LiteralEncoder literalEncoder, FunctionRegistry functionRegistry)
        {
            this.inputs = inputs;
            this.columns = columns;
            this.literalEncoder = literalEncoder;
            this.functionRegistry = functionRegistry;
        }

        @Override
        public Optional<Expression> visitCall(CallExpression call, Void context)
        {
            String functionName = call.getSignature().getName();
            List<Expression> arguments = call.getArguments()
                    .stream()
                    .map(rowExpression -> rowExpression.accept(this, context))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            // LIKE_PATTERN cannot be converted to expression
            if (functionName.equalsIgnoreCase("LIKE")) {
                checkBinaryExpression(call);
                ColumnExpression pattern = call.getArguments().get(1);
                if (pattern instanceof CallExpression) {
                    if (((CallExpression) pattern).getSignature().getName().equalsIgnoreCase(CAST)) {
                        Optional<Expression> patternString = ((CallExpression) pattern).getArguments().get(0).accept(this, context);
                        if (patternString.isPresent()) {
                            return Optional.of(new LikePredicate(arguments.get(0), patternString.get(), Optional.empty()));
                        }
                    }
                    else if (((CallExpression) pattern).getSignature().equals(likePatternSignature())) {
                        CallExpression likePatternWithEscape = (CallExpression) pattern;
                        checkBinaryExpression(likePatternWithEscape);
                        Optional<Expression> patternExpression = likePatternWithEscape.getArguments().get(0).accept(this, context);
                        Optional<Expression> escapeExpression = likePatternWithEscape.getArguments().get(1).accept(this, context);
                        if (patternExpression.isPresent() && escapeExpression.isPresent()) {
                            return Optional.of(new LikePredicate(arguments.get(0), patternExpression.get(), escapeExpression));
                        }
                    }
                }
            }

            if (arguments.size() != call.getArguments().size()) {
                return Optional.empty();
            }

            if (functionName.equalsIgnoreCase("AND")) {
                checkBinaryExpression(call);
                return Optional.of(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, arguments.get(0), arguments.get(1)));
            }
            else if (functionName.equalsIgnoreCase("OR")) {
                checkBinaryExpression(call);
                return Optional.of(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, arguments.get(0), arguments.get(1)));
            }
            else if (functionName.equalsIgnoreCase("IN")) {
                return Optional.of(new InPredicate(arguments.get(0), new InListExpression(arguments.subList(1, arguments.size()))));
            }
            else if (functionName.equalsIgnoreCase("IF")) {
                checkTernaryExpression(call);
                return Optional.of(new IfExpression(arguments.get(0), arguments.get(1), arguments.get(2)));
            }
            else if (functionName.equalsIgnoreCase("ARRAY_CONSTRUCTOR")) {
                return Optional.of(new ArrayConstructor(arguments));
            }
            else if (functionName.equalsIgnoreCase("ROW_CONSTRUCTOR")) {
                return Optional.of(new Row(arguments));
            }
            else if (functionName.equalsIgnoreCase("COALESCE")) {
                return Optional.of(new CoalesceExpression(arguments));
            }
            else if (functionName.equalsIgnoreCase("IS_NULL")) {
                checkUnaryExpression(call);
                return Optional.of(new IsNullPredicate(arguments.get(0)));
            }
            else if (functionName.equalsIgnoreCase("DEREFERENCE")) {
                checkBinaryExpression(call);
                RowType rowType = (RowType) call.getArguments().get(0).getType();
                int field = (Integer) (((ConstantExpression) call.getArguments().get(1)).getValue());
                return Optional.of(new DereferenceExpression(arguments.get(0), new Identifier(rowType.getFields().get(field).getName().get())));
            }
            else if (functionName.equalsIgnoreCase("TRY")) {
                checkUnaryExpression(call);
                return Optional.of(new TryExpression(arguments.get(0)));
            }
            else if (functionName.equalsIgnoreCase("TRY_CAST")) {
                checkUnaryExpression(call);
                return Optional.of(new Cast(arguments.get(0), call.getType().getTypeSignature().toString(), true));
            }

            if (isOperator(functionName)) {
                switch (unmangleOperator(functionName)) {
                    case ADD:
                        checkBinaryExpression(call);
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, arguments.get(0), arguments.get(1)));
                    case SUBTRACT:
                        checkBinaryExpression(call);
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, arguments.get(0), arguments.get(1)));
                    case DIVIDE:
                        checkBinaryExpression(call);
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE, arguments.get(0), arguments.get(1)));
                    case MULTIPLY:
                        checkBinaryExpression(call);
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, arguments.get(0), arguments.get(1)));
                    case MODULUS:
                        checkBinaryExpression(call);
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MODULUS, arguments.get(0), arguments.get(1)));
                    case NEGATION:
                        checkUnaryExpression(call);
                        return Optional.of(new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, arguments.get(0)));
                    case EQUAL:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, arguments.get(0), arguments.get(1)));
                    case NOT_EQUAL:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.NOT_EQUAL, arguments.get(0), arguments.get(1)));
                    case LESS_THAN:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, arguments.get(0), arguments.get(1)));
                    case LESS_THAN_OR_EQUAL:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)));
                    case GREATER_THAN:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, arguments.get(0), arguments.get(1)));
                    case GREATER_THAN_OR_EQUAL:
                        checkBinaryExpression(call);
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)));
                    case BETWEEN:
                        checkTernaryExpression(call);
                        return Optional.of(new LogicalBinaryExpression(
                                LogicalBinaryExpression.Operator.AND,
                                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)),
                                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, arguments.get(0), arguments.get(2))));
                    case CAST:
                        // TODO better way to convert back type to SQL?
                        checkUnaryExpression(call);
                        return Optional.of(new Cast(arguments.get(0), call.getType().getTypeSignature().toString()));
                    case SUBSCRIPT:
                        checkBinaryExpression(call);
                        return Optional.of(new SubscriptExpression(arguments.get(0), arguments.get(1)));
                    default:
                        return Optional.empty();
                }
            }

            // TODO change to functionHandle once FunctionManager is available
            Set<QualifiedName> functions = functionRegistry.getFunctions(call.getSignature());
            if (functions.isEmpty()) {
                return Optional.empty();
            }
            checkArgument(functions.size() == 1, "Resolved multiple functions %s for %s", functions, call.getSignature());
            return Optional.of(new FunctionCall(Iterables.get(functions, 0), arguments));
        }

        @Override
        public Optional<Expression> visitInputReference(InputReferenceExpression reference, Void context)
        {
            if (inputs.isEmpty()) {
                return Optional.of(new FieldReference(reference.getField()));
            }
            checkArgument(reference.getField() < inputs.size(), "field not found.");
            return Optional.of(inputs.get(reference.getField()).toSymbolReference());
        }

        @Override
        public Optional<Expression> visitConstant(ConstantExpression literal, Void context)
        {
            return Optional.of(literalEncoder.toExpression(literal.getValue(), literal.getType()));
        }

        @Override
        public Optional<Expression> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            Optional<Expression> body = lambda.getBody()
                    .accept(this, context);
            if (body.isPresent()) {
                return Optional.of(
                        new LambdaExpression(
                                lambda
                                        .getArguments()
                                        .stream()
                                        .map(
                                                name -> new LambdaArgumentDeclaration(
                                                        new Identifier(name)))
                                        .collect(toImmutableList()),
                                body.get()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<Expression> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return Optional.of(new SymbolReference(reference.getName()));
        }

        @Override
        public Optional<Expression> visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            checkArgument(columns.containsKey(columnReferenceExpression.getColumnHandle()), "columnHandle not found.");
            return Optional.of(new SymbolReference(columns.get(columnReferenceExpression.getColumnHandle())));
        }

        private void checkUnaryExpression(CallExpression call)
        {
            checkArgument(call.getArguments().size() == 1, "Call %s must has exactly 1 argument but got %s", call.getSignature(), call.getArguments());
        }

        private void checkBinaryExpression(CallExpression call)
        {
            checkArgument(call.getArguments().size() == 2, "Call %s must has exactly 2 argument but got %s", call.getSignature(), call.getArguments());
        }

        private void checkTernaryExpression(CallExpression call)
        {
            checkArgument(call.getArguments().size() == 3, "Call %s must has exactly 2 argument but got %s", call.getSignature(), call.getArguments());
        }
    }
}
