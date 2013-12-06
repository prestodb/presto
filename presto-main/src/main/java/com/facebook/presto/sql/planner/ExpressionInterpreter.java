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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.UnixTimeFunctions;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.Casts;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joni.Regex;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpressions;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExpressionInterpreter
{
    private final Expression expression;
    private final Metadata metadata;
    private final Session session;
    private final boolean optimize;

    private final Visitor visitor;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, Regex> LIKE_PATTERN_CACHE = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<Object>> IN_LIST_CACHE = new IdentityHashMap<>();

    public static ExpressionInterpreter expressionInterpreter(Expression expression, Metadata metadata, Session session)
    {
        checkNotNull(expression, "expression is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, false);
    }

    public static ExpressionInterpreter expressionOptimizer(Expression expression, Metadata metadata, Session session)
    {
        checkNotNull(expression, "expression is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, true);
    }

    private ExpressionInterpreter(Expression expression, Metadata metadata, Session session, boolean optimize)
    {
        this.expression = expression;
        this.metadata = metadata;
        this.session = session;
        this.optimize = optimize;

        this.visitor = new Visitor();
    }

    public Object evaluate(RecordCursor inputs)
    {
        Preconditions.checkState(!optimize, "evaluate(RecordCursor) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object evaluate(TupleReadable[] inputs)
    {
        Preconditions.checkState(!optimize, "evaluate(TupleReadable[]) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object optimize(SymbolResolver inputs)
    {
        Preconditions.checkState(optimize, "evaluate(SymbolResolver) not allowed for interpreter");
        return visitor.process(expression, inputs);
    }

    private class Visitor
            extends AstVisitor<Object, Object>
    {
        @Override
        protected Object visitCurrentTime(CurrentTime node, Object context)
        {
            if (node.getType() != CurrentTime.Type.TIMESTAMP) {
                throw new UnsupportedOperationException("not yet implemented: " + node.getType());
            }
            else if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            return UnixTimeFunctions.currentTimestamp(session);
        }

        @Override
        public Object visitInputReference(InputReference node, Object context)
        {
            Input input = node.getInput();

            int channel = input.getChannel();
            if (context instanceof TupleReadable[]) {
                TupleReadable[] inputs = (TupleReadable[]) context;
                TupleReadable tuple = inputs[channel];

                if (tuple.isNull()) {
                    return null;
                }

                switch (tuple.getTupleInfo().getType()) {
                    case BOOLEAN:
                        return tuple.getBoolean();
                    case FIXED_INT_64:
                        return tuple.getLong();
                    case DOUBLE:
                        return tuple.getDouble();
                    case VARIABLE_BINARY:
                        return tuple.getSlice();
                    default:
                        throw new UnsupportedOperationException("not yet implemented");
                }
            }
            else if (context instanceof RecordCursor) {
                RecordCursor cursor = (RecordCursor) context;
                if (cursor.isNull(channel)) {
                    return null;
                }

                switch (cursor.getType(input.getChannel())) {
                    case BOOLEAN:
                        return cursor.getBoolean(channel);
                    case LONG:
                        return cursor.getLong(channel);
                    case DOUBLE:
                        return cursor.getDouble(channel);
                    case STRING:
                        return Slices.wrappedBuffer(cursor.getString(channel));
                    default:
                        throw new UnsupportedOperationException("not yet implemented");
                }
            }
            throw new UnsupportedOperationException("Inputs or cursor myst be set");
        }

        @Override
        protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context)
        {
            if (node.getName().getPrefix().isPresent()) {
                // not a symbol
                return node;
            }

            Symbol symbol = Symbol.fromQualifiedName(node.getName());
            return ((SymbolResolver) context).getValue(symbol);
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return LiteralInterpreter.evaluate(node);
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return node;
            }

            return value == null;
        }

        @Override
        protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return node;
            }

            return value != null;
        }

        @Override
        protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context)
        {
            Expression resultClause = node.getDefaultValue();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object value = process(whenClause.getOperand(), context);
                if (value instanceof Expression) {
                    // TODO: optimize this case
                    return node;
                }

                if (Boolean.TRUE.equals(value)) {
                    resultClause = whenClause.getResult();
                    break;
                }
            }

            if (resultClause == null) {
                return null;
            }

            Object result = process(resultClause, context);
            if (result instanceof Expression) {
                return node;
            }
            return result;
        }

        @Override
        protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Object context)
        {
            Object operand = process(node.getOperand(), context);
            if (operand instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            Expression resultClause = node.getDefaultValue();
            if (operand != null) {
                for (WhenClause whenClause : node.getWhenClauses()) {
                    Object value = process(whenClause.getOperand(), context);
                    if (value instanceof Expression) {
                        // TODO: optimize this case
                        return node;
                    }

                    if (operand instanceof Long && value instanceof Long) {
                        if (((Long) operand).longValue() == ((Long) value).longValue()) {
                            resultClause = whenClause.getResult();
                            break;
                        }
                    }
                    else if (operand instanceof Number && value instanceof Number) {
                        if (((Number) operand).doubleValue() == ((Number) value).doubleValue()) {
                            resultClause = whenClause.getResult();
                            break;
                        }
                    }
                    else if (operand.equals(value)) {
                        resultClause = whenClause.getResult();
                        break;
                    }
                }
            }
            if (resultClause == null) {
                return null;
            }

            Object result = process(resultClause, context);
            if (result instanceof Expression) {
                return node;
            }
            return result;
        }

        @Override
        protected Object visitCoalesceExpression(CoalesceExpression node, Object context)
        {
            for (Expression expression : node.getOperands()) {
                Object value = process(expression, context);

                if (value instanceof Expression) {
                    // TODO: optimize this case
                    return node;
                }

                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                if (!optimize) {
                    throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
                }
                return node;
            }
            InListExpression valueList = (InListExpression) valueListExpression;

            Set<Object> set = IN_LIST_CACHE.get(valueList);

            // We use the presence of the node in the map to indicate that we've already done
            // the analysis below. If the value is null, it means that we can't apply the HashSet
            // optimization
            if (!IN_LIST_CACHE.containsKey(valueList)) {
                if (Iterables.all(valueList.getValues(), isNonNullLiteralPredicate())) {
                    // if all elements are constant, create a set with them
                    set = new HashSet<>();
                    for (Expression expression : valueList.getValues()) {
                        set.add(process(expression, context));
                    }
                }
                IN_LIST_CACHE.put(valueList, set);
            }

            if (set != null && !(value instanceof Expression)) {
                return set.contains(value);
            }

            boolean hasUnresolvedValue = false;
            if (value instanceof Expression) {
                hasUnresolvedValue = true;
            }

            boolean hasNullValue = false;
            boolean found = false;
            List<Object> values = new ArrayList<>(valueList.getValues().size());
            for (Expression expression : valueList.getValues()) {
                Object inValue = process(expression, context);
                if (value instanceof Expression || inValue instanceof Expression) {
                    hasUnresolvedValue = true;
                    values.add(inValue);
                    continue;
                }

                if (inValue == null) {
                    hasNullValue = true;
                }
                else if (!found && value.equals(inValue)) {
                    // in does not short-circuit so we must evaluate all value in the list
                    found = true;
                }
            }
            if (found) {
                return true;
            }

            if (hasUnresolvedValue) {
                return new InPredicate(toExpression(value), new InListExpression(toExpressions(values)));
            }
            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitNegativeExpression(NegativeExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            if (value instanceof Expression) {
                return node;
            }

            if (value instanceof Long) {
                return -((long) value);
            }
            return -((double) value);
        }

        @Override
        protected Object visitArithmeticExpression(ArithmeticExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            if (left == null) {
                return null;
            }
            Object right = process(node.getRight(), context);
            if (right == null) {
                return null;
            }

            if (left instanceof Expression || right instanceof Expression) {
                return node;
            }

            Number leftNumber = (Number) left;
            Number rightNumber = (Number) right;
            switch (node.getType()) {
                case ADD:
                    if (leftNumber instanceof Long && rightNumber instanceof Long) {
                        return leftNumber.longValue() + rightNumber.longValue();
                    }
                    else {
                        return leftNumber.doubleValue() + rightNumber.doubleValue();
                    }
                case SUBTRACT:
                    if (leftNumber instanceof Long && rightNumber instanceof Long) {
                        return leftNumber.longValue() - rightNumber.longValue();
                    }
                    else {
                        return leftNumber.doubleValue() - rightNumber.doubleValue();
                    }
                case DIVIDE:
                    if (leftNumber instanceof Long && rightNumber instanceof Long) {
                        return leftNumber.longValue() / rightNumber.longValue();
                    }
                    else {
                        return leftNumber.doubleValue() / rightNumber.doubleValue();
                    }
                case MULTIPLY:
                    if (leftNumber instanceof Long && rightNumber instanceof Long) {
                        return leftNumber.longValue() * rightNumber.longValue();
                    }
                    else {
                        return leftNumber.doubleValue() * rightNumber.doubleValue();
                    }
                case MODULUS:
                    if (leftNumber instanceof Long && rightNumber instanceof Long) {
                        return leftNumber.longValue() % rightNumber.longValue();
                    }
                    else {
                        return leftNumber.doubleValue() % rightNumber.doubleValue();
                    }
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getType());
            }
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            if (node.getType() == ComparisonExpression.Type.IS_DISTINCT_FROM) {
                Object left = process(node.getLeft(), context);
                Object right = process(node.getRight(), context);

                if (left == null && right == null) {
                    return false;
                }
                else if (left == null || right == null) {
                    return true;
                }
                else if (left instanceof Long && right instanceof Long) {
                    return ((Number) left).longValue() != ((Number) right).longValue();
                }
                else if (left instanceof Number && right instanceof Number) {
                    return ((Number) left).doubleValue() != ((Number) right).doubleValue();
                }
                else if (left instanceof Boolean && right instanceof Boolean) {
                    return !left.equals(right);
                }
                else if (left instanceof Slice && right instanceof Slice) {
                    return !left.equals(right);
                }

                return new ComparisonExpression(node.getType(), toExpression(left), toExpression(right));
            }

            Object left = process(node.getLeft(), context);
            if (left == null) {
                return null;
            }
            Object right = process(node.getRight(), context);
            if (right == null) {
                return null;
            }

            if (left instanceof Long && right instanceof Long) {
                switch (node.getType()) {
                    case EQUAL:
                        return ((Number) left).longValue() == ((Number) right).longValue();
                    case NOT_EQUAL:
                        return ((Number) left).longValue() != ((Number) right).longValue();
                    case LESS_THAN:
                        return ((Number) left).longValue() < ((Number) right).longValue();
                    case LESS_THAN_OR_EQUAL:
                        return ((Number) left).longValue() <= ((Number) right).longValue();
                    case GREATER_THAN:
                        return ((Number) left).longValue() > ((Number) right).longValue();
                    case GREATER_THAN_OR_EQUAL:
                        return ((Number) left).longValue() >= ((Number) right).longValue();
                }
                throw new UnsupportedOperationException("unhandled type: " + node.getType());
            }

            if (left instanceof Number && right instanceof Number) {
                switch (node.getType()) {
                    case EQUAL:
                        return ((Number) left).doubleValue() == ((Number) right).doubleValue();
                    case NOT_EQUAL:
                        return ((Number) left).doubleValue() != ((Number) right).doubleValue();
                    case LESS_THAN:
                        return ((Number) left).doubleValue() < ((Number) right).doubleValue();
                    case LESS_THAN_OR_EQUAL:
                        return ((Number) left).doubleValue() <= ((Number) right).doubleValue();
                    case GREATER_THAN:
                        return ((Number) left).doubleValue() > ((Number) right).doubleValue();
                    case GREATER_THAN_OR_EQUAL:
                        return ((Number) left).doubleValue() >= ((Number) right).doubleValue();
                }
                throw new UnsupportedOperationException("unhandled type: " + node.getType());
            }

            if (left instanceof Slice && right instanceof Slice) {
                switch (node.getType()) {
                    case EQUAL:
                        return left.equals(right);
                    case NOT_EQUAL:
                        return !left.equals(right);
                    case LESS_THAN:
                        return ((Slice) left).compareTo((Slice) right) < 0;
                    case LESS_THAN_OR_EQUAL:
                        return ((Slice) left).compareTo((Slice) right) <= 0;
                    case GREATER_THAN:
                        return ((Slice) left).compareTo((Slice) right) > 0;
                    case GREATER_THAN_OR_EQUAL:
                        return ((Slice) left).compareTo((Slice) right) >= 0;
                }
                throw new UnsupportedOperationException("unhandled type: " + node.getType());
            }

            if (left instanceof Boolean && right instanceof Boolean) {
                switch (node.getType()) {
                    case EQUAL:
                        return left.equals(right);
                    case NOT_EQUAL:
                        return !left.equals(right);
                }
                throw new UnsupportedOperationException("unhandled type: " + node.getType());
            }

            return new ComparisonExpression(node.getType(), toExpression(left), toExpression(right));
        }

        @Override
        protected Object visitBetweenPredicate(BetweenPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            Object min = process(node.getMin(), context);
            if (min == null) {
                return null;
            }
            Object max = process(node.getMax(), context);
            if (max == null) {
                return null;
            }

            if (value instanceof Number && min instanceof Number && max instanceof Number) {
                return ((Number) min).doubleValue() <= ((Number) value).doubleValue() && ((Number) value).doubleValue() <= ((Number) max).doubleValue();
            }
            else if (value instanceof Slice && min instanceof Slice && max instanceof Slice) {
                return ((Slice) min).compareTo((Slice) value) <= 0 && ((Slice) value).compareTo((Slice) max) <= 0;
            }

            return new BetweenPredicate(toExpression(value), toExpression(min), toExpression(max));
        }

        @Override
        protected Object visitNullIfExpression(NullIfExpression node, Object context)
        {
            Object first = process(node.getFirst(), context);
            if (first == null) {
                return null;
            }
            Object second = process(node.getSecond(), context);
            if (second == null) {
                return first;
            }

            if (first instanceof Long && second instanceof Long) {
                return ((Long) first).longValue() == ((Long) second).longValue() ? null : first;
            }
            else if (first instanceof Number && second instanceof Number) {
                return ((Number) first).doubleValue() == ((Number) second).doubleValue() ? null : first;
            }
            else if (first instanceof Boolean && second instanceof Boolean) {
                return first.equals(second) ? null : first;
            }
            else if (first instanceof Slice && second instanceof Slice) {
                return first.equals(second) ? null : first;
            }

            return node;
        }

        @Override
        protected Object visitIfExpression(IfExpression node, Object context)
        {
            Object condition = process(node.getCondition(), context);

            if (Boolean.TRUE.equals(condition)) {
                return process(node.getTrueValue(), context);
            }

            if ((condition == null) || (Boolean.FALSE.equals(condition))) {
                if (node.getFalseValue().isPresent()) {
                    return process(node.getFalseValue().get(), context);
                }
                return null;
            }

            Object trueValue = optimize(node.getTrueValue(), context);
            Object falseValue = null;
            if (node.getFalseValue().isPresent()) {
                falseValue = optimize(node.getFalseValue().get(), context);
            }

            return new IfExpression(toExpression(condition), toExpression(trueValue), toExpression(falseValue));
        }

        @Override
        protected Object visitNotExpression(NotExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            if (value instanceof Expression) {
                return node;
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            Object right = process(node.getRight(), context);

            switch (node.getType()) {
                case AND: {
                    // if either left or right is false, result is always false regardless of nulls
                    if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }
                }
                case OR: {
                    // if either left or right is true, result is always true regardless of nulls
                    if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }
                }
            }

            if (left == null && right == null) {
                return null;
            }

            return node;
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Object context)
        {
            return node.equals(BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        protected Object visitFunctionCall(FunctionCall node, Object context)
        {
            // TODO: remove this huge hack
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.getArguments()) {
                Object value = process(expression, context);
                if (value == null) {
                    return null;
                }
                Type type;
                if (value instanceof Double) {
                    type = Type.DOUBLE;
                }
                else if (value instanceof Long) {
                    type = Type.BIGINT;
                }
                else if (value instanceof Slice) {
                    type = Type.VARCHAR;
                }
                else if (value instanceof Boolean) {
                    type = Type.BOOLEAN;
                }
                else if (value instanceof Expression) {
                    // TODO when we know the type of this expression, construct new FunctionCall node with optimized arguments
                    return node;
                }
                else {
                    throw new UnsupportedOperationException("Unhandled value type: " + value.getClass().getName());
                }
                argumentValues.add(value);
                argumentTypes.add(type);
            }
            FunctionInfo function = metadata.getFunction(node.getName(), argumentTypes);
            // do not optimize non-deterministic functions
            if (optimize && !function.isDeterministic()) {
                return new FunctionCall(node.getName(), node.getWindow().orNull(), node.isDistinct(), toExpressions(argumentValues));
            }
            MethodHandle handle = function.getScalarFunction();
            if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == Session.class) {
                handle = handle.bindTo(session);
            }
            try {
                return handle.invokeWithArguments(argumentValues);
            }
            catch (Throwable throwable) {
                Throwables.propagateIfInstanceOf(throwable, RuntimeException.class);
                Throwables.propagateIfInstanceOf(throwable, Error.class);
                throw new RuntimeException(throwable.getMessage(), throwable);
            }
        }

        @Override
        protected Object visitLikePredicate(LikePredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value == null) {
                return null;
            }

            if (value instanceof Slice &&
                    node.getPattern() instanceof StringLiteral &&
                    (node.getEscape() instanceof StringLiteral || node.getEscape() == null)) {
                // fast path when we know the pattern and escape are constant
                return LikeUtils.regexMatches(getConstantPattern(node), (Slice) value);
            }

            Object pattern = process(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Object escape = null;
            if (node.getEscape() != null) {
                escape = process(node.getEscape(), context);

                if (escape == null) {
                    return null;
                }
            }

            if (value instanceof Slice &&
                    pattern instanceof Slice &&
                    (escape == null || escape instanceof Slice)) {
                Regex regex = LikeUtils.likeToPattern((Slice) pattern, (Slice) escape);

                return LikeUtils.regexMatches(regex, (Slice) value);
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (pattern instanceof Slice && escape == null) {
                String stringPattern = ((Slice) pattern).toString(Charsets.UTF_8);
                if (!stringPattern.contains("%") && !stringPattern.contains("_")) {
                    return new ComparisonExpression(ComparisonExpression.Type.EQUAL, toExpression(value), toExpression(pattern));
                }
            }

            Expression optimizedEscape = null;
            if (node.getEscape() != null) {
                optimizedEscape = toExpression(escape);
            }

            return new LikePredicate(toExpression(value), toExpression(pattern), optimizedEscape);
        }

        private Regex getConstantPattern(LikePredicate node)
        {
            Regex result = LIKE_PATTERN_CACHE.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();
                StringLiteral escape = (StringLiteral) node.getEscape();

                result = LikeUtils.likeToPattern(pattern.getSlice(), escape == null ? null : escape.getSlice());

                LIKE_PATTERN_CACHE.put(node, result);
            }

            return result;
        }

        @Override
        @SuppressWarnings("fallthrough")
        protected Object visitExtract(Extract node, Object context)
        {
            Object value = process(node.getExpression(), context);
            if (value == null) {
                return null;
            }

            if (value instanceof Expression) {
                return new Extract(toExpression(value), node.getField());
            }

            long time = (long) value;
            switch (node.getField()) {
                case CENTURY:
                    return UnixTimeFunctions.century(time);
                case YEAR:
                    return UnixTimeFunctions.year(time);
                case QUARTER:
                    return UnixTimeFunctions.quarter(time);
                case MONTH:
                    return UnixTimeFunctions.month(time);
                case WEEK:
                    return UnixTimeFunctions.week(time);
                case DAY:
                case DAY_OF_MONTH:
                    return UnixTimeFunctions.day(time);
                case DAY_OF_WEEK:
                case DOW:
                    return UnixTimeFunctions.dayOfWeek(time);
                case DAY_OF_YEAR:
                case DOY:
                    return UnixTimeFunctions.dayOfYear(time);
                case HOUR:
                    return UnixTimeFunctions.hour(time);
                case MINUTE:
                    return UnixTimeFunctions.minute(time);
                case SECOND:
                    return UnixTimeFunctions.second(time);
                case TIMEZONE_HOUR:
                case TIMEZONE_MINUTE:
                    return 0L; // we assume all times are UTC for now  TODO
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }

        @Override
        public Object visitCast(Cast node, Object context)
        {
            Object value = process(node.getExpression(), context);

            if (value instanceof Expression) {
                return new Cast((Expression) value, node.getType());
            }

            if (value == null) {
                return null;
            }

            switch (node.getType()) {
                case "BOOLEAN":
                    return Casts.toBoolean(value);
                case "VARCHAR":
                    return Casts.toSlice(value);
                case "DOUBLE":
                    return Casts.toDouble(value);
                case "BIGINT":
                    return Casts.toLong(value);
            }

            throw new UnsupportedOperationException("Unsupported type: " + node.getType());
        }

        @Override
        protected Object visitExpression(Expression node, Object context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Object visitNode(Node node, Object context)
        {
            throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
        }

        private Object optimize(Node node, Object context)
        {
            checkState(optimize, "not optimizing");
            try {
                return process(node, context);
            }
            catch (RuntimeException e) {
                return node;
            }
        }
    }

    private static Predicate<Expression> isNonNullLiteralPredicate()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(@Nullable Expression input)
            {
                return input instanceof Literal && !(input instanceof NullLiteral);
            }
        };
    }
}
