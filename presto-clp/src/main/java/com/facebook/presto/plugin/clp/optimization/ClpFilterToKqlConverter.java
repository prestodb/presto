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
package com.facebook.presto.plugin.clp.optimization;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.flip;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A translator to translate Presto {@link RowExpression}s into:
 * <ul>
 *     <li>KQL (Kibana Query Language) filters used to push down supported filters to the CLP
 *     engine.</li>
 *     <li>SQL filters used for filtering splits in CLP's metadata database.</li>
 * </ul>
 * This class implements the {@link RowExpressionVisitor} interface and recursively walks Presto
 * filter expressions, attempting to convert supported expressions into corresponding KQL filter
 * strings and SQL filter strings for metadata filtering. Any part of the expression that cannot be
 * translated to KQL is preserved as a "remaining expression" for potential fallback processing.
 * <p></p>
 * Supported translations for KQL include:
 * <ul>
 *     <li>Comparisons between variables and constants (e.g., =, !=, <, >, <=, >=).</li>
 *     <li>String pattern matches using LIKE with constant patterns only. Patterns that begin and
 *         end with <code>%</code> (i.e., <code>"^%[^%_]*%$"</code>) are not supported.</li>
 *     <li>Membership checks using IN with a list of constants only.</li>
 *     <li>NULL checks via IS NULL.</li>
 *     <li>Substring comparisons (e.g., <code>SUBSTR(x, start, len) = "val"</code>) against a
 *         constant.</li>
 *     <li>Dereferencing fields from row-typed variables.</li>
 *     <li>Logical operators AND, OR, and NOT.</li>
 *     <li><code>CLP_GET_*</code> UDFs.</li>
 * </ul>
 * <p></p>
 * Supported translations for SQL include:
 * <ul>
 *     <li>Comparisons between variables and constants (e.g., =, !=, <, >, <=, >=).</li>
 *     <li>Dereferencing fields from row-typed variables.</li>
 *     <li>Logical operators AND, OR, and NOT.</li>
 * </ul>
 */
public class ClpFilterToKqlConverter
        implements RowExpressionVisitor<ClpExpression, Void>
{
    private static final Set<OperatorType> LOGICAL_BINARY_OPS_FILTER =
            ImmutableSet.of(EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL);

    private final StandardFunctionResolution standardFunctionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final Set<String> metadataFilterColumns;

    public ClpFilterToKqlConverter(
            StandardFunctionResolution standardFunctionResolution,
            FunctionMetadataManager functionMetadataManager,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            Set<String> metadataFilterColumns)
    {
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.metadataFilterColumns = requireNonNull(metadataFilterColumns, "metadataFilterColumns is null");
    }

    @Override
    public ClpExpression visitCall(CallExpression node, Void context)
    {
        FunctionHandle functionHandle = node.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(node);
        }

        if (standardFunctionResolution.isLikeFunction(functionHandle)) {
            return handleLike(node);
        }

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
                return handleLogicalBinary(operatorType, node);
            }
            if (BETWEEN == operatorType) {
                return handleBetween(node);
            }
        }

        return new ClpExpression(node);
    }

    @Override
    public ClpExpression visitConstant(ConstantExpression node, Void context)
    {
        return new ClpExpression(getLiteralString(node));
    }

    @Override
    public ClpExpression visitVariableReference(VariableReferenceExpression node, Void context)
    {
        return new ClpExpression(getVariableName(node));
    }

    @Override
    public ClpExpression visitSpecialForm(SpecialFormExpression node, Void context)
    {
        switch (node.getForm()) {
            case AND:
                return handleAnd(node);
            case OR:
                return handleOr(node);
            case IN:
                return handleIn(node);
            case IS_NULL:
                return handleIsNull(node);
            case DEREFERENCE:
                return handleDereference(node);
            default:
                return new ClpExpression(node);
        }
    }

    @Override
    public ClpExpression visitExpression(RowExpression node, Void context)
    {
        // For all other expressions, return the original expression
        return new ClpExpression(node);
    }

    /**
     * Extracts the string representation of a constant expression.
     *
     * @param literal the constant expression
     * @return the string representation of the literal
     */
    private String getLiteralString(ConstantExpression literal)
    {
        if (literal.getValue() instanceof Slice) {
            return ((Slice) literal.getValue()).toStringUtf8();
        }
        return literal.toString();
    }

    /**
     * Retrieves the original column name from a variable reference.
     *
     * @param variable the variable reference expression
     * @return the original column name as a string
     */
    private String getVariableName(VariableReferenceExpression variable)
    {
        return ((ClpColumnHandle) assignments.get(variable)).getOriginalColumnName();
    }

    /**
     * Handles the <code>BETWEEN</code> expression.
     * <p></p>
     * The translation is only performed if:
     * <ul>
     *     <li>all arguments have numeric types.</li>
     *     <li>the first argument is a variable reference expression.</li>
     *     <li>the second and third arguments are constant expressions.</li>
     * </ul>
     * <p></p>
     * Example: <code>col1 BETWEEN 0 AND 5</code> → <code>col1 >= 0 AND col1 <= 5</code>
     *
     * @param node the <code>BETWEEN</code> call expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleBetween(CallExpression node)
    {
        List<RowExpression> arguments = node.getArguments();
        if (arguments.size() != 3) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "BETWEEN operator must have exactly three arguments. Received: " + node);
        }
        RowExpression first = arguments.get(0);
        RowExpression second = arguments.get(1);
        RowExpression third = arguments.get(2);
        if (!(first instanceof VariableReferenceExpression)
                || !(second instanceof ConstantExpression)
                || !(third instanceof ConstantExpression)) {
            return new ClpExpression(node);
        }
        if (!isClpCompatibleNumericType(first.getType())
                || !isClpCompatibleNumericType(second.getType())
                || !isClpCompatibleNumericType(third.getType())) {
            return new ClpExpression(node);
        }
        Optional<String> variableOpt = first.accept(this, null).getPushDownExpression();
        if (!variableOpt.isPresent()) {
            return new ClpExpression(node);
        }
        String variable = variableOpt.get();
        String lowerBound = getLiteralString((ConstantExpression) second);
        String upperBound = getLiteralString((ConstantExpression) third);
        String kql = String.format("%s >= %s AND %s <= %s", variable, lowerBound, variable, upperBound);
        String metadataSqlQuery = metadataFilterColumns.contains(variable)
                ? String.format("\"%s\" >= %s AND \"%s\" <= %s", variable, lowerBound, variable, upperBound)
                : null;
        return new ClpExpression(kql, metadataSqlQuery);
    }

    /**
     * Handles the logical NOT expression.
     * <p></p>
     * Example: <code>NOT (col1 = 5)</code> → <code>NOT col1: 5</code>
     *
     * @param node the NOT call expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleNot(CallExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "NOT operator must have exactly one argument. Received: " + node);
        }

        RowExpression input = node.getArguments().get(0);
        ClpExpression expression = input.accept(this, null);
        if (expression.getRemainingExpression().isPresent() || !expression.getPushDownExpression().isPresent()) {
            return new ClpExpression(node);
        }
        String notPushDownExpression = "NOT " + expression.getPushDownExpression().get();
        if (expression.getMetadataSqlQuery().isPresent()) {
            return new ClpExpression(notPushDownExpression, "NOT " + expression.getMetadataSqlQuery());
        }
        else {
            return new ClpExpression(notPushDownExpression);
        }
    }

    /**
     * Handles LIKE expressions.
     * <p></p>
     * Converts SQL LIKE patterns into equivalent KQL queries using <code>*</code> (for
     * <code>%</code>) and <code>?</code> (for <code>_</code>). Only supports constant or casted
     * constant patterns.
     * <p></p>
     * Example: <code>col1 LIKE 'a_bc%'</code> → <code>col1: "a?bc*"</code>
     *
     * @param node the LIKE call expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleLike(CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "LIKE operator must have exactly two arguments. Received: " + node);
        }
        ClpExpression variable = node.getArguments().get(0).accept(this, null);
        if (!variable.getPushDownExpression().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = variable.getPushDownExpression().get();
        RowExpression argument = node.getArguments().get(1);

        String pattern;
        if (argument instanceof ConstantExpression) {
            ConstantExpression literal = (ConstantExpression) argument;
            pattern = getLiteralString(literal);
        }
        else if (argument instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) argument;
            if (!standardFunctionResolution.isCastFunction(callExpression.getFunctionHandle())) {
                return new ClpExpression(node);
            }
            if (callExpression.getArguments().size() != 1) {
                throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "CAST function must have exactly one argument. Received: " + callExpression);
            }
            if (!(callExpression.getArguments().get(0) instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
            pattern = getLiteralString((ConstantExpression) callExpression.getArguments().get(0));
        }
        else {
            return new ClpExpression(node);
        }
        pattern = pattern.replace("%", "*").replace("_", "?");
        return new ClpExpression(format("%s: \"%s\"", variableName, pattern));
    }

    /**
     * Handles logical binary operators (e.g., <code>=, !=, <, ></code>) between two expressions.
     * <p></p>
     * Supports constant values on either side and flips the operator if necessary. Also delegates
     * to a substring handler for <code>SUBSTR(x, ...) = 'value'</code> patterns.
     *
     * @param operator the binary operator (e.g., EQUAL, NOT_EQUAL)
     * @param node the call expression representing the binary operation
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleLogicalBinary(OperatorType operator, CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "Logical binary operator must have exactly two arguments. Received: " + node);
        }
        RowExpression left = node.getArguments().get(0);
        RowExpression right = node.getArguments().get(1);

        Optional<ClpExpression> maybeLeftSubstring = tryInterpretSubstringEquality(operator, left, right);
        if (maybeLeftSubstring.isPresent()) {
            return maybeLeftSubstring.get();
        }

        Optional<ClpExpression> maybeRightSubstring = tryInterpretSubstringEquality(operator, right, left);
        if (maybeRightSubstring.isPresent()) {
            return maybeRightSubstring.get();
        }

        ClpExpression leftExpression = left.accept(this, null);
        ClpExpression rightExpression = right.accept(this, null);
        Optional<String> leftPushDownExpression = leftExpression.getPushDownExpression();
        Optional<String> rightPushDownExpression = rightExpression.getPushDownExpression();
        if (!leftPushDownExpression.isPresent() || !rightPushDownExpression.isPresent()) {
            return new ClpExpression(node);
        }

        boolean leftIsConstant = (left instanceof ConstantExpression);
        boolean rightIsConstant = (right instanceof ConstantExpression);

        Type leftType = left.getType();
        Type rightType = right.getType();

        if (rightIsConstant) {
            return buildClpExpression(
                    leftPushDownExpression.get(),    // variable
                    rightPushDownExpression.get(),   // literal
                    operator,
                    rightType,
                    node);
        }
        else if (leftIsConstant) {
            OperatorType newOperator = flip(operator);
            return buildClpExpression(
                    rightPushDownExpression.get(),   // variable
                    leftPushDownExpression.get(),    // literal
                    newOperator,
                    leftType,
                    node);
        }
        // fallback
        return new ClpExpression(node);
    }

    /**
     * Builds a CLP expression from a basic comparison between a variable and a constant.
     * <p></p>
     * Handles different operator types and formats them appropriately based on whether the literal
     * is a string or a non-string type.
     * <p></p>
     * Examples:
     * <ul>
     *   <li><code>col = 'abc'</code> → <code>col: "abc"</code></li>
     *   <li><code>col != 42</code> → <code>NOT col: 42</code></li>
     *   <li><code>5 < col</code> → <code>col > 5</code></li>
     * </ul>
     *
     * @param variableName name of the variable
     * @param literalString string representation of the literal
     * @param operator the comparison operator
     * @param literalType the type of the literal
     * @param originalNode the original RowExpression node
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression buildClpExpression(
            String variableName,
            String literalString,
            OperatorType operator,
            Type literalType,
            RowExpression originalNode)
    {
        String metadataSqlQuery = null;
        if (operator.equals(EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(format("%s: \"%s\"", variableName, escapeKqlSpecialCharsForStringValue(literalString)));
            }
            else {
                if (metadataFilterColumns.contains(variableName)) {
                    metadataSqlQuery = format("\"%s\" = %s", variableName, literalString);
                }
                return new ClpExpression(format("%s: %s", variableName, literalString), metadataSqlQuery);
            }
        }
        else if (operator.equals(NOT_EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(format("NOT %s: \"%s\"", variableName, escapeKqlSpecialCharsForStringValue(literalString)));
            }
            else {
                if (metadataFilterColumns.contains(variableName)) {
                    metadataSqlQuery = format("NOT \"%s\" = %s", variableName, literalString);
                }
                return new ClpExpression(format("NOT %s: %s", variableName, literalString), metadataSqlQuery);
            }
        }
        else if (LOGICAL_BINARY_OPS_FILTER.contains(operator) && !(literalType instanceof VarcharType)) {
            if (metadataFilterColumns.contains(variableName)) {
                metadataSqlQuery = format("\"%s\" %s %s", variableName, operator.getOperator(), literalString);
            }
            return new ClpExpression(format("%s %s %s", variableName, operator.getOperator(), literalString), metadataSqlQuery);
        }
        return new ClpExpression(originalNode);
    }

    /**
     * Checks whether the given expression matches the pattern
     * <code>SUBSTR(x, ...) = 'someString'</code>, and if so, attempts to convert it into a KQL
     * query using wildcards and constructs a CLP expression.
     *
     * @param operator the comparison operator (should be EQUAL)
     * @param possibleSubstring the left or right expression, possibly a SUBSTR call
     * @param possibleLiteral the opposite expression, possibly a string constant
     * @return an Optional containing a ClpExpression with the equivalent KQL query
     */
    private Optional<ClpExpression> tryInterpretSubstringEquality(
            OperatorType operator,
            RowExpression possibleSubstring,
            RowExpression possibleLiteral)
    {
        if (!operator.equals(EQUAL)) {
            return Optional.empty();
        }

        if (!(possibleSubstring instanceof CallExpression) ||
                !(possibleLiteral instanceof ConstantExpression)) {
            return Optional.empty();
        }

        Optional<SubstrInfo> maybeSubstringCall = parseSubstringCall((CallExpression) possibleSubstring);
        if (!maybeSubstringCall.isPresent()) {
            return Optional.empty();
        }

        String targetString = getLiteralString((ConstantExpression) possibleLiteral);
        return interpretSubstringEquality(maybeSubstringCall.get(), targetString);
    }

    /**
     * Parses a <code>SUBSTR(x, start [, length])</code> call into a SubstrInfo object if valid.
     *
     * @param callExpression the call expression to inspect
     * @return an Optional containing SubstrInfo if the expression is a valid SUBSTR call
     */
    private Optional<SubstrInfo> parseSubstringCall(CallExpression callExpression)
    {
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(callExpression.getFunctionHandle());
        String functionName = functionMetadata.getName().getObjectName();
        if (!functionName.equals("substr")) {
            return Optional.empty();
        }

        int argCount = callExpression.getArguments().size();
        if (argCount < 2 || argCount > 3) {
            return Optional.empty();
        }

        ClpExpression variable = callExpression.getArguments().get(0).accept(this, null);
        if (!variable.getPushDownExpression().isPresent()) {
            return Optional.empty();
        }

        String varName = variable.getPushDownExpression().get();
        RowExpression startExpression = callExpression.getArguments().get(1);
        RowExpression lengthExpression = null;
        if (argCount == 3) {
            lengthExpression = callExpression.getArguments().get(2);
        }

        return Optional.of(new SubstrInfo(varName, startExpression, lengthExpression));
    }

    /**
     * Converts a <code>SUBSTR(x, start [, length]) = 'someString'</code> into a KQL-style wildcard
     * query.
     * <p></p>
     * Examples:
     * <ul>
     *   <li><code>SUBSTR(message, 1, 3) = 'abc'</code> → <code>message: "abc*"</code></li>
     *   <li><code>SUBSTR(message, 4, 3) = 'abc'</code> → <code>message: "???abc*"</code></li>
     *   <li><code>SUBSTR(message, 2) = 'hello'</code> → <code>message: "?hello"</code></li>
     *   <li><code>SUBSTR(message, -5) = 'hello'</code> → <code>message: "*hello"</code></li>
     * </ul>
     *
     * @param info parsed SUBSTR call info
     * @param targetString the literal string being compared to
     * @return an Optional containing either a ClpExpression with the equivalent KQL query
     */
    private Optional<ClpExpression> interpretSubstringEquality(SubstrInfo info, String targetString)
    {
        if (info.lengthExpression != null) {
            Optional<Integer> maybeStart = parseIntValue(info.startExpression);
            Optional<Integer> maybeLen = parseLengthLiteral(info.lengthExpression, targetString);

            if (maybeStart.isPresent() && maybeLen.isPresent()) {
                int start = maybeStart.get();
                int len = maybeLen.get();
                if (start > 0 && len == targetString.length()) {
                    StringBuilder result = new StringBuilder();
                    result.append(info.variableName).append(": \"");
                    for (int i = 1; i < start; i++) {
                        result.append("?");
                    }
                    result.append(targetString).append("*\"");
                    return Optional.of(new ClpExpression(result.toString()));
                }
            }
        }
        else {
            Optional<Integer> maybeStart = parseIntValue(info.startExpression);
            if (maybeStart.isPresent()) {
                int start = maybeStart.get();
                if (start > 0) {
                    StringBuilder result = new StringBuilder();
                    result.append(info.variableName).append(": \"");
                    for (int i = 1; i < start; i++) {
                        result.append("?");
                    }
                    result.append(targetString).append("\"");
                    return Optional.of(new ClpExpression(result.toString()));
                }
                if (start == -targetString.length()) {
                    return Optional.of(new ClpExpression(format("%s: \"*%s\"", info.variableName, targetString)));
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Attempts to parse a RowExpression as an integer constant.
     *
     * @param expression the row expression to parse
     * @return an Optional containing the integer value if it could be parsed
     */
    private Optional<Integer> parseIntValue(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            try {
                return Optional.of(parseInt(getLiteralString((ConstantExpression) expression)));
            }
            catch (NumberFormatException ignored) {
            }
        }
        else if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
            Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
            if (operatorTypeOptional.isPresent() && operatorTypeOptional.get().equals(NEGATION)) {
                RowExpression arg0 = call.getArguments().get(0);
                if (arg0 instanceof ConstantExpression) {
                    try {
                        return Optional.of(-parseInt(getLiteralString((ConstantExpression) arg0)));
                    }
                    catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Attempts to parse the length expression and match it against the target string's length.
     *
     * @param lengthExpression the expression representing the length parameter
     * @param targetString the target string to compare length against
     * @return an Optional containing the length if it matches targetString.length()
     */
    private Optional<Integer> parseLengthLiteral(RowExpression lengthExpression, String targetString)
    {
        if (lengthExpression instanceof ConstantExpression) {
            String val = getLiteralString((ConstantExpression) lengthExpression);
            try {
                int parsed = parseInt(val);
                if (parsed == targetString.length()) {
                    return Optional.of(parsed);
                }
            }
            catch (NumberFormatException ignored) {
            }
        }
        return Optional.empty();
    }

    /**
     * Handles the logical <code>AND</code> expression.
     * <p></p>
     * Combines all definable child expressions into a single KQL query joined by AND. Any
     * unsupported children are collected into the remaining expression.
     * <p></p>
     * Example: <code>col1 = 5 AND col2 = 'abc'</code> → <code>(col1: 5 AND col2: "abc")</code>
     *
     * @param node the <code>AND</code> special form expression
     * @return a ClpExpression containing the KQL query and any remaining sub-expressions
     */
    private ClpExpression handleAnd(SpecialFormExpression node)
    {
        StringBuilder metadataQueryBuilder = new StringBuilder();
        metadataQueryBuilder.append("(");
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        List<RowExpression> remainingExpressions = new ArrayList<>();
        boolean hasMetadataSql = false;
        boolean hasPushDownExpression = false;
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getPushDownExpression().isPresent()) {
                hasPushDownExpression = true;
                queryBuilder.append(expression.getPushDownExpression().get());
                queryBuilder.append(" AND ");
                if (expression.getMetadataSqlQuery().isPresent()) {
                    hasMetadataSql = true;
                    metadataQueryBuilder.append(expression.getMetadataSqlQuery().get());
                    metadataQueryBuilder.append(" AND ");
                }
            }
            if (expression.getRemainingExpression().isPresent()) {
                remainingExpressions.add(expression.getRemainingExpression().get());
            }
        }
        if (!hasPushDownExpression) {
            return new ClpExpression(node);
        }
        else if (!remainingExpressions.isEmpty()) {
            if (remainingExpressions.size() == 1) {
                return new ClpExpression(
                        queryBuilder.substring(0, queryBuilder.length() - 5) + ")",
                        hasMetadataSql ? metadataQueryBuilder.substring(0, metadataQueryBuilder.length() - 5) + ")" : null,
                        remainingExpressions.get(0));
            }
            else {
                return new ClpExpression(
                        queryBuilder.substring(0, queryBuilder.length() - 5) + ")",
                        hasMetadataSql ? metadataQueryBuilder.substring(0, metadataQueryBuilder.length() - 5) + ")" : null,
                        new SpecialFormExpression(node.getSourceLocation(), AND, BOOLEAN, remainingExpressions));
            }
        }
        // Remove the last " AND " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 5) + ")",
                hasMetadataSql ? metadataQueryBuilder.substring(0, metadataQueryBuilder.length() - 5) + ")" : null);
    }

    /**
     * Handles the logical <code>OR</code> expression.
     * <p></p>
     * Combines all fully convertible child expressions into a single KQL query joined by OR.
     * Falls back to the original node if any child cannot be converted.
     * <p></p>
     * Example: <code>col1 = 5 OR col1 = 10</code> → <code>(col1: 5 OR col1: 10)</code>
     *
     * @param node the <code>OR</code> special form expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be fully translated
     */
    private ClpExpression handleOr(SpecialFormExpression node)
    {
        StringBuilder metadataQueryBuilder = new StringBuilder();
        metadataQueryBuilder.append("(");
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        boolean allPushedDown = true;
        boolean hasAllMetadataSql = true;
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            // Note: It is possible in the future that an expression cannot be pushed down as a KQL query, but can be
            // pushed down as a metadata SQL query.
            if (expression.getRemainingExpression().isPresent() || !expression.getPushDownExpression().isPresent()) {
                allPushedDown = false;
                continue;
            }
            queryBuilder.append(expression.getPushDownExpression().get());
            queryBuilder.append(" OR ");
            if (hasAllMetadataSql && expression.getMetadataSqlQuery().isPresent()) {
                metadataQueryBuilder.append(expression.getMetadataSqlQuery().get());
                metadataQueryBuilder.append(" OR ");
            }
            else {
                hasAllMetadataSql = false;
            }
        }
        if (allPushedDown) {
            // Remove the last " OR " from the query
            return new ClpExpression(
                    queryBuilder.substring(0, queryBuilder.length() - 4) + ")",
                    hasAllMetadataSql ? metadataQueryBuilder.substring(0, metadataQueryBuilder.length() - 4) + ")" : null);
        }
        return new ClpExpression(node);
    }

    /**
     * Handles the <code>IN</code> predicate.
     * <p></p>
     * Example: <code>col1 IN (1, 2, 3)</code> → <code>(col1: 1 OR col1: 2 OR col1: 3)</code>
     *
     * @param node the <code>IN</code> special form expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleIn(SpecialFormExpression node)
    {
        ClpExpression variable = node.getArguments().get(0).accept(this, null);
        if (!variable.getPushDownExpression().isPresent()) {
            return new ClpExpression(node);
        }
        String variableName = variable.getPushDownExpression().get();
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        for (RowExpression argument : node.getArguments().subList(1, node.getArguments().size())) {
            if (!(argument instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
            ConstantExpression literal = (ConstantExpression) argument;
            String literalString = getLiteralString(literal);
            queryBuilder.append(variableName).append(": ");
            if (literal.getType() instanceof VarcharType) {
                queryBuilder.append("\"").append(literalString).append("\"");
            }
            else {
                queryBuilder.append(literalString);
            }
            queryBuilder.append(" OR ");
        }

        // Remove the last " OR " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    /**
     * Handles the <code>IS NULL</code> predicate.
     * <p></p>
     * Example: <code>col1 IS NULL</code> → <code>NOT col1: *</code>
     *
     * @param node the <code>IS_NULL</code> special form expression
     * @return a ClpExpression containing either the equivalent KQL query, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleIsNull(SpecialFormExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "IS NULL operator must have exactly one argument. Received: " + node);
        }

        ClpExpression expression = node.getArguments().get(0).accept(this, null);
        if (!expression.getPushDownExpression().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = expression.getPushDownExpression().get();
        return new ClpExpression(format("NOT %s: *", variableName));
    }

    /**
     * Handles dereference expressions on RowTypes (e.g., <code>col.row_field</code>).
     * <p></p>
     * Converts nested row field accesses into dot-separated KQL-compatible field names.
     * <p></p>
     * Example: <code>address.city</code> (from a RowType 'address') → <code>address.city</code>
     *
     * @param expression the dereference expression ({@link SpecialFormExpression} or
     * {@link VariableReferenceExpression})
     * @return a ClpExpression containing either the dot-separated field name, or the original
     * expression if it couldn't be translated
     */
    private ClpExpression handleDereference(RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return expression.accept(this, null);
        }

        if (!(expression instanceof SpecialFormExpression)) {
            return new ClpExpression(expression);
        }

        SpecialFormExpression specialForm = (SpecialFormExpression) expression;
        List<RowExpression> arguments = specialForm.getArguments();
        if (arguments.size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE expects 2 arguments");
        }

        RowExpression base = arguments.get(0);
        RowExpression index = arguments.get(1);
        if (!(index instanceof ConstantExpression)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE index must be a constant");
        }

        ConstantExpression constExpr = (ConstantExpression) index;
        Object value = constExpr.getValue();
        if (!(value instanceof Long)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE index constant is not a long");
        }

        int fieldIndex = ((Long) value).intValue();

        Type baseType = base.getType();
        if (!(baseType instanceof RowType)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE base is not a RowType: " + baseType);
        }

        RowType rowType = (RowType) baseType;
        if (fieldIndex < 0 || fieldIndex >= rowType.getFields().size()) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Invalid field index " + fieldIndex + " for RowType: " + rowType);
        }

        RowType.Field field = rowType.getFields().get(fieldIndex);
        String fieldName = field.getName().orElse("field" + fieldIndex);

        ClpExpression baseString = handleDereference(base);
        if (!baseString.getPushDownExpression().isPresent()) {
            return new ClpExpression(expression);
        }
        return new ClpExpression(baseString.getPushDownExpression().get() + "." + fieldName);
    }

    /**
     * See
     * <a href="https://docs.yscope.com/clp/main/user-docs/reference-json-search-syntax">here
     * </a> for all special chars in the string value that need to be escaped.
     *
     * @param literalString
     * @return the string with special characters escaped
     */
    public static String escapeKqlSpecialCharsForStringValue(String literalString)
    {
        String escaped = literalString;
        escaped = escaped.replace("\\", "\\\\");
        escaped = escaped.replace("\"", "\\\"");
        escaped = escaped.replace("?", "\\?");
        escaped = escaped.replace("*", "\\*");
        return escaped;
    }

    /**
     * Checks if the type is one of the numeric types that can be pushed down to CLP.
     *
     * @param type the type to check
     * @return whether the type can be pushed down.
     */
    public static boolean isClpCompatibleNumericType(Type type)
    {
        return type.equals(BIGINT)
                || type.equals(INTEGER)
                || type.equals(SMALLINT)
                || type.equals(TINYINT)
                || type.equals(DOUBLE)
                || type.equals(REAL)
                || type instanceof DecimalType;
    }

    private static class SubstrInfo
    {
        String variableName;
        RowExpression startExpression;
        RowExpression lengthExpression;

        SubstrInfo(String variableName, RowExpression start, RowExpression length)
        {
            this.variableName = variableName;
            this.startExpression = start;
            this.lengthExpression = length;
        }
    }
}
