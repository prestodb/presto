package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.facebook.presto.sql.relational.Expressions.constant;

/**
 * Minimal iterative rule that rewrites:
 *   CAST(ts_col AS date) = DATE 'yyyy-mm-dd'
 * into
 *   ts_col >= TIMESTAMP 'yyyy-mm-dd 00:00:00.000'
 * (No upper bound to avoid referencing "AND".)
 */
public class TimestampRuleChange
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;

    public TimestampRuleChange(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filter, Captures captures, Context context)
    {
        RowExpression originalPredicate = filter.getPredicate();
        RowExpression rewritten = rewritePredicate(originalPredicate);
        if (rewritten.equals(originalPredicate)) {
            return Result.empty();
        }

        FilterNode newFilter = new FilterNode(
                filter.getSourceLocation(),
                filter.getId(),
                filter.getSource(),
                rewritten);
        return Result.ofPlanNode(newFilter);
    }

    private RowExpression rewritePredicate(RowExpression expression)
    {
        // If not a function call, do nothing
        if (!(expression instanceof CallExpression)) {
            return expression;
        }
        CallExpression call = (CallExpression) expression;

        // If the displayName is '=' and there are exactly two arguments, check for date(...) usage
        if ("=".equalsIgnoreCase(call.getDisplayName()) && call.getArguments().size() == 2) {
            RowExpression left = call.getArguments().get(0);
            RowExpression right = call.getArguments().get(1);

            Optional<RowExpression> maybeCol = extractCastToDate(left);
            Optional<LocalDate> maybeDate = extractDateLiteral(right);

            if (!maybeCol.isPresent() || !maybeDate.isPresent()) {
                // try flipping
                maybeCol = extractCastToDate(right);
                maybeDate = extractDateLiteral(left);
            }

            if (maybeCol.isPresent() && maybeDate.isPresent()) {
                // We'll rewrite to: col >= TIMESTAMP 'yyyy-mm-dd 00:00:00.000'
                LocalDate dateVal = maybeDate.get();
                String start = dateVal.toString() + " 00:00:00.000";

                return comparisonExpression(
                        functionResolution,
                        GREATER_THAN_OR_EQUAL,
                        maybeCol.get(),
                        constant(start, TIMESTAMP));
            }
        }

        // If not recognized, leave it alone
        return expression;
    }

    private Optional<RowExpression> extractCastToDate(RowExpression expr)
    {
        if (!(expr instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expr;
        // Check if return type is date
        if (!call.getType().toString().equalsIgnoreCase("date")) {
            return Optional.empty();
        }
        // Check if name includes "cast"
        if (!call.getDisplayName().toLowerCase().contains("cast")) {
            return Optional.empty();
        }
        if (call.getArguments().isEmpty()) {
            return Optional.empty();
        }
        // Child should be the underlying timestamp expression
        return Optional.of(call.getArguments().get(0));
    }

    private Optional<LocalDate> extractDateLiteral(RowExpression expr)
    {
        if (!(expr instanceof ConstantExpression)) {
            return Optional.empty();
        }
        ConstantExpression c = (ConstantExpression) expr;
        if (c.getValue() == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(LocalDate.parse(c.getValue().toString()));
        }
        catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }
}
