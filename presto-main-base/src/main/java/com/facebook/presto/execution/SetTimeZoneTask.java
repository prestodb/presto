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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.SetTimeZone;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static com.facebook.presto.SystemSessionProperties.TIME_ZONE_ID;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;

public class SetTimeZoneTask
        implements SessionTransactionControlTask<SetTimeZone>
{
    @Override
    public String getName()
    {
        return "SET TIME ZONE";
    }

    @Override
    public ListenableFuture<?> execute(
            SetTimeZone statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            String query)
    {
        Session session = stateMachine.getSession();
        Optional<Expression> timeZoneExpression = statement.getTimeZone();

        // SET TIME ZONE LOCAL - set to JVM default timezone
        if (timeZoneExpression.isEmpty()) {
            stateMachine.addSetSessionProperties(TIME_ZONE_ID, TimeZone.getDefault().getID());
            return immediateFuture(null);
        }

        TimeZoneKey timeZoneKey;
        {
            Expression expression = timeZoneExpression.get();

            if (expression instanceof StringLiteral) {
                // Region-based time zone: SET TIME ZONE 'America/Los_Angeles'
                String zoneId = ((StringLiteral) expression).getValue();
                try {
                    timeZoneKey = getTimeZoneKey(zoneId);
                }
                catch (TimeZoneNotSupportedException e) {
                    throw new PrestoException(
                            INVALID_SESSION_PROPERTY,
                            format("Invalid time zone: %s", zoneId));
                }
            }
            else if (expression instanceof IntervalLiteral) {
                // Offset-based time zone: SET TIME ZONE INTERVAL '10' HOUR
                IntervalLiteral interval = (IntervalLiteral) expression;
                long offsetMinutes;

                try {
                    if (interval.isYearToMonth()) {
                        throw new SemanticException(
                                INVALID_LITERAL,
                                expression,
                                "Invalid interval literal for SET TIME ZONE: %s",
                                interval);
                    }

                    // Parse the interval value
                    String value = interval.getValue();
                    IntervalLiteral.IntervalField startField = interval.getStartField();
                    IntervalLiteral.IntervalField endField = interval.getEndField().orElse(startField);

                    // Calculate offset in minutes
                    offsetMinutes = parseIntervalToMinutes(interval.getSign(), value, startField, endField);
                }
                catch (Exception e) {
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format("Invalid interval for SET TIME ZONE: %s", e.getMessage()));
                }

                try {
                    timeZoneKey = getTimeZoneKeyForOffset(offsetMinutes);
                }
                catch (Exception e) {
                    throw new PrestoException(
                            INVALID_SESSION_PROPERTY,
                            format("Invalid time zone offset minutes: %d", offsetMinutes));
                }
            }
            else {
                // Try to evaluate as a general expression
                try {
                    Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

                    ExpressionAnalyzer analyzer = ExpressionAnalyzer.createConstantAnalyzer(
                            metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                            session,
                            parameterLookup,
                            WarningCollector.NOOP);
                    analyzer.analyze(expression, Scope.create());
                    Type expressionType = analyzer.getExpressionTypes().get(NodeRef.of(expression));

                    Object result = evaluateConstantExpression(expression, expressionType, metadata, session, parameterLookup);

                    if (result == null) {
                        throw new PrestoException(
                                INVALID_SESSION_PROPERTY,
                                "Time zone expression evaluated to NULL");
                    }

                    if (result instanceof String) {
                        try {
                            timeZoneKey = getTimeZoneKey((String) result);
                        }
                        catch (TimeZoneNotSupportedException e) {
                            throw new PrestoException(
                                    INVALID_SESSION_PROPERTY,
                                    format("Invalid time zone: %s", result));
                        }
                    }
                    else if (result instanceof Slice) {
                        String timeZoneString = ((Slice) result).toStringUtf8();
                        try {
                            timeZoneKey = getTimeZoneKey(timeZoneString);
                        }
                        catch (TimeZoneNotSupportedException e) {
                            throw new PrestoException(
                                    INVALID_SESSION_PROPERTY,
                                    format("Invalid time zone: %s", timeZoneString));
                        }
                    }
                    else if (result instanceof Long) {
                        long value = (Long) result;
                        long offsetMinutes;

                        // If value > 100000, it's likely milliseconds from parse_duration()
                        if (Math.abs(value) > 100000) {
                            // Convert milliseconds to minutes
                            long milliseconds = value;
                            offsetMinutes = milliseconds / 60000;

                            // Check if there are remaining seconds (not a whole minute)
                            long remainingMillis = Math.abs(milliseconds) % 60000;
                            if (remainingMillis != 0) {
                                throw new PrestoException(
                                        INVALID_FUNCTION_ARGUMENT,
                                        "Time zone offset must be in whole minutes, got seconds component");
                            }
                        }
                        else {
                            offsetMinutes = value;
                        }

                        try {
                            timeZoneKey = getTimeZoneKeyForOffset(offsetMinutes);
                        }
                        catch (Exception e) {
                            throw new PrestoException(
                                    INVALID_SESSION_PROPERTY,
                                    format("Invalid time zone offset minutes: %d", offsetMinutes));
                        }
                    }
                    else {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("Time zone must be a string or interval, got: %s", result.getClass().getSimpleName()));
                    }
                }
                catch (PrestoException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new PrestoException(
                            INVALID_SESSION_PROPERTY,
                            format("Unable to evaluate time zone expression: %s", e.getMessage()));
                }
            }
        }

        // Set the time zone in the session
        stateMachine.addSetSessionProperties(TIME_ZONE_ID, timeZoneKey.getId());

        return immediateFuture(null);
    }

    private long parseIntervalToMinutes(
            IntervalLiteral.Sign sign,
            String value,
            IntervalLiteral.IntervalField startField,
            IntervalLiteral.IntervalField endField)
    {
        long totalMinutes = 0;
        boolean negative = (sign == IntervalLiteral.Sign.NEGATIVE);

        // Parse the value based on the interval fields
        if (startField == IntervalLiteral.IntervalField.HOUR && endField == IntervalLiteral.IntervalField.HOUR) {
            // INTERVAL '10' HOUR
            totalMinutes = Long.parseLong(value) * 60;
        }
        else if (startField == IntervalLiteral.IntervalField.MINUTE && endField == IntervalLiteral.IntervalField.MINUTE) {
            // INTERVAL '30' MINUTE
            totalMinutes = Long.parseLong(value);
        }
        else if (startField == IntervalLiteral.IntervalField.HOUR && endField == IntervalLiteral.IntervalField.MINUTE) {
            // INTERVAL '10:30' HOUR TO MINUTE or INTERVAL '10' HOUR TO MINUTE (just hours)
            if (value.contains(":")) {
                String[] parts = value.split(":");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid HOUR TO MINUTE interval format");
                }
                totalMinutes = Long.parseLong(parts[0]) * 60 + Long.parseLong(parts[1]);
            }
            else {
                totalMinutes = Long.parseLong(value) * 60;
            }
        }
        else {
            throw new IllegalArgumentException(
                    format("Unsupported interval fields for time zone: %s TO %s", startField, endField));
        }

        return negative ? -totalMinutes : totalMinutes;
    }
}
