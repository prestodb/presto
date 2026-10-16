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
import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.SetTimeZone;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.IntervalDayTimeType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.facebook.presto.SystemSessionProperties.TIME_ZONE_ID;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.facebook.presto.util.Failures.checkCondition;
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
        String timeZoneId = statement.getTimeZone()
                .map(timeZone -> getTimeZoneId(timeZone, statement, metadata, stateMachine, parameters))
                .orElse(TimeZone.getDefault().getID());
        stateMachine.addSetSessionProperties(TIME_ZONE_ID, timeZoneId);

        return immediateFuture(null);
    }

    private static String getTimeZoneId(
            Expression expression,
            SetTimeZone statement,
            Metadata metadata,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createConstantAnalyzer(
                metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                session,
                parameterLookup,
                WarningCollector.NOOP);
        analyzer.analyze(expression, Scope.create());
        Type expressionType = analyzer.getExpressionTypes().get(NodeRef.of(expression));

        if (!(expressionType instanceof com.facebook.presto.common.type.VarcharType || expressionType instanceof IntervalDayTimeType)) {
            throw new PrestoException(
                    INVALID_SESSION_PROPERTY,
                    format("Expected expression of varchar or interval day-time type, but '%s' has %s type", expression, expressionType.getDisplayName()));
        }

        Object timeZoneValue = evaluateConstantExpression(expression, expressionType, metadata, session, parameterLookup);

        TimeZoneKey timeZoneKey;
        if (timeZoneValue instanceof Slice) {
            String zoneId = ((Slice) timeZoneValue).toStringUtf8();
            try {
                timeZoneKey = getTimeZoneKey(zoneId);
            }
            catch (TimeZoneNotSupportedException e) {
                throw new PrestoException(INVALID_SESSION_PROPERTY, format("Time zone not supported: %s", zoneId), e);
            }
        }
        else if (timeZoneValue instanceof Long) {
            try {
                timeZoneKey = getTimeZoneKeyForOffset(getZoneOffsetMinutes((Long) timeZoneValue));
            }
            catch (InvalidFunctionArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
            catch (TimeZoneNotSupportedException e) {
                throw new PrestoException(INVALID_SESSION_PROPERTY, e.getMessage(), e);
            }
        }
        else {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("Time zone expression '%s' not supported", expression));
        }
        return timeZoneKey.getId();
    }

    private static long getZoneOffsetMinutes(long interval)
    {
        checkCondition((interval % 60_000L) == 0L, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        return interval / 60_000L;
    }
}
