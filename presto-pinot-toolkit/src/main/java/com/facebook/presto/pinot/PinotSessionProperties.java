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

package com.facebook.presto.pinot;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.base.Preconditions.checkArgument;

public class PinotSessionProperties
{
    public static final String FORBID_BROKER_QUERIES = "forbid_broker_queries";
    public static final String ATTEMPT_BROKER_QUERIES = "attempt_broker_queries";
    public static final String RETRY_COUNT = "retry_count";
    public static final String MARK_DATA_FETCH_EXCEPTIONS_AS_RETRIABLE = "mark_data_fetch_exceptions_as_retriable";
    public static final String USE_DATE_TRUNC = "use_date_trunc";
    public static final String NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES = "non_aggregate_limit_for_broker_queries";
    public static final String PUSHDOWN_TOPN_BROKER_QUERIES = "pushdown_topn_broker_queries";
    public static final String PUSHDOWN_PROJECT_EXPRESSIONS = "pushdown_project_expressions";
    public static final String FORBID_SEGMENT_QUERIES = "forbid_segment_queries";
    public static final String NUM_SEGMENTS_PER_SPLIT = "num_segments_per_split";
    public static final String TOPN_LARGE = "topn_large";
    public static final String LIMIT_LARGE_FOR_SEGMENT = "limit_larger_for_segment";
    public static final String OVERRIDE_DISTINCT_COUNT_FUNCTION = "override_distinct_count_function";
    public static final String CONTROLLER_AUTHENTICATION_USER = "controller_authentication_user";
    public static final String CONTROLLER_AUTHENTICATION_PASSWORD = "controller_authentication_password";
    public static final String BROKER_AUTHENTICATION_USER = "broker_authentication_user";
    public static final String BROKER_AUTHENTICATION_PASSWORD = "broker_authentication_password";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static int getNumSegmentsPerSplit(ConnectorSession session)
    {
        int segmentsPerSplit = session.getProperty(NUM_SEGMENTS_PER_SPLIT, Integer.class);
        return segmentsPerSplit <= 0 ? Integer.MAX_VALUE : segmentsPerSplit;
    }

    public static boolean isForbidBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(FORBID_BROKER_QUERIES, Boolean.class);
    }

    public static boolean isForbidSegmentQueries(ConnectorSession session)
    {
        return session.getProperty(FORBID_SEGMENT_QUERIES, Boolean.class);
    }

    public static boolean isAttemptBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(ATTEMPT_BROKER_QUERIES, Boolean.class);
    }

    public static int getPinotRetryCount(ConnectorSession session)
    {
        return session.getProperty(RETRY_COUNT, Integer.class);
    }

    public static boolean isMarkDataFetchExceptionsAsRetriable(ConnectorSession session)
    {
        return session.getProperty(MARK_DATA_FETCH_EXCEPTIONS_AS_RETRIABLE, Boolean.class);
    }

    public static boolean isUseDateTruncation(ConnectorSession session)
    {
        return session.getProperty(USE_DATE_TRUNC, Boolean.class);
    }

    public static int getNonAggregateLimitForBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES, Integer.class);
    }

    public static boolean getPushdownTopnBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_TOPN_BROKER_QUERIES, Boolean.class);
    }

    public static boolean getPushdownProjectExpressions(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_PROJECT_EXPRESSIONS, Boolean.class);
    }

    public static int getTopNLarge(ConnectorSession session)
    {
        return session.getProperty(TOPN_LARGE, Integer.class);
    }

    public static int getLimitLargerForSegment(ConnectorSession session)
    {
        return session.getProperty(LIMIT_LARGE_FOR_SEGMENT, Integer.class);
    }

    public static String getOverrideDistinctCountFunction(ConnectorSession session)
    {
        return session.getProperty(OVERRIDE_DISTINCT_COUNT_FUNCTION, String.class);
    }

    public static String getControllerAuthenticationUser(ConnectorSession session)
    {
        return session.getProperty(CONTROLLER_AUTHENTICATION_USER, String.class);
    }

    public static String getControllerAuthenticationPassword(ConnectorSession session)
    {
        return session.getProperty(CONTROLLER_AUTHENTICATION_PASSWORD, String.class);
    }

    public static String getBrokerAuthenticationUser(ConnectorSession session)
    {
        return session.getProperty(BROKER_AUTHENTICATION_USER, String.class);
    }

    public static String getBrokerAuthenticationPassword(ConnectorSession session)
    {
        return session.getProperty(BROKER_AUTHENTICATION_PASSWORD, String.class);
    }

    @Inject
    public PinotSessionProperties(PinotConfig pinotConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        FORBID_BROKER_QUERIES,
                        "Forbid queries to the broker",
                        pinotConfig.isForbidBrokerQueries(),
                        false),
                booleanProperty(
                        FORBID_SEGMENT_QUERIES,
                        "Forbid segment queries",
                        pinotConfig.isForbidSegmentQueries(),
                        false),
                booleanProperty(
                        ATTEMPT_BROKER_QUERIES,
                        "Attempt broker queries",
                        pinotConfig.isAttemptBrokerQueries(),
                        false),
                integerProperty(
                        RETRY_COUNT,
                        "Retry count for retriable pinot data fetch calls",
                        pinotConfig.getFetchRetryCount(),
                        false),
                booleanProperty(
                        MARK_DATA_FETCH_EXCEPTIONS_AS_RETRIABLE,
                        "Retry Pinot query on data fetch exceptions",
                        pinotConfig.isMarkDataFetchExceptionsAsRetriable(),
                        false),
                integerProperty(
                        NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES,
                        "Max limit for non aggregate queries to the pinot broker",
                        pinotConfig.getNonAggregateLimitForBrokerQueries(),
                        false),
                integerProperty(
                        LIMIT_LARGE_FOR_SEGMENT,
                        "Server query selection limit for large segment",
                        pinotConfig.getLimitLargeForSegment(),
                        false),
                integerProperty(
                        TOPN_LARGE,
                        "Broker query group by limit",
                        pinotConfig.getTopNLarge(),
                        false),
                stringProperty(
                        OVERRIDE_DISTINCT_COUNT_FUNCTION,
                        "Override distinct count function to another function name",
                        pinotConfig.getOverrideDistinctCountFunction(),
                        false),
                stringProperty(
                        CONTROLLER_AUTHENTICATION_USER,
                        "Controller authentication user",
                        pinotConfig.getControllerAuthenticationUser(),
                        false),
                stringProperty(
                        CONTROLLER_AUTHENTICATION_PASSWORD,
                        "Controller authentication password",
                        pinotConfig.getControllerAuthenticationPassword(),
                        false),
                stringProperty(
                        BROKER_AUTHENTICATION_USER,
                        "Broker authentication user",
                        pinotConfig.getBrokerAuthenticationUser(),
                        false),
                stringProperty(
                        BROKER_AUTHENTICATION_PASSWORD,
                        "Broker authentication password",
                        pinotConfig.getBrokerAuthenticationPassword(),
                        false),
                booleanProperty(
                        USE_DATE_TRUNC,
                        "Use the new UDF dateTrunc in pinot that is more presto compatible",
                        pinotConfig.isUseDateTrunc(),
                        false),
                booleanProperty(
                        PUSHDOWN_TOPN_BROKER_QUERIES,
                        "Push down order by to pinot broker for top queries",
                        pinotConfig.isPushdownTopNBrokerQueries(),
                        false),
                booleanProperty(
                        PUSHDOWN_PROJECT_EXPRESSIONS,
                        "Push down expressions in projection to Pinot broker",
                        pinotConfig.isPushdownProjectExpressions(),
                        false),
                new PropertyMetadata<>(
                        NUM_SEGMENTS_PER_SPLIT,
                        "Number of segments of the same host per split",
                        INTEGER,
                        Integer.class,
                        pinotConfig.getNumSegmentsPerSplit(),
                        false,
                        value -> {
                            int ret = ((Number) value).intValue();
                            checkArgument(ret > 0, "Number of segments per split must be more than zero");
                            return ret;
                        },
                        object -> object));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
