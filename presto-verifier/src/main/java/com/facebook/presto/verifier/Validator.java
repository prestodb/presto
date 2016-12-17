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
package com.facebook.presto.verifier;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.verifier.Validator.ChangedRow.Changed;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedMultiset;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.presto.verifier.QueryResult.State;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.primitives.Doubles.isFinite;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class Validator
{
    private final String testUsername;
    private final String controlUsername;
    private final String testPassword;
    private final String controlPassword;
    private final String controlGateway;
    private final String testGateway;
    private final Duration controlTimeout;
    private final Duration testTimeout;
    private final int maxRowCount;
    private final boolean checkCorrectness;
    private final boolean checkDeterministic;
    private final boolean verboseResultsComparison;
    private final QueryPair queryPair;
    private final boolean explainOnly;
    private final Map<String, String> sessionProperties;
    private final int precision;
    private final int controlTeardownRetries;
    private final int testTeardownRetries;

    private Boolean valid;

    private QueryResult controlResult;
    private QueryResult testResult;

    private final List<QueryResult> controlPreQueryResults = new ArrayList<>();
    private final List<QueryResult> controlPostQueryResults = new ArrayList<>();
    private final List<QueryResult> testPreQueryResults = new ArrayList<>();
    private final List<QueryResult> testPostQueryResults = new ArrayList<>();

    private boolean deterministic = true;

    public Validator(
            String controlGateway,
            String testGateway,
            Duration controlTimeout,
            Duration testTimeout,
            int maxRowCount,
            boolean explainOnly,
            int precision,
            boolean checkCorrectness,
            boolean checkDeterministic,
            boolean verboseResultsComparison,
            int controlTeardownRetries,
            int testTeardownRetries,
            QueryPair queryPair)
    {
        this.testUsername = requireNonNull(queryPair.getTest().getUsername(), "test username is null");
        this.controlUsername = requireNonNull(queryPair.getControl().getUsername(), "control username is null");
        this.testPassword = queryPair.getTest().getPassword();
        this.controlPassword = queryPair.getControl().getPassword();
        this.controlGateway = requireNonNull(controlGateway, "controlGateway is null");
        this.testGateway = requireNonNull(testGateway, "testGateway is null");
        this.controlTimeout = controlTimeout;
        this.testTimeout = testTimeout;
        this.maxRowCount = maxRowCount;
        this.explainOnly = explainOnly;
        this.precision = precision;
        this.checkCorrectness = checkCorrectness;
        this.checkDeterministic = checkDeterministic;
        this.verboseResultsComparison = verboseResultsComparison;
        this.controlTeardownRetries = controlTeardownRetries;
        this.testTeardownRetries = testTeardownRetries;

        this.queryPair = requireNonNull(queryPair, "queryPair is null");
        // Test and Control always have the same session properties.
        this.sessionProperties = queryPair.getTest().getSessionProperties();
    }

    public boolean isSkipped()
    {
        if (queryPair.getControl().getQuery().isEmpty() || queryPair.getTest().getQuery().isEmpty()) {
            return true;
        }

        if (getControlResult().getState() != State.SUCCESS) {
            return true;
        }

        if (!isDeterministic()) {
            return true;
        }

        if (getTestResult().getState() == State.TIMEOUT) {
            return true;
        }

        return false;
    }

    public String getSkippedMessage()
    {
        StringBuilder sb = new StringBuilder();
        if (getControlResult().getState() == State.TOO_MANY_ROWS) {
            sb.append("----------\n");
            sb.append("Name: " + queryPair.getName() + "\n");
            sb.append("Schema (control): " + queryPair.getControl().getSchema() + "\n");
            sb.append("Too many rows.\n");
        }
        else if (!isDeterministic()) {
            sb.append("----------\n");
            sb.append("Name: " + queryPair.getName() + "\n");
            sb.append("Schema (control): " + queryPair.getControl().getSchema() + "\n");
            sb.append("NON DETERMINISTIC\n");
        }
        else if (getControlResult().getState() == State.TIMEOUT || getTestResult().getState() == State.TIMEOUT) {
            sb.append("----------\n");
            sb.append("Name: " + queryPair.getName() + "\n");
            sb.append("Schema (control): " + queryPair.getControl().getSchema() + "\n");
            sb.append("TIMEOUT\n");
        }
        else {
            sb.append("SKIPPED: ");
            if (getControlResult().getException() != null) {
                sb.append(getControlResult().getException().getMessage());
            }
        }
        return sb.toString();
    }

    public boolean valid()
    {
        if (valid == null) {
            valid = validate();
        }
        return valid;
    }

    public boolean isDeterministic()
    {
        if (valid == null) {
            valid = validate();
        }
        return deterministic;
    }

    private boolean validate()
    {
        controlResult = executeQueryControl();

        // query has too many rows. Consider blacklisting.
        if (controlResult.getState() == State.TOO_MANY_ROWS) {
            testResult = new QueryResult(State.INVALID, null, null, null, null, ImmutableList.of());
            return false;
        }
        // query failed in the control
        if (controlResult.getState() != State.SUCCESS) {
            testResult = new QueryResult(State.INVALID, null, null, null, null, ImmutableList.of());
            return true;
        }

        testResult = executeQueryTest();

        if (controlResult.getState() != State.SUCCESS || testResult.getState() != State.SUCCESS) {
            return false;
        }

        if (!checkCorrectness) {
            return true;
        }

        boolean matches = resultsMatch(controlResult, testResult, precision);
        if (!matches && checkDeterministic) {
            return checkForDeterministicAndRerunTestQueriesIfNeeded();
        }
        return matches;
    }

    private static QueryResult tearDown(Query query, List<QueryResult> postQueryResults, Function<String, QueryResult> executor)
    {
        postQueryResults.clear();
        for (String postqueryString : query.getPostQueries()) {
            QueryResult queryResult = executor.apply(postqueryString);
            postQueryResults.add(queryResult);
            if (queryResult.getState() != State.SUCCESS) {
                return new QueryResult(State.FAILED_TO_TEARDOWN, queryResult.getException(), queryResult.getWallTime(), queryResult.getCpuTime(), queryResult.getQueryId(), ImmutableList.of());
            }
        }

        return new QueryResult(State.SUCCESS, null, null, null, null, ImmutableList.of());
    }

    private static QueryResult setup(Query query, List<QueryResult> preQueryResults, Function<String, QueryResult> executor)
    {
        preQueryResults.clear();
        for (String prequeryString : query.getPreQueries()) {
            QueryResult queryResult = executor.apply(prequeryString);
            preQueryResults.add(queryResult);
            if (queryResult.getState() != State.SUCCESS) {
                return new QueryResult(State.FAILED_TO_SETUP, queryResult.getException(), queryResult.getWallTime(), queryResult.getCpuTime(), queryResult.getQueryId(), ImmutableList.of());
            }
        }

        return new QueryResult(State.SUCCESS, null, null, null, null, ImmutableList.of());
    }

    private boolean checkForDeterministicAndRerunTestQueriesIfNeeded()
    {
        // check if the control query is deterministic
        for (int i = 0; i < 3; i++) {
            QueryResult results = executeQueryControl();
            if (results.getState() != State.SUCCESS) {
                return false;
            }

            if (!resultsMatch(controlResult, results, precision)) {
                deterministic = false;
                return false;
            }
        }

        // Re-run the test query to confirm that the results don't match, in case there was caching on the test tier,
        // but require that it matches 3 times in a row to rule out a non-deterministic correctness bug.
        for (int i = 0; i < 3; i++) {
            testResult = executeQueryTest();
            if (testResult.getState() != State.SUCCESS) {
                return false;
            }
            if (!resultsMatch(controlResult, testResult, precision)) {
                return false;
            }
        }

        // test result agrees with control result 3 times in a row although the first test result didn't agree
        return true;
    }

    private QueryResult executeQueryTest()
    {
        Query query = queryPair.getTest();
        QueryResult queryResult = new QueryResult(State.INVALID, null, null, null, null, ImmutableList.of());
        try {
            // startup
            queryResult = setup(query, testPreQueryResults, testPrequery -> executeQuery(testGateway, testUsername, testPassword, queryPair.getTest(), testPrequery, testTimeout, sessionProperties));

            // if startup is successful -> execute query
            if (queryResult.getState() == State.SUCCESS) {
                queryResult = executeQuery(testGateway, testUsername, testPassword, queryPair.getTest(), query.getQuery(), testTimeout, sessionProperties);
            }
        }
        finally {
            int retry = 0;
            QueryResult tearDownResult;
            do {
                tearDownResult = tearDown(query, testPostQueryResults, testPostquery -> executeQuery(testGateway, testUsername, testPassword, queryPair.getTest(), testPostquery, testTimeout, sessionProperties));
                if (tearDownResult.getState() == State.SUCCESS) {
                    break;
                }
                try {
                    TimeUnit.MINUTES.sleep(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                retry++;
            }
            while (retry < testTeardownRetries);
            // if teardown is not successful the query fails
            queryResult = tearDownResult.getState() == State.SUCCESS ? queryResult : tearDownResult;
        }
        return queryResult;
    }

    private QueryResult executeQueryControl()
    {
        Query query = queryPair.getControl();
        QueryResult queryResult = new QueryResult(State.INVALID, null, null, null, null, ImmutableList.of());
        try {
            // startup
            queryResult = setup(query, controlPreQueryResults, controlPrequery -> executeQuery(controlGateway, controlUsername, controlPassword, queryPair.getControl(), controlPrequery, controlTimeout, sessionProperties));

            // if startup is successful -> execute query
            if (queryResult.getState() == State.SUCCESS) {
                queryResult = executeQuery(controlGateway, controlUsername, controlPassword, queryPair.getControl(), query.getQuery(), controlTimeout, sessionProperties);
            }
        }
        finally {
            int retry = 0;
            QueryResult tearDownResult;
            do {
                tearDownResult = tearDown(query, controlPostQueryResults, controlPostquery -> executeQuery(controlGateway, controlUsername, controlPassword, queryPair.getControl(), controlPostquery, controlTimeout, sessionProperties));
                if (tearDownResult.getState() == State.SUCCESS) {
                    break;
                }
                try {
                    TimeUnit.MINUTES.sleep(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                retry++;
            }
            while (retry < controlTeardownRetries);
            // if teardown is not successful the query fails
            queryResult = tearDownResult.getState() == State.SUCCESS ? queryResult : tearDownResult;
        }
        return queryResult;
    }

    public QueryPair getQueryPair()
    {
        return queryPair;
    }

    public QueryResult getControlResult()
    {
        return controlResult;
    }

    public QueryResult getTestResult()
    {
        return testResult;
    }

    public List<QueryResult> getControlPreQueryResults()
    {
        return controlPreQueryResults;
    }

    public List<QueryResult> getControlPostQueryResults()
    {
        return controlPostQueryResults;
    }

    public List<QueryResult> getTestPreQueryResults()
    {
        return testPreQueryResults;
    }

    public List<QueryResult> getTestPostQueryResults()
    {
        return testPostQueryResults;
    }

    private QueryResult executeQuery(String url, String username, String password, Query query, String sql, Duration timeout, Map<String, String> sessionProperties)
    {
        String queryId = null;
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            trySetConnectionProperties(query, connection);
            for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
                connection.unwrap(PrestoConnection.class).setSessionProperty(entry.getKey(), entry.getValue());
            }

            try (Statement statement = connection.createStatement()) {
                TimeLimiter limiter = new SimpleTimeLimiter();
                Stopwatch stopwatch = Stopwatch.createStarted();
                Statement limitedStatement = limiter.newProxy(statement, Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (explainOnly) {
                    sql = "EXPLAIN " + sql;
                }
                long start = System.nanoTime();
                PrestoStatement prestoStatement = limitedStatement.unwrap(PrestoStatement.class);
                ProgressMonitor progressMonitor = new ProgressMonitor();
                prestoStatement.setProgressMonitor(progressMonitor);
                try {
                    boolean isSelectQuery = limitedStatement.execute(sql);
                    List<List<Object>> results = null;
                    if (isSelectQuery) {
                        results = limiter.callWithTimeout(
                                getResultSetConverter(limitedStatement.getResultSet()),
                                timeout.toMillis() - stopwatch.elapsed(TimeUnit.MILLISECONDS),
                                TimeUnit.MILLISECONDS, true);
                    }
                    else {
                        results = ImmutableList.of(ImmutableList.of(limitedStatement.getLargeUpdateCount()));
                    }
                    prestoStatement.clearProgressMonitor();
                    QueryStats queryStats = progressMonitor.getFinalQueryStats();
                    if (queryStats == null) {
                        throw new VerifierException("Cannot fetch query stats");
                    }
                    Duration queryCpuTime = new Duration(queryStats.getCpuTimeMillis(), TimeUnit.MILLISECONDS);
                    queryId = queryStats.getQueryId();
                    return new QueryResult(State.SUCCESS, null, nanosSince(start), queryCpuTime, queryId, results);
                }
                catch (AssertionError e) {
                    if (e.getMessage().startsWith("unimplemented type:")) {
                        return new QueryResult(State.INVALID, null, null, null, queryId, ImmutableList.of());
                    }
                    throw e;
                }
                catch (SQLException | VerifierException e) {
                    throw e;
                }
                catch (UncheckedTimeoutException e) {
                    return new QueryResult(State.TIMEOUT, null, null, null, queryId, ImmutableList.of());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        catch (SQLException e) {
            Exception exception = e;
            if (("Error executing query".equals(e.getMessage()) || "Error fetching results".equals(e.getMessage())) &&
                    (e.getCause() instanceof Exception)) {
                exception = (Exception) e.getCause();
            }
            State state = isPrestoQueryInvalid(e) ? State.INVALID : State.FAILED;
            return new QueryResult(state, exception, null, null, null, null);
        }
        catch (VerifierException e) {
            return new QueryResult(State.TOO_MANY_ROWS, e, null, null, null, null);
        }
    }

    private void trySetConnectionProperties(Query query, Connection connection)
            throws SQLException
    {
        // Required for jdbc drivers that do not implement all/some of these functions (eg. impala jdbc driver)
        // For these drivers, set the database default values in the query database
        try {
            connection.setClientInfo("ApplicationName", "verifier-test:" + queryPair.getName());
            connection.setCatalog(query.getCatalog());
            connection.setSchema(query.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }
    }

    private Callable<List<List<Object>>> getResultSetConverter(ResultSet resultSet)
    {
        return () -> convertJdbcResultSet(resultSet);
    }

    private static boolean isPrestoQueryInvalid(SQLException e)
    {
        for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
            if (t.toString().contains(".SemanticException:")) {
                return true;
            }
            if (t.toString().contains(".ParsingException:")) {
                return true;
            }
            if (nullToEmpty(t.getMessage()).matches("Function .* not registered")) {
                return true;
            }
        }
        return false;
    }

    private List<List<Object>> convertJdbcResultSet(ResultSet resultSet)
            throws SQLException, VerifierException
    {
        int rowCount = 0;
        int columnCount = resultSet.getMetaData().getColumnCount();

        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultSet.getObject(i);
                if (object instanceof BigDecimal) {
                    if (((BigDecimal) object).scale() <= 0) {
                        object = ((BigDecimal) object).longValueExact();
                    }
                    else {
                        object = ((BigDecimal) object).doubleValue();
                    }
                }
                if (object instanceof Array) {
                    object = ((Array) object).getArray();
                }
                if (object instanceof byte[]) {
                    object = new SqlVarbinary((byte[]) object);
                }
                row.add(object);
            }
            rows.add(unmodifiableList(row));
            rowCount++;
            if (rowCount > maxRowCount) {
                throw new VerifierException("More than '" + maxRowCount + "' rows, failing query");
            }
        }
        return rows.build();
    }

    private static boolean resultsMatch(QueryResult controlResult, QueryResult testResult, int precision)
    {
        SortedMultiset<List<Object>> control = ImmutableSortedMultiset.copyOf(rowComparator(precision), controlResult.getResults());
        SortedMultiset<List<Object>> test = ImmutableSortedMultiset.copyOf(rowComparator(precision), testResult.getResults());
        try {
            return control.equals(test);
        }
        catch (TypesDoNotMatchException e) {
            return false;
        }
    }

    public String getResultsComparison(int precision)
    {
        List<List<Object>> controlResults = controlResult.getResults();
        List<List<Object>> testResults = testResult.getResults();

        if (valid() || (controlResults == null) || (testResults == null)) {
            return "";
        }

        Multiset<List<Object>> control = ImmutableSortedMultiset.copyOf(rowComparator(precision), controlResults);
        Multiset<List<Object>> test = ImmutableSortedMultiset.copyOf(rowComparator(precision), testResults);

        try {
            Iterable<ChangedRow> diff = ImmutableSortedMultiset.<ChangedRow>naturalOrder()
                    .addAll(Iterables.transform(Multisets.difference(control, test), row -> new ChangedRow(Changed.REMOVED, row, precision)))
                    .addAll(Iterables.transform(Multisets.difference(test, control), row -> new ChangedRow(Changed.ADDED, row, precision)))
                    .build();
            diff = Iterables.limit(diff, 100);

            StringBuilder sb = new StringBuilder();

            sb.append(format("Control %s rows, Test %s rows%n", control.size(), test.size()));
            if (verboseResultsComparison) {
                Joiner.on("\n").appendTo(sb, diff);
            }
            else {
                sb.append("RESULTS DO NOT MATCH\n");
            }

            return sb.toString();
        }
        catch (TypesDoNotMatchException e) {
            return e.getMessage();
        }
    }

    private static Comparator<List<Object>> rowComparator(int precision)
    {
        Comparator<Object> comparator = Ordering.from(columnComparator(precision)).nullsFirst();
        return (a, b) -> {
            if (a.size() != b.size()) {
                return Integer.compare(a.size(), b.size());
            }
            for (int i = 0; i < a.size(); i++) {
                int r = comparator.compare(a.get(i), b.get(i));
                if (r != 0) {
                    return r;
                }
            }
            return 0;
        };
    }

    private static Comparator<Object> columnComparator(int precision)
    {
        return (a, b) -> {
            if (a == null || b == null) {
                if (a == null && b == null) {
                    return 0;
                }
                return a == null ? -1 : 1;
            }
            if (a instanceof Number && b instanceof Number) {
                Number x = (Number) a;
                Number y = (Number) b;
                boolean bothReal = isReal(x) && isReal(y);
                boolean bothIntegral = isIntegral(x) && isIntegral(y);
                if (!(bothReal || bothIntegral)) {
                    throw new TypesDoNotMatchException(format("item types do not match: %s vs %s", a.getClass().getName(), b.getClass().getName()));
                }
                if (isIntegral(x)) {
                    return Long.compare(x.longValue(), y.longValue());
                }
                return precisionCompare(x.doubleValue(), y.doubleValue(), precision);
            }
            if (a.getClass() != b.getClass()) {
                throw new TypesDoNotMatchException(format("item types do not match: %s vs %s", a.getClass().getName(), b.getClass().getName()));
            }
            if ((a.getClass().isArray() && b.getClass().isArray())) {
                Object[] aArray = (Object[]) a;
                Object[] bArray = (Object[]) b;

                if (aArray.length != bArray.length) {
                    return Arrays.hashCode((Object[]) a) < Arrays.hashCode((Object[]) b) ? -1 : 1;
                }

                for (int i = 0; i < aArray.length; i++) {
                    int compareResult = columnComparator(precision).compare(aArray[i], bArray[i]);
                    if (compareResult != 0) {
                        return compareResult;
                    }
                }

                return 0;
            }
            if (a instanceof List && b instanceof List) {
                List aList = (List) a;
                List bList = (List) b;

                if (aList.size() != bList.size()) {
                    return a.hashCode() < b.hashCode() ? -1 : 1;
                }

                for (int i = 0; i < aList.size(); i++) {
                    int compareResult = columnComparator(precision).compare(aList.get(i), bList.get(i));
                    if (compareResult != 0) {
                        return compareResult;
                    }
                }

                return 0;
            }
            if (a instanceof Map && b instanceof Map) {
                Map aMap = (Map) a;
                Map bMap = (Map) b;

                if (aMap.size() != bMap.size()) {
                    return a.hashCode() < b.hashCode() ? -1 : 1;
                }

                for (Object aKey : aMap.keySet()) {
                    boolean foundMatchingKey = false;
                    for (Object bKey : bMap.keySet()) {
                        if (columnComparator(precision).compare(aKey, bKey) == 0) {
                            int compareResult = columnComparator(precision).compare(aMap.get(aKey), bMap.get(bKey));
                            if (compareResult != 0) {
                                return compareResult;
                            }
                            foundMatchingKey = true;
                        }
                    }
                    if (!foundMatchingKey) {
                        return a.hashCode() < b.hashCode() ? -1 : 1;
                    }
                }

                return 0;
            }
            checkArgument(a instanceof Comparable, "item is not Comparable: %s", a.getClass().getName());
            return ((Comparable<Object>) a).compareTo(b);
        };
    }

    private static boolean isReal(Number x)
    {
        return x instanceof Float || x instanceof Double;
    }

    private static boolean isIntegral(Number x)
    {
        return x instanceof Byte || x instanceof Short || x instanceof Integer || x instanceof Long;
    }

    private static int precisionCompare(double a, double b, int precision)
    {
        if (!isFinite(a) || !isFinite(b)) {
            return Double.compare(a, b);
        }

        MathContext context = new MathContext(precision);
        BigDecimal x = new BigDecimal(a).round(context);
        BigDecimal y = new BigDecimal(b).round(context);
        return x.compareTo(y);
    }

    public static class ChangedRow
            implements Comparable<ChangedRow>
    {
        public enum Changed
        {
            ADDED, REMOVED
        }

        private final Changed changed;
        private final List<Object> row;
        private final int precision;

        private ChangedRow(Changed changed, List<Object> row, int precision)
        {
            this.changed = changed;
            this.row = row;
            this.precision = precision;
        }

        @Override
        public String toString()
        {
            if (changed == Changed.ADDED) {
                return "+ " + row;
            }
            else {
                return "- " + row;
            }
        }

        @Override
        public int compareTo(ChangedRow that)
        {
            return ComparisonChain.start()
                    .compare(this.row, that.row, rowComparator(precision))
                    .compareFalseFirst(this.changed == Changed.ADDED, that.changed == Changed.ADDED)
                    .result();
        }
    }

    private static class ProgressMonitor
            implements Consumer<QueryStats>
    {
        private QueryStats queryStats;
        private boolean finished = false;

        @Override
        public synchronized void accept(QueryStats queryStats)
        {
            checkState(!finished);
            this.queryStats = queryStats;
        }

        public synchronized QueryStats getFinalQueryStats()
        {
            finished = true;
            return queryStats;
        }
    }
}
