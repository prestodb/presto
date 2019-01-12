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
package io.prestosql.verifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.jdbc.PrestoConnection;
import io.prestosql.jdbc.PrestoStatement;
import io.prestosql.jdbc.QueryStats;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.verifier.Validator.ChangedRow.Changed;

import java.math.BigDecimal;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.primitives.Doubles.isFinite;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.verifier.QueryResult.State;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Validator
{
    private static final Logger log = Logger.get(Validator.class);

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
    private final boolean runTearDownOnResultMismatch;

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
            boolean runTearDownOnResultMismatch,
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
        this.runTearDownOnResultMismatch = runTearDownOnResultMismatch;

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
        boolean tearDownControl = true;
        boolean tearDownTest = false;
        try {
            controlResult = executePreAndMainForControl();

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

            testResult = executePreAndMainForTest();
            tearDownTest = true;

            if (controlResult.getState() != State.SUCCESS || testResult.getState() != State.SUCCESS) {
                return false;
            }

            if (!checkCorrectness) {
                return true;
            }

            boolean matches = resultsMatch(controlResult, testResult, precision);
            if (!matches && checkDeterministic) {
                matches = checkForDeterministicAndRerunTestQueriesIfNeeded();
            }
            if (!matches && !runTearDownOnResultMismatch) {
                tearDownControl = false;
                tearDownTest = false;
            }
            return matches;
        }
        finally {
            if (tearDownControl) {
                tearDownControl();
            }
            if (tearDownTest) {
                tearDownTest();
            }
        }
    }

    private void tearDownControl()
    {
        QueryResult controlTearDownResult = executeTearDown(
                queryPair.getControl(),
                controlGateway,
                controlUsername,
                controlPassword,
                controlTimeout,
                controlPostQueryResults,
                controlTeardownRetries);
        if (controlTearDownResult.getState() != State.SUCCESS) {
            log.warn("Control table teardown failed");
        }
    }

    private void tearDownTest()
    {
        QueryResult testTearDownResult = executeTearDown(
                queryPair.getTest(),
                testGateway,
                testUsername,
                testPassword,
                testTimeout,
                testPostQueryResults,
                testTeardownRetries);
        if (testTearDownResult.getState() != State.SUCCESS) {
            log.warn("Test table teardown failed");
        }
    }

    private QueryResult tearDown(Query query, List<QueryResult> postQueryResults, Function<String, QueryResult> executor)
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
            if (queryResult.getState() == State.TIMEOUT) {
                return queryResult;
            }
            else if (queryResult.getState() != State.SUCCESS) {
                return new QueryResult(State.FAILED_TO_SETUP, queryResult.getException(), queryResult.getWallTime(), queryResult.getCpuTime(), queryResult.getQueryId(), ImmutableList.of());
            }
        }

        return new QueryResult(State.SUCCESS, null, null, null, null, ImmutableList.of());
    }

    private boolean checkForDeterministicAndRerunTestQueriesIfNeeded()
    {
        // check if the control query is deterministic
        for (int i = 0; i < 3; i++) {
            QueryResult results = executePreAndMainForControl();
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
            testResult = executePreAndMainForTest();
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

    private QueryResult executePreAndMainForTest()
    {
        return executePreAndMain(
                queryPair.getTest(),
                testPreQueryResults,
                testGateway,
                testUsername,
                testPassword,
                testTimeout,
                testPostQueryResults,
                testTeardownRetries);
    }

    private QueryResult executePreAndMainForControl()
    {
        return executePreAndMain(
                queryPair.getControl(),
                controlPreQueryResults,
                controlGateway,
                controlUsername,
                controlPassword,
                controlTimeout,
                controlPostQueryResults,
                controlTeardownRetries);
    }

    private QueryResult executePreAndMain(
            Query query,
            List<QueryResult> preQueryResults,
            String gateway,
            String username,
            String password,
            Duration timeout,
            List<QueryResult> postQueryResults,
            int teardownRetries)
    {
        try {
            // startup
            QueryResult queryResult = setup(query, preQueryResults, preQuery ->
                    executeQuery(gateway, username, password, query, preQuery, timeout, sessionProperties));

            // if startup is successful -> execute query
            if (queryResult.getState() == State.SUCCESS) {
                queryResult = executeQuery(gateway, username, password, query, query.getQuery(), timeout, sessionProperties);
            }

            return queryResult;
        }
        catch (Exception e) {
            executeTearDown(query, gateway, username, password, timeout, postQueryResults, teardownRetries);
            throw e;
        }
    }

    private QueryResult executeTearDown(
            Query query,
            String gateway,
            String username,
            String password,
            Duration timeout,
            List<QueryResult> postQueryResults,
            int teardownRetries)
    {
        int attempt = 0;
        QueryResult tearDownResult;
        do {
            tearDownResult = tearDown(query, postQueryResults, postQuery ->
                    executeQuery(gateway, username, password, query, postQuery, timeout, sessionProperties));
            if (tearDownResult.getState() == State.SUCCESS) {
                break;
            }
            try {
                TimeUnit.MINUTES.sleep(1);
                log.info("Query teardown failed on attempt #%s, will sleep and retry", attempt);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            attempt++;
        }
        while (attempt < teardownRetries);
        return tearDown(query, postQueryResults, postQuery ->
                executeQuery(gateway, username, password, query, postQuery, timeout, sessionProperties));
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
        ExecutorService executor = newSingleThreadExecutor();
        TimeLimiter limiter = SimpleTimeLimiter.create(executor);

        String queryId = null;
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            trySetConnectionProperties(query, connection);
            for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
                connection.unwrap(PrestoConnection.class).setSessionProperty(entry.getKey(), entry.getValue());
            }

            try (Statement statement = connection.createStatement()) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                Statement limitedStatement = limiter.newProxy(statement, Statement.class, timeout.toMillis(), MILLISECONDS);
                if (explainOnly) {
                    sql = "EXPLAIN " + sql;
                }

                long start = System.nanoTime();
                PrestoStatement prestoStatement = limitedStatement.unwrap(PrestoStatement.class);
                ProgressMonitor progressMonitor = new ProgressMonitor();
                prestoStatement.setProgressMonitor(progressMonitor);
                boolean isSelectQuery = limitedStatement.execute(sql);

                List<List<Object>> results;
                if (isSelectQuery) {
                    ResultSetConverter converter = limiter.newProxy(
                            this::convertJdbcResultSet,
                            ResultSetConverter.class,
                            timeout.toMillis() - stopwatch.elapsed(MILLISECONDS),
                            MILLISECONDS);
                    results = converter.convert(limitedStatement.getResultSet());
                }
                else {
                    results = ImmutableList.of(ImmutableList.of(limitedStatement.getLargeUpdateCount()));
                }

                prestoStatement.clearProgressMonitor();
                QueryStats queryStats = progressMonitor.getFinalQueryStats();
                if (queryStats == null) {
                    throw new VerifierException("Cannot fetch query stats");
                }
                Duration queryCpuTime = new Duration(queryStats.getCpuTimeMillis(), MILLISECONDS);
                queryId = queryStats.getQueryId();
                return new QueryResult(State.SUCCESS, null, nanosSince(start), queryCpuTime, queryId, results);
            }
        }
        catch (SQLException e) {
            Exception exception = e;
            if (("Error executing query".equals(e.getMessage()) || "Error fetching results".equals(e.getMessage())) &&
                    (e.getCause() instanceof Exception)) {
                exception = (Exception) e.getCause();
            }
            State state = isPrestoQueryInvalid(e) ? State.INVALID : State.FAILED;
            return new QueryResult(state, exception, null, null, queryId, ImmutableList.of());
        }
        catch (VerifierException e) {
            return new QueryResult(State.TOO_MANY_ROWS, e, null, null, queryId, ImmutableList.of());
        }
        catch (UncheckedTimeoutException e) {
            return new QueryResult(State.TIMEOUT, e, null, null, queryId, ImmutableList.of());
        }
        finally {
            executor.shutdownNow();
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

    //adapted from http://floating-point-gui.de/errors/comparison/
    private static boolean isClose(double a, double b, double epsilon)
    {
        double absA = Math.abs(a);
        double absB = Math.abs(b);
        double diff = Math.abs(a - b);

        if (!isFinite(a) || !isFinite(b)) {
            return Double.compare(a, b) == 0;
        }

        // a or b is zero or both are extremely close to it
        // relative error is less meaningful here
        if (a == 0 || b == 0 || diff < Float.MIN_NORMAL) {
            return diff < (epsilon * Float.MIN_NORMAL);
        }
        else {
            // use relative error
            return diff / Math.min((absA + absB), Float.MAX_VALUE) < epsilon;
        }
    }

    @VisibleForTesting
    static int precisionCompare(double a, double b, int precision)
    {
        //we don't care whether a is smaller than b or not when they are not close since we will fail verification anyway
        return isClose(a, b, Math.pow(10, -1 * (precision - 1))) ? 0 : -1;
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
        private boolean finished;

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

    public interface ResultSetConverter
    {
        List<List<Object>> convert(ResultSet resultSet)
                throws SQLException, VerifierException;
    }
}
