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

import com.facebook.presto.verifier.Validator.ChangedRow.Changed;
import com.google.common.base.Function;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.verifier.QueryResult.State;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.primitives.Doubles.isFinite;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

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
    private final boolean verboseResultsComparison;
    private final QueryPair queryPair;

    private Boolean valid;

    private QueryResult controlResult;
    private QueryResult testResult;

    private boolean deterministic = true;

    public Validator(VerifierConfig config, QueryPair queryPair)
    {
        checkNotNull(config, "config is null");
        this.testUsername = checkNotNull(config.getTestUsername(), "test username is null");
        this.controlUsername = checkNotNull(config.getControlUsername(), "control username is null");
        this.testPassword = config.getTestPassword();
        this.controlPassword = config.getControlPassword();
        this.controlGateway = checkNotNull(config.getControlGateway(), "controlGateway is null");
        this.testGateway = checkNotNull(config.getTestGateway(), "testGateway is null");
        this.controlTimeout = config.getControlTimeout();
        this.testTimeout = config.getTestTimeout();
        this.maxRowCount = config.getMaxRowCount();
        this.checkCorrectness = config.isCheckCorrectnessEnabled();
        this.verboseResultsComparison = config.isVerboseResultsComparison();

        this.queryPair = checkNotNull(queryPair, "queryPair is null");
    }

    public boolean isSkipped()
    {
        if (!checkCorrectness) {
            return false;
        }

        if (queryPair.getControl().getQuery().isEmpty() || queryPair.getTest().getQuery().isEmpty()) {
            return true;
        }

        if (getControlResult().getState() != State.SUCCESS) {
            return true;
        }

        if (!isDeterministic()) {
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
        if (!checkCorrectness) {
            testResult = executeQuery(testGateway, testUsername, testPassword, queryPair.getTest(), testTimeout);
            controlResult = testResult;

            return testResult.getState() == State.SUCCESS;
        }

        controlResult = executeQuery(controlGateway, controlUsername, controlPassword, queryPair.getControl(), controlTimeout);

        // query has too many rows. Consider blacklisting.
        if (controlResult.getState() == State.TOO_MANY_ROWS) {
            return false;
        }
        // query failed in the control
        else if (controlResult.getState() != State.SUCCESS) {
            return true;
        }

        testResult = executeQuery(testGateway, testUsername, testPassword, queryPair.getTest(), testTimeout);

        if ((controlResult.getState() != State.SUCCESS) || (testResult.getState() != State.SUCCESS)) {
            return false;
        }

        if (resultsMatch(controlResult, testResult)) {
            return true;
        }

        // check if the control query is deterministic
        for (int i = 0; i < 3; i++) {
            QueryResult results = executeQuery(controlGateway, controlUsername, controlPassword, queryPair.getControl(), controlTimeout);
            if (results.getState() != State.SUCCESS) {
                return false;
            }

            if (!resultsMatch(controlResult, results)) {
                deterministic = false;
                return false;
            }
        }

        // query is deterministic and result don't match
        return false;
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

    private QueryResult executeQuery(String url, String username, String password, Query query, Duration timeout)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            connection.setClientInfo("ApplicationName", "verifier-test-" + queryPair.getName());
            connection.setCatalog(query.getCatalog());
            connection.setSchema(query.getSchema());
            long start = System.nanoTime();

            try (Statement statement = connection.createStatement()) {
                TimeLimiter limiter = new SimpleTimeLimiter();
                Stopwatch stopwatch = Stopwatch.createStarted();
                Statement limitedStatement = limiter.newProxy(statement, Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
                try (final ResultSet resultSet = limitedStatement.executeQuery(query.getQuery())) {
                    List<List<Object>> results = limiter.callWithTimeout(getResultSetConverter(resultSet), timeout.toMillis() - stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, true);
                    return new QueryResult(State.SUCCESS, null, nanosSince(start), results);
                }
                catch (AssertionError e) {
                    if (e.getMessage().startsWith("unimplemented type:")) {
                        return new QueryResult(State.INVALID, null, null, ImmutableList.<List<Object>>of());
                    }
                    throw e;
                }
                catch (SQLException | VerifierException e) {
                    throw e;
                }
                catch (UncheckedTimeoutException e) {
                    throw new SQLTimeoutException(e);
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
            return new QueryResult(state, exception, null, null);
        }
        catch (VerifierException e) {
            return new QueryResult(State.TOO_MANY_ROWS, e, null, null);
        }
    }

    private Callable<List<List<Object>>> getResultSetConverter(final ResultSet resultSet)
    {
        return new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call()
                    throws Exception
            {
                return convertJdbcResultSet(resultSet);
            }
        };
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

    private static boolean resultsMatch(QueryResult controlResult, QueryResult testResult)
    {
        SortedMultiset<List<Object>> control = ImmutableSortedMultiset.copyOf(rowComparator(), controlResult.getResults());
        SortedMultiset<List<Object>> test = ImmutableSortedMultiset.copyOf(rowComparator(), testResult.getResults());
        try {
            return control.equals(test);
        }
        catch (TypesDoNotMatchException e) {
            return false;
        }
    }

    public String getResultsComparison()
    {
        List<List<Object>> controlResults = controlResult.getResults();
        List<List<Object>> testResults = testResult.getResults();

        if (valid() || (controlResults == null) || (testResults == null)) {
            return "";
        }

        Multiset<List<Object>> control = ImmutableSortedMultiset.copyOf(rowComparator(), controlResults);
        Multiset<List<Object>> test = ImmutableSortedMultiset.copyOf(rowComparator(), testResults);

        try {
            Iterable<ChangedRow> diff = ImmutableSortedMultiset.<ChangedRow>naturalOrder()
                    .addAll(Iterables.transform(Multisets.difference(control, test), ChangedRow.changedRows(Changed.REMOVED)))
                    .addAll(Iterables.transform(Multisets.difference(test, control), ChangedRow.changedRows(Changed.ADDED)))
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

    private static Comparator<List<Object>> rowComparator()
    {
        final Comparator<Object> comparator = Ordering.from(columnComparator()).nullsFirst();
        return new Comparator<List<Object>>()
        {
            @Override
            public int compare(List<Object> a, List<Object> b)
            {
                checkArgument(a.size() == b.size(), "list sizes do not match");
                for (int i = 0; i < a.size(); i++) {
                    int r = comparator.compare(a.get(i), b.get(i));
                    if (r != 0) {
                        return r;
                    }
                }
                return 0;
            }
        };
    }

    private static Comparator<Object> columnComparator()
    {
        return new Comparator<Object>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public int compare(Object a, Object b)
            {
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
                    return precisionCompare(x.doubleValue(), y.doubleValue());
                }
                if (a.getClass() != b.getClass()) {
                    throw new TypesDoNotMatchException(format("item types do not match: %s vs %s", a.getClass().getName(), b.getClass().getName()));
                }
                checkArgument(a instanceof Comparable, "item is not Comparable: %s", a.getClass().getName());
                return ((Comparable<Object>) a).compareTo(b);
            }
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

    private static int precisionCompare(double a, double b)
    {
        if (!isFinite(a) || !isFinite(b)) {
            return Double.compare(a, b);
        }

        MathContext context = new MathContext(5);
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

        public static Function<List<Object>, ChangedRow> changedRows(final Changed changed)
        {
            return new Function<List<Object>, ChangedRow>()
            {
                @Override
                public ChangedRow apply(List<Object> row)
                {
                    return new ChangedRow(changed, row);
                }
            };
        }

        private final Changed changed;
        private final List<Object> row;

        private ChangedRow(Changed changed, List<Object> row)
        {
            this.changed = changed;
            this.row = row;
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
                    .compare(this.row, that.row, rowComparator())
                    .compareFalseFirst(this.changed == Changed.ADDED, that.changed == Changed.ADDED)
                    .result();
        }
    }
}
