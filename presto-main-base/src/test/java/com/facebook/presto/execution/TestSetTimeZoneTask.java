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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetTimeZone;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.TIME_ZONE_ID;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static com.facebook.presto.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static com.facebook.presto.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static com.facebook.presto.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSetTimeZoneTask
{
    private LocalQueryRunner localQueryRunner;
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        localQueryRunner = new LocalQueryRunner(TEST_SESSION);
        // Executor is required for QueryStateMachine initialization
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (localQueryRunner != null) {
            localQueryRunner.close();
            localQueryRunner = null;
        }
    }

    @Test
    public void testSetTimeZoneLocal()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE LOCAL");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.empty());
        executeSetTimeZone(setTimeZone, stateMachine);

        // LOCAL should set the timezone to JVM default
        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertTrue(setSessionProperties.containsKey(TIME_ZONE_ID));
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), TimeZone.getDefault().getID());
    }

    @Test
    public void testSetTimeZoneStringLiteral()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE 'America/Los_Angeles'");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new StringLiteral("America/Los_Angeles")));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "America/Los_Angeles");
    }

    @Test
    public void testSetTimeZoneVarcharFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE concat('America', '/', 'Los_Angeles')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "concat", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 22),
                                        "America"),
                                new StringLiteral(
                                        new NodeLocation(1, 33),
                                        "/"),
                                new StringLiteral(
                                        new NodeLocation(1, 38),
                                        "Los_Angeles")))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "America/Los_Angeles");
    }

    @Test
    public void testSetTimeZoneInvalidFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE e()");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "e", false))),
                        ImmutableList.of())));

        try {
            executeSetTimeZone(setTimeZone, stateMachine);
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            // Expected - e() returns double, not varchar or interval
        }
    }

    @Test
    public void testSetTimeZoneStringLiteralInvalidZoneId()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE 'Matrix/Zion'");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new StringLiteral("Matrix/Zion")));

        try {
            executeSetTimeZone(setTimeZone, stateMachine);
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            // Expected - invalid time zone wrapped in PrestoException
            assertEquals(e.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
        }
    }

    @Test
    public void testSetTimeZoneIntervalLiteral()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL '10' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("10", POSITIVE, HOUR)));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "+10:00");
    }

    @Test
    public void testSetTimeZoneIntervalDayTimeTypeFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE parse_duration('8h')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "parse_duration", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 30),
                                        "8h")))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "+08:00");
    }

    @Test
    public void testSetTimeZoneIntervalDayTimeTypeInvalidFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE parse_duration('3601s')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "parse_duration", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 30),
                                        "3601s")))));

        try {
            executeSetTimeZone(setTimeZone, stateMachine);
            fail("Expected PrestoException for interval with seconds");
        }
        catch (PrestoException e) {
            // Expected - interval contains seconds which is invalid
        }
    }
    @Test
    public void testSetTimeZoneIntervalDayTimeTypeMillisecondsThreshold()
    {
        // Test that values > 100000 are treated as milliseconds and converted to minutes
        // parse_duration('180m') returns 10800000 milliseconds (180 minutes = 3 hours)
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE parse_duration('180m')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "parse_duration", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 30),
                                        "180m")))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "+03:00");
    }

    @Test
    public void testSetTimeZoneLocalResetsToDefault()
    {
        // First, change the timezone to something different
        QueryStateMachine stateMachine1 = createQueryStateMachine("SET TIME ZONE 'America/Los_Angeles'");
        SetTimeZone setTimeZone1 = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new StringLiteral("America/Los_Angeles")));
        executeSetTimeZone(setTimeZone1, stateMachine1);

        Map<String, String> setSessionProperties1 = stateMachine1.getSetSessionProperties();
        assertEquals(setSessionProperties1.size(), 1);
        assertEquals(setSessionProperties1.get(TIME_ZONE_ID), "America/Los_Angeles");

        // Now execute SET TIME ZONE LOCAL - it should set to JVM default timezone
        QueryStateMachine stateMachine2 = createQueryStateMachine("SET TIME ZONE LOCAL");
        SetTimeZone setTimeZone2 = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.empty());
        executeSetTimeZone(setTimeZone2, stateMachine2);

        // Verify that LOCAL sets the timezone to JVM default (not reset)
        Map<String, String> setSessionProperties2 = stateMachine2.getSetSessionProperties();
        assertEquals(setSessionProperties2.size(), 1);
        assertTrue(setSessionProperties2.containsKey(TIME_ZONE_ID));
        assertEquals(setSessionProperties2.get(TIME_ZONE_ID), TimeZone.getDefault().getID());
    }

    @Test
    public void testSetTimeZoneIntervalLiteralGreaterThanOffsetTimeZoneMax()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL '15' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("15", POSITIVE, HOUR)));

        try {
            executeSetTimeZone(setTimeZone, stateMachine);
            fail("Expected PrestoException for offset > +14:00");
        }
        catch (PrestoException e) {
            // Expected - offset too large
        }
    }

    @Test
    public void testSetTimeZoneIntervalLiteralLessThanOffsetTimeZoneMin()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL -'15' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("15", NEGATIVE, HOUR)));

        try {
            executeSetTimeZone(setTimeZone, stateMachine);
            fail("Expected PrestoException for offset < -14:00");
        }
        catch (PrestoException e) {
            // Expected - offset too small
        }
    }

    @Test
    public void testSetTimeIntervalLiteralZoneHourToMinute()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL -'08:00' HOUR TO MINUTE");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("8", NEGATIVE, HOUR, Optional.of(MINUTE))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(setSessionProperties.size(), 1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "-08:00");
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                query,
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                Optional.empty(),
                false,
                localQueryRunner.getTransactionManager(),
                localQueryRunner.getAccessControl(),
                executor,
                localQueryRunner.getMetadata(),
                WarningCollector.NOOP);
    }

    private void executeSetTimeZone(SetTimeZone setTimeZone, QueryStateMachine stateMachine)
    {
        Session session = stateMachine.getSession();
        SetTimeZoneTask task = new SetTimeZoneTask();
        getFutureValue(task.execute(
                setTimeZone,
                localQueryRunner.getTransactionManager(),
                localQueryRunner.getMetadata(),
                localQueryRunner.getAccessControl(),
                stateMachine,
                emptyList(),
                "SET TIME ZONE"));
    }
}
