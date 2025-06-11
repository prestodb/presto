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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.plugin.arrow.testingServer.TestingArrowProducer;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightQueries
        extends AbstractTestQueries
{
    private static final Logger logger = Logger.get(TestArrowFlightQueries.class);
    private final int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;

    public TestArrowFlightQueries()
            throws IOException
    {
        this.serverPort = ArrowFlightQueryRunner.findUnusedPort();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        File certChainFile = new File("src/test/resources/server.crt");
        File privateKeyFile = new File("src/test/resources/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = Location.forGrpcTls("localhost", serverPort);
        server = FlightServer.builder(allocator, location, new TestingArrowProducer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port %s", server.getPort());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        arrowFlightQueryRunner.close();
        server.close();
        allocator.close();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ArrowFlightQueryRunner.createQueryRunner(serverPort);
    }

    @Test
    public void testShowCharColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM member");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("id", "integer", "", "", 10L, null, null)
                .row("name", "varchar", "", "", null, null, 2147483647L)
                .row("sex", "char", "", "", null, null, 2147483647L)
                .row("state", "char", "", "", null, null, 2147483647L)
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("id", "integer", "", "", 10L, null, null)
                .row("name", "varchar(50)", "", "", null, null, 50L)
                .row("sex", "char(1)", "", "", null, null, 1L)
                .row("state", "char(5)", "", "", null, null, 5L)
                .build();

        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s matches neither %s nor %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testPredicateOnCharColumn()
    {
        MaterializedResult actualRow = computeActual("SELECT * from member WHERE state = 'CD'");
        MaterializedResult expectedRow = resultBuilder(getSession(), INTEGER, createVarcharType(50), createCharType(1), createCharType(5))
                .row(2, "MARY", "F", "CD   ")
                .build();
        assertTrue(actualRow.equals(expectedRow));
    }

    @Test
    public void testSelectTime()
    {
        MaterializedResult actualRow = computeActual("SELECT * from event WHERE id = 1");
        Session session = getSession();
        MaterializedResult expectedRow = resultBuilder(session, INTEGER, DATE, TIME, TIMESTAMP)
                .row(1,
                        getDate("2004-12-31"),
                        getTimeAtZone("23:59:59", session.getTimeZoneKey()),
                        getDateTimeAtZone("2005-12-31 23:59:59", session.getTimeZoneKey()))
                .build();
        assertTrue(actualRow.equals(expectedRow));
    }

    @Test
    public void testSystemJdbcColumns()
    {
        MaterializedResult actualRow = computeActual("SELECT * from system.jdbc.columns");
        assertTrue(actualRow.getRowCount() > 0);
    }

    @Test
    public void testSystemJdbcTables()
    {
        MaterializedResult actualRow = computeActual("SELECT * from system.jdbc.tables");
        assertTrue(actualRow.getRowCount() > 0);
    }

    @Test
    public void testDescribeUnknownTable()
    {
        MaterializedResult actualRows = computeActual("DESCRIBE information_schema.enabled_roles");
        MaterializedResult expectedRows = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("role_name", "varchar", "", "", null, null, 2147483647L)
                .build();

        assertEquals(actualRows, expectedRows);
    }

    private LocalDate getDate(String dateString)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse(dateString, formatter);

        return localDate;
    }

    private LocalTime getTimeAtZone(String timeString, TimeZoneKey timeZoneKey)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalTime localTime = LocalTime.parse(timeString, formatter);

        LocalDateTime localDateTime = LocalDateTime.of(LocalDate.of(1970, 1, 1), localTime);
        ZonedDateTime localZonedDateTime = localDateTime.atZone(ZoneId.systemDefault());

        ZoneId zoneId = ZoneId.of(timeZoneKey.getId());
        ZonedDateTime zonedDateTime = localZonedDateTime.withZoneSameInstant(zoneId);

        LocalTime localTimeAtZone = zonedDateTime.toLocalTime();
        return localTimeAtZone;
    }

    private LocalDateTime getDateTimeAtZone(String dateTimeString, TimeZoneKey timeZoneKey)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);

        ZonedDateTime localZonedDateTime = localDateTime.atZone(ZoneId.systemDefault());

        ZoneId zoneId = ZoneId.of(timeZoneKey.getId());
        ZonedDateTime zonedDateTime = localZonedDateTime.withZoneSameInstant(zoneId);

        LocalDateTime localDateTimeAtZone = zonedDateTime.toLocalDateTime();
        return localDateTimeAtZone;
    }
}
