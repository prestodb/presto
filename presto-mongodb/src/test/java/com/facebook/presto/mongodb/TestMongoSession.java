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
package com.facebook.presto.mongodb;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.bson.Document;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.predicate.Range.equal;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThan;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestMongoSession
{
    private static final MongoColumnHandle COL1 = new MongoColumnHandle("col1", BIGINT, false);
    private static final MongoColumnHandle COL2 = new MongoColumnHandle("col2", createUnboundedVarcharType(), false);
    private static final MongoColumnHandle COL3 = new MongoColumnHandle("col3", VARBINARY, false);
    private static final MongoColumnHandle COL4 = new MongoColumnHandle("col4", DATE, false);
    private static final MongoColumnHandle COL5 = new MongoColumnHandle("col5", TIMESTAMP, false);
    private static final MongoColumnHandle COL6 = new MongoColumnHandle("col6", TIME, false);
    private static final MongoColumnHandle COL7 = new MongoColumnHandle("col7", TIMESTAMP_WITH_TIME_ZONE, false);

    @Test
    public void testBuildQuery()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
                COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getName(), new Document().append("$gt", 100L).append("$lte", 200L))
                .append(COL2.getName(), new Document("$eq", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryBinaryType()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL3, Domain.singleValue(VARBINARY, wrappedBuffer("VarBinary Value".getBytes(UTF_8)))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL3.getName(), new Document().append("$eq", "VarBinary Value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryDateType()
    {
        long days = LocalDate.of(2024, 1, 15).toEpochDay();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL4, Domain.singleValue(DATE, days)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL4.getName(), new Document("$eq", new Date(TimeUnit.DAYS.toMillis(days))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryDateRange()
    {
        long low = LocalDate.of(2024, 1, 15).toEpochDay();
        long high = LocalDate.of(2024, 1, 25).toEpochDay();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL4, Domain.create(ValueSet.ofRanges(range(DATE, low, true, high, false)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL4.getName(), new Document()
                        .append("$gte", new Date(TimeUnit.DAYS.toMillis(low)))
                        .append("$lt", new Date(TimeUnit.DAYS.toMillis(high))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryTimestampType()
    {
        long millis = 1_705_312_953_456L;
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL5, Domain.create(ValueSet.ofRanges(greaterThan(TIMESTAMP, millis)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL5.getName(), new Document("$gt", new Date(millis)));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryTimeType()
    {
        long millisOfDay = TimeUnit.HOURS.toMillis(11) + TimeUnit.MINUTES.toMillis(22);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL6, Domain.singleValue(TIME, millisOfDay)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL6.getName(), new Document("$eq", new Date(millisOfDay)));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryTimestampWithTimeZoneType()
    {
        long millis = 1_705_312_953_456L;
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL7, Domain.singleValue(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(millis, UTC_KEY))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL7.getName(), new Document("$eq", new Date(millis)));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryStringType()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("hello"), false, utf8Slice("world"), true)), false),
                COL2, Domain.create(ValueSet.ofRanges(greaterThanOrEqual(createUnboundedVarcharType(), utf8Slice("a value"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getName(), new Document().append("$gt", "hello").append("$lte", "world"))
                .append(COL2.getName(), new Document("$gte", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryIn()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL2, Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")), equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document(COL2.getName(), new Document("$in", ImmutableList.of("hello", "world")));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryOr()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$lt", 100L)),
                new Document(COL1.getName(), new Document("$gt", 200L))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryNull()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$gt", 200L)),
                new Document(COL1.getName(), new Document("$exists", true).append("$eq", null))));
        assertEquals(query, expected);
    }
}
