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

import static com.facebook.presto.common.predicate.Range.equal;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThan;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
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
