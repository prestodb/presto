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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.type.TypeRegistry;
import io.prestosql.util.DateTimeUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveTestUtils.longDecimal;
import static io.prestosql.plugin.hive.HiveTestUtils.shortDecimal;
import static io.prestosql.plugin.hive.HiveType.HIVE_DATE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.prestosql.spi.predicate.TupleDomain.withColumnDomains;
import static io.prestosql.spi.predicate.ValueSet.ofRanges;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestIonSqlQueryBuilder
{
    @Test
    public void testBuildSQL()
    {
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(new TypeRegistry());
        List<HiveColumnHandle> columns = ImmutableList.of(
                new HiveColumnHandle("n_nationkey", HIVE_INT, parseTypeSignature(INTEGER), 0, REGULAR, Optional.empty()),
                new HiveColumnHandle("n_name", HIVE_STRING, parseTypeSignature(VARCHAR), 1, REGULAR, Optional.empty()),
                new HiveColumnHandle("n_regionkey", HIVE_INT, parseTypeSignature(INTEGER), 2, REGULAR, Optional.empty()));

        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s",
                queryBuilder.buildSql(columns, TupleDomain.all()));
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                columns.get(2), Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s WHERE (case s._3 when '' then null else CAST(s._3 AS INT) end = 3)",
                queryBuilder.buildSql(columns, tupleDomain));
    }

    @Test
    public void testEmptyColumns()
    {
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(new TypeRegistry());
        assertEquals("SELECT ' ' FROM S3Object s", queryBuilder.buildSql(ImmutableList.of(), TupleDomain.all()));
    }

    @Test
    public void testDecimalColumns()
    {
        TypeManager typeManager = new TypeRegistry();
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager);
        List<HiveColumnHandle> columns = ImmutableList.of(
                new HiveColumnHandle("quantity", HiveType.valueOf("decimal(20,0)"), parseTypeSignature(DECIMAL), 0, REGULAR, Optional.empty()),
                new HiveColumnHandle("extendedprice", HiveType.valueOf("decimal(20,2)"), parseTypeSignature(DECIMAL), 1, REGULAR, Optional.empty()),
                new HiveColumnHandle("discount", HiveType.valueOf("decimal(10,2)"), parseTypeSignature(DECIMAL), 2, REGULAR, Optional.empty()));
        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(
                ImmutableMap.of(
                        columns.get(0), Domain.create(ofRanges(Range.lessThan(DecimalType.createDecimalType(20, 0), longDecimal("50"))), false),
                        columns.get(1), Domain.create(ofRanges(Range.equal(HiveType.valueOf("decimal(20,2)").getType(typeManager), longDecimal("0.05"))), false),
                        columns.get(2), Domain.create(ofRanges(Range.range(decimalType, shortDecimal("0.0"), true, shortDecimal("0.02"), true)), false)));
        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s WHERE ((case s._1 when '' then null else CAST(s._1 AS DECIMAL(20,0)) end < 50)) AND " +
                        "(case s._2 when '' then null else CAST(s._2 AS DECIMAL(20,2)) end = 0.05) AND ((case s._3 when '' then null else CAST(s._3 AS DECIMAL(10,2)) " +
                        "end >= 0.00 AND case s._3 when '' then null else CAST(s._3 AS DECIMAL(10,2)) end <= 0.02))",
                queryBuilder.buildSql(columns, tupleDomain));
    }

    @Test
    public void testDateColumn()
    {
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(new TypeRegistry());
        List<HiveColumnHandle> columns = ImmutableList.of(
                new HiveColumnHandle("t1", HIVE_TIMESTAMP, parseTypeSignature(TIMESTAMP), 0, REGULAR, Optional.empty()),
                new HiveColumnHandle("t2", HIVE_DATE, parseTypeSignature(StandardTypes.DATE), 1, REGULAR, Optional.empty()));
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                columns.get(1), Domain.create(SortedRangeSet.copyOf(DATE, ImmutableList.of(Range.equal(DATE, (long) DateTimeUtils.parseDate("2001-08-22")))), false)));

        assertEquals("SELECT s._1, s._2 FROM S3Object s WHERE (case s._2 when '' then null else CAST(s._2 AS TIMESTAMP) end = `2001-08-22`)", queryBuilder.buildSql(columns, tupleDomain));
    }

    @Test
    public void testNotPushDoublePredicates()
    {
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(new TypeRegistry());
        List<HiveColumnHandle> columns = ImmutableList.of(
                new HiveColumnHandle("quantity", HIVE_INT, parseTypeSignature(INTEGER), 0, REGULAR, Optional.empty()),
                new HiveColumnHandle("extendedprice", HIVE_DOUBLE, parseTypeSignature(StandardTypes.DOUBLE), 1, REGULAR, Optional.empty()),
                new HiveColumnHandle("discount", HIVE_DOUBLE, parseTypeSignature(StandardTypes.DOUBLE), 2, REGULAR, Optional.empty()));
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(
                ImmutableMap.of(
                        columns.get(0), Domain.create(ofRanges(Range.lessThan(BIGINT, 50L)), false),
                        columns.get(1), Domain.create(ofRanges(Range.equal(DOUBLE, 0.05)), false),
                        columns.get(2), Domain.create(ofRanges(Range.range(DOUBLE, 0.0, true, 0.02, true)), false)));
        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s WHERE ((case s._1 when '' then null else CAST(s._1 AS INT) end < 50))",
                queryBuilder.buildSql(columns, tupleDomain));
    }
}
