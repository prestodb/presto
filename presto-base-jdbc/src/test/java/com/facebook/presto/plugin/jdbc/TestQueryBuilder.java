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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.List;

import static com.facebook.presto.spi.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestQueryBuilder
{
    static final List<JdbcColumnHandle> COLUMN_HANDLES = copyOf(newArrayList(
            new JdbcColumnHandle("testConnector", "name", VARCHAR),
            new JdbcColumnHandle("testConnector", "loan-number", BIGINT),
            new JdbcColumnHandle("testConnector", "lease", BOOLEAN),
            new JdbcColumnHandle("testConnector", "rate", DOUBLE)
    ));

    @Test
    public void testQueryBuilderWhere()
    {
        TupleDomain<ColumnHandle> tupleDomain = withColumnDomains(of(
                COLUMN_HANDLES.get(0), Domain.singleValue(utf8Slice("Homer Simpson")),
                COLUMN_HANDLES.get(2), Domain.singleValue(false),
                COLUMN_HANDLES.get(3), Domain.create(SortedRangeSet.of(Range.greaterThan(10_000.0)), false)
        ));

        QueryBuilder queryBuilder = new QueryBuilder("\"");
        String sql = queryBuilder.buildSql("bank", "loan", "car-loan", COLUMN_HANDLES, tupleDomain);

        assertEquals(extractWhereClause(sql), "((\"lease\" = false) AND (\"rate\" > 10000.0))");
    }

    @Test
    public void testExtendedQueryBuilderWhere()
    {
        TupleDomain<ColumnHandle> tupleDomain = withColumnDomains(of(
                COLUMN_HANDLES.get(0), Domain.singleValue(utf8Slice("Marge Simpson")),
                COLUMN_HANDLES.get(2), Domain.singleValue(true),
                COLUMN_HANDLES.get(3), Domain.create(SortedRangeSet.of(Range.greaterThan(45_003.08)), false)
        ));

        QueryBuilder qbWithVarcharSupport = new QueryBuilder("\"")
        {
            @Override
            protected boolean supportPredicateOnType(Type type)
            {
                return super.supportPredicateOnType(type) || type.equals(VarcharType.VARCHAR);
            }

            @Override
            protected String encode(Type type, Object value)
            {
                if (type.equals(VarcharType.VARCHAR)) {
                    Slice slice = (Slice) value;
                    CharBuffer cb = UTF_8.decode(slice.toByteBuffer());
                    //Escape this string to prevent SQL injection.
                    return "'" + cb.toString() + "'";
                }

                return super.encode(type, value);
            }
        };
        String sql = qbWithVarcharSupport.buildSql("bank", "loan", "car-loan", COLUMN_HANDLES, tupleDomain);

        assertEquals(
                extractWhereClause(sql),
                "(((\"name\" = 'Marge Simpson') AND (\"lease\" = true)) AND (\"rate\" > 45003.08))");
    }

    static String extractWhereClause(String sql)
    {
        final String extractedWhereHolder[] = new String[1];

        //Use the Presto parser itself and extract the WHERE clause as a string.
        SqlParser sqlParser = new SqlParser();
        sqlParser.createStatement(sql).accept(new DefaultTraversalVisitor<Object, Object>()
        {
            @Override
            protected Object visitQuery(Query node, Object context)
            {
                extractedWhereHolder[0] = ((QuerySpecification) node.getQueryBody())
                        .getWhere().get().toString();
                return super.visitQuery(node, context);
            }
        }, new HashMap<>());

        return extractedWhereHolder[0];
    }
}
