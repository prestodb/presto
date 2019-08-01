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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestGlueExpressionUtil
{
    private static final List<Column> PARTITION_KEYS = ImmutableList.of(
            getColumn("name", "string"),
            getColumn("birthday", "date"),
            getColumn("age", "int"));

    private static Column getColumn(String name, String type)
    {
        return new Column(name, HiveType.valueOf(type), Optional.empty());
    }

    @Test
    public void testBuildExpression()
    {
        List<String> partitionValues = ImmutableList.of("foo", "2018-01-02", "99");
        String expression = buildGlueExpression(PARTITION_KEYS, partitionValues);
        assertEquals(expression, "(name='foo') AND (birthday='2018-01-02') AND (age=99)");

        partitionValues = ImmutableList.of("foo", "2018-01-02", "");
        expression = buildGlueExpression(PARTITION_KEYS, partitionValues);
        assertEquals(expression, "(name='foo') AND (birthday='2018-01-02')");
    }

    @Test
    public void testBuildExpressionFromPartialSpecification()
    {
        List<String> partitionValues = ImmutableList.of("", "2018-01-02", "");
        String expression = buildGlueExpression(PARTITION_KEYS, partitionValues);
        assertEquals(expression, "(birthday='2018-01-02')");

        partitionValues = ImmutableList.of("foo", "", "99");
        expression = buildGlueExpression(PARTITION_KEYS, partitionValues);
        assertEquals(expression, "(name='foo') AND (age=99)");
    }

    @Test
    public void testBuildExpressionNullOrEmptyValues()
    {
        assertNull(buildGlueExpression(PARTITION_KEYS, ImmutableList.of()));
        assertNull(buildGlueExpression(PARTITION_KEYS, null));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testBuildExpressionInvalidPartitionValueListSize()
    {
        List<String> partitionValues = ImmutableList.of("foo", "2017-01-02", "99", "extra");
        buildGlueExpression(PARTITION_KEYS, partitionValues);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testBuildExpressionNullPartitionKeys()
    {
        List<String> partitionValues = ImmutableList.of("foo", "2018-01-02", "99");
        buildGlueExpression(null, partitionValues);
    }
}
