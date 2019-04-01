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
package com.facebook.presto.connector.thrift.api.expression;

import com.facebook.presto.connector.thrift.api.PrestoThriftDomain;
import com.facebook.presto.connector.thrift.api.valuesets.PrestoThriftValueSet;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SpiUnaryExpression;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.HyperLogLogType;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.type.JsonType.JSON;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

public class TestPrestoThriftUnaryExpression
{
    private static final String JSON1 = "\"key1\":\"value1\"";
    private static final String JSON2 = "\"key2\":\"value2\"";

    @Test
    public void testToUnaryExpressionAll()
    {
        assertEquivalence(ValueSet.all(BIGINT));
    }

    @Test
    public void testToUnaryExpressionNone()
    {
        assertEquivalence(ValueSet.none(BIGINT));
    }

    @Test
    public void testToUnaryExpressionOf()
    {
        assertEquivalence(ValueSet.of(BIGINT, 1L, 2L, 3L));
    }

    @Test
    public void testToUnaryExpressionOfRangesUnbounded()
    {
        assertEquivalence(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 0L)));
    }

    @Test
    public void testToSpiExpressionOfRangesBounded()
    {
        assertEquivalence(ValueSet.ofRanges(
                Range.range(BIGINT, -10L, true, -1L, false),
                Range.range(BIGINT, -1L, false, 100L, true)));
    }

    @Test
    public void testToUnaryExpressionAllHLL()
    {
        PrestoThriftValueSet thriftValueSet = PrestoThriftValueSet.fromValueSet(ValueSet.all(HyperLogLogType.HYPER_LOG_LOG));
        PrestoThriftDomain domain = new PrestoThriftDomain(thriftValueSet, false);
        PrestoThriftUnaryExpression unaryExpression = new PrestoThriftUnaryExpression("col1", HyperLogLogType.HYPER_LOG_LOG.getTypeSignature().getBase(), domain);
        assertThrows(IllegalArgumentException.class, () -> PrestoThriftUnaryExpression.toSpiUnaryExpression(unaryExpression));
    }

    @Test
    public void testToUnaryExpressionEquatableValueSet()
    {
        PrestoThriftValueSet thriftValueSet = PrestoThriftValueSet.fromValueSet(ValueSet.of(JSON, utf8Slice(JSON1), utf8Slice(JSON2)));
        PrestoThriftDomain domain = new PrestoThriftDomain(thriftValueSet, false);
        PrestoThriftUnaryExpression unaryExpression = new PrestoThriftUnaryExpression("col1", JSON.getTypeSignature().getBase(), domain);
        assertThrows(IllegalArgumentException.class, () -> PrestoThriftUnaryExpression.toSpiUnaryExpression(unaryExpression));
    }

    private static void assertEquivalence(ValueSet valueSet)
    {
        PrestoThriftValueSet thriftValueSet = PrestoThriftValueSet.fromValueSet(valueSet);
        PrestoThriftDomain domain = new PrestoThriftDomain(thriftValueSet, false);
        PrestoThriftUnaryExpression unaryExpression = new PrestoThriftUnaryExpression("col1", BIGINT.getTypeSignature().getBase(), domain);
        SpiUnaryExpression spiUnaryExpression = PrestoThriftUnaryExpression.toSpiUnaryExpression(unaryExpression);
        assertNotNull(spiUnaryExpression.getDomain());
        assertNotNull(spiUnaryExpression.getDomain().getValues());
        assertEquals(spiUnaryExpression.getDomain().getValues(), valueSet);
    }
}
