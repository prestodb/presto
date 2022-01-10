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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.parquet.AbstractTestParquetReader.intsBetween;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.transform;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;

public class AbstractTestParquetSelectiveReader
        extends AbstractTestParquetReader
{
    public AbstractTestParquetSelectiveReader(ParquetTester tester)
    {
        super(tester);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234), TupleDomainFilter.BigintRange.of(10, 100, false), TupleDomainFilter.BigintRange.of(10, 100, true));
        testRoundTripNumeric(intsBetween(0, 31_234), TupleDomainFilter.BigintRange.of(0, 31_234, false), TupleDomainFilter.BigintRange.of(0, 31_234, true));
        testRoundTripNumeric(intsBetween(0, 31_234), TupleDomainFilter.BigintRange.of(0, 0, false), TupleDomainFilter.BigintRange.of(31_234, 31_234, true));
        testRoundTripNumeric(intsBetween(0, 31_234), TupleDomainFilter.BigintRange.of(-100, -10, false), TupleDomainFilter.BigintRange.of(-100, -10, true));
    }

    private void testRoundTripNumeric(Iterable<Integer> writeValues)
            throws Exception
    {
        tester.testRoundTrip(javaLongObjectInspector,
                transform(writeValues, AbstractTestParquetReader::intToLong),
                AbstractTestParquetReader::toLong,
                BIGINT);
    }

    private void testRoundTripNumeric(Iterable<Integer> writeValues, TupleDomainFilter... filter)
            throws Exception
    {
        tester.testRoundTrip(javaLongObjectInspector,
                transform(writeValues, AbstractTestParquetReader::intToLong),
                AbstractTestParquetReader::toLong,
                BIGINT,
                filter);
    }

    private static Map<Subfield, TupleDomainFilter> toSubfieldFilter(String subfield, TupleDomainFilter filter)
    {
        return ImmutableMap.of(new Subfield(subfield), filter);
    }

    private static Map<Subfield, TupleDomainFilter> toSubfieldFilter(TupleDomainFilter filter)
    {
        return ImmutableMap.of(new Subfield("c"), filter);
    }

    private static List<Map<Subfield, TupleDomainFilter>> toSubfieldFilters(TupleDomainFilter... filters)
    {
        return Arrays.stream(filters).map(AbstractTestParquetSelectiveReader::toSubfieldFilter).collect(toImmutableList());
    }
}
