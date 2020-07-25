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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.verifier.checksum.ArrayColumnChecksum;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.checksum.FloatingPointColumnChecksum;
import com.facebook.presto.verifier.checksum.MapColumnChecksum;
import com.facebook.presto.verifier.checksum.SimpleColumnChecksum;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.facebook.presto.verifier.resolver.FailureResolverTestUtil.binary;
import static com.facebook.presto.verifier.resolver.FailureResolverTestUtil.createMismatchedColumn;

public class TestStructuredColumnMismatchResolver
        extends AbstractTestResultMismatchResolver
{
    public TestStructuredColumnMismatchResolver()
    {
        super(new StructuredColumnMismatchResolver());
    }

    @Test
    public void testResolveArray()
    {
        ArrayColumnChecksum checksum1 = new ArrayColumnChecksum(binary(0xa), binary(0xb), 1);
        ArrayColumnChecksum checksum2 = new ArrayColumnChecksum(binary(0x1a), binary(0xb), 1);
        ArrayColumnChecksum checksum3 = new ArrayColumnChecksum(binary(0xa), binary(0x1b), 1);
        ArrayColumnChecksum checksum4 = new ArrayColumnChecksum(binary(0xa), binary(0xb), 2);

        // resolved
        assertResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum1, checksum2));
        assertResolved(createMismatchedColumn(new ArrayType(REAL), checksum1, checksum2));
        assertResolved(createMismatchedColumn(new ArrayType(new ArrayType(DOUBLE)), checksum1, checksum2));

        // not resolved, contains no floating point types
        assertNotResolved(createMismatchedColumn(new ArrayType(INTEGER), checksum1, checksum2));
        assertNotResolved(createMismatchedColumn(new ArrayType(new ArrayType(INTEGER)), checksum1, checksum2));

        // not resolved, cardinality mismatches
        assertNotResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum1, checksum3));
        assertNotResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum1, checksum4));
    }

    @Test
    public void testResolveMap()
    {
        MapColumnChecksum checksum1 = new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xa), binary(0xc), 1);
        MapColumnChecksum checksum2 = new MapColumnChecksum(binary(0x2), binary(0xa), binary(0xb), binary(0xc), 1);
        MapColumnChecksum checksum3 = new MapColumnChecksum(binary(0x3), binary(0xb), binary(0xa), binary(0xc), 1);
        MapColumnChecksum checksum4 = new MapColumnChecksum(binary(0x4), binary(0xb), binary(0xb), binary(0xc), 1);
        MapColumnChecksum checksum5 = new MapColumnChecksum(binary(0x5), binary(0xb), binary(0xb), binary(0x1c), 1);
        MapColumnChecksum checksum6 = new MapColumnChecksum(binary(0x5), binary(0xb), binary(0xb), binary(0xc), 2);

        // resolved
        assertResolved(createMismatchedColumn(mapType(REAL, DOUBLE), checksum1, checksum4));
        assertResolved(createMismatchedColumn(mapType(new ArrayType(REAL), RowType.anonymous(ImmutableList.of(INTEGER, DOUBLE))), checksum1, checksum4));

        // not resolved, contains no floating point types
        assertNotResolved(createMismatchedColumn(mapType(new ArrayType(VARCHAR), RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR))), checksum1, checksum4));

        // not resolved, cardinality mismatches
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), checksum1, checksum5));
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), checksum1, checksum6));

        // Key/value checks
        assertResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), checksum1, checksum3));
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), checksum1, checksum4));

        assertResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), checksum1, checksum2));
        assertNotResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), checksum1, checksum4));
    }

    @Test
    public void testNotResolved()
    {
        assertNotResolved(createMismatchedColumn(VARCHAR, new SimpleColumnChecksum(binary(0xa)), new SimpleColumnChecksum(binary(0xb))));
        assertNotResolved(createMismatchedColumn(
                DOUBLE,
                new FloatingPointColumnChecksum(1.0, 0, 0, 0, 5),
                new FloatingPointColumnChecksum(1.1, 0, 0, 0, 5)));
    }

    @Test
    public void testMixed()
    {
        ColumnMatchResult<?> resolvable1 = createMismatchedColumn(
                new ArrayType(new ArrayType(DOUBLE)),
                new ArrayColumnChecksum(binary(0xa), binary(0xc), 1),
                new ArrayColumnChecksum(binary(0xb), binary(0xc), 1));
        ColumnMatchResult<?> resolvable2 = createMismatchedColumn(
                mapType(REAL, DOUBLE),
                new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xa), binary(0xc), 1),
                new MapColumnChecksum(binary(0x4), binary(0xb), binary(0xb), binary(0xc), 1));
        ColumnMatchResult<?> nonResolvable = createMismatchedColumn(VARCHAR, new SimpleColumnChecksum(binary(0xa)), new SimpleColumnChecksum(binary(0xb)));

        assertResolved(resolvable1, resolvable2);
        assertNotResolved(resolvable1, nonResolvable);
        assertNotResolved(nonResolvable, resolvable2);
        assertNotResolved(resolvable1, nonResolvable, resolvable2);
    }
}
