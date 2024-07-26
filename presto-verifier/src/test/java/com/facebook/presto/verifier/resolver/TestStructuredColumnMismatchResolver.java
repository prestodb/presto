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

import java.util.Optional;

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
        ArrayColumnChecksum checksum1 = new ArrayColumnChecksum(binary(0xa), binary(0xb), 1, Optional.empty());
        ArrayColumnChecksum checksum2 = new ArrayColumnChecksum(binary(0x1a), binary(0xb), 1, Optional.empty());
        ArrayColumnChecksum checksum3 = new ArrayColumnChecksum(binary(0xa), binary(0x1b), 1, Optional.empty());
        ArrayColumnChecksum checksum4 = new ArrayColumnChecksum(binary(0xa), binary(0xb), 2, Optional.empty());

        // Resolved: floating point, but without FloatingPointColumnChecksum part.
        assertResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum1, checksum2));
        assertResolved(createMismatchedColumn(new ArrayType(REAL), checksum1, checksum2));
        assertResolved(createMismatchedColumn(new ArrayType(new ArrayType(DOUBLE)), checksum1, checksum2));

        // Not resolved: contains no floating point types.
        assertNotResolved(createMismatchedColumn(new ArrayType(INTEGER), checksum1, checksum2));
        assertNotResolved(createMismatchedColumn(new ArrayType(new ArrayType(INTEGER)), checksum1, checksum2));

        // Not resolved: floating point, without FloatingPointColumnChecksum part, but cardinality mismatches.
        assertNotResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum1, checksum3));
        assertNotResolved(createMismatchedColumn(new ArrayType(REAL), checksum1, checksum4));

        ArrayColumnChecksum checksum5 = new ArrayColumnChecksum(null, binary(0xb), 1,
                Optional.of(new FloatingPointColumnChecksum(binary(0xa), 1, 2, 3, 1)));
        ArrayColumnChecksum checksum6 = new ArrayColumnChecksum(null, binary(0xb), 1,
                Optional.of(new FloatingPointColumnChecksum(binary(0xc), 1, 2, 3, 1)));

        // Not resolved: floating point, but with FloatingPointColumnChecksum part.
        assertNotResolved(createMismatchedColumn(new ArrayType(DOUBLE), checksum5, checksum6));
        assertNotResolved(createMismatchedColumn(new ArrayType(REAL), checksum1, checksum4));
    }

    @Test
    public void testResolveMap()
    {
        MapColumnChecksum cs1 = new MapColumnChecksum(binary(0x1), binary(0xa), null, Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum cs2 = new MapColumnChecksum(binary(0x2), binary(0xb), null, Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum cs3 = new MapColumnChecksum(binary(0x3), binary(0xa), null, Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum cs4 = new MapColumnChecksum(binary(0x1), binary(0xa), null, Optional.empty(), Optional.empty(), binary(0x1c), 1);
        MapColumnChecksum cs5 = new MapColumnChecksum(binary(0x1), binary(0xa), null, Optional.empty(), Optional.empty(), binary(0xc), 2);

        // Resolved - both floating points, cardinality is good.
        assertResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), cs1, cs2));
        assertResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), cs1, cs3));
        // Not resolved - both floating points, but cardinality is bad.
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), cs1, cs4));
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, DOUBLE), cs1, cs5));

        MapColumnChecksum csFloatNonFloat1 = new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xa), Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum csFloatNonFloat2 = new MapColumnChecksum(binary(0x2), binary(0xb), binary(0xa), Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum csFloatNonFloat3 = new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xa), Optional.empty(), Optional.empty(), binary(0x1c), 1);
        MapColumnChecksum csFloatNonFloat4 = new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xa), Optional.empty(), Optional.empty(), binary(0x1c), 2);
        MapColumnChecksum csFloatNonFloat5 = new MapColumnChecksum(binary(0x1), binary(0xa), binary(0xb), Optional.empty(), Optional.empty(), binary(0xc), 1);
        MapColumnChecksum csFloatNonFloat6 = new MapColumnChecksum(binary(0x2), binary(0xb), binary(0xb), Optional.empty(), Optional.empty(), binary(0xc), 1);

        // Resolved - key floating points, cardinality is good.
        assertResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), csFloatNonFloat1, csFloatNonFloat2));
        // Not resolved - cardinality is bad.
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), csFloatNonFloat1, csFloatNonFloat3));
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), csFloatNonFloat1, csFloatNonFloat4));
        // Not resolved - key is floating point, but value is not.
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), csFloatNonFloat1, csFloatNonFloat5));
        assertNotResolved(createMismatchedColumn(mapType(DOUBLE, INTEGER), csFloatNonFloat1, csFloatNonFloat6));

        // Not resolved, contains no floating point types.
        assertNotResolved(createMismatchedColumn(mapType(new ArrayType(VARCHAR), RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR))), cs1, cs2));

        // Resolved - key matches, but value is floating point.
        assertResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), cs1, cs3));
        // Not resolved - both floating points, but cardinality is bad.
        assertNotResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), csFloatNonFloat1, csFloatNonFloat3));
        assertNotResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), csFloatNonFloat1, csFloatNonFloat4));
        // Not resolved - key does not match.
        assertNotResolved(createMismatchedColumn(mapType(INTEGER, DOUBLE), cs1, cs2));
    }

    @Test
    public void testNotResolved()
    {
        assertNotResolved(createMismatchedColumn(
                VARCHAR,
                new SimpleColumnChecksum(binary(0xa), Optional.empty()),
                new SimpleColumnChecksum(binary(0xb), Optional.empty())));
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
                new ArrayColumnChecksum(binary(0xa), binary(0xc), 1, Optional.empty()),
                new ArrayColumnChecksum(binary(0xb), binary(0xc), 1, Optional.empty()));
        ColumnMatchResult<?> resolvable2 = createMismatchedColumn(
                mapType(REAL, DOUBLE),
                new MapColumnChecksum(binary(0x1), binary(0xa), null, Optional.empty(), Optional.empty(), binary(0xc), 1),
                new MapColumnChecksum(binary(0x4), binary(0xb), null, Optional.empty(), Optional.empty(), binary(0xc), 1));
        ColumnMatchResult<?> nonResolvable = createMismatchedColumn(
                VARCHAR,
                new SimpleColumnChecksum(binary(0xa), Optional.empty()),
                new SimpleColumnChecksum(binary(0xb), Optional.empty()));

        assertResolved(resolvable1, resolvable2);
        assertNotResolved(resolvable1, nonResolvable);
        assertNotResolved(nonResolvable, resolvable2);
        assertNotResolved(resolvable1, nonResolvable, resolvable2);
    }
}
