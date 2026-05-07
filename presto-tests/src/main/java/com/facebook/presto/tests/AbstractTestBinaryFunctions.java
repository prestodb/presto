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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

/**
 * Abstract test class for binary functions including FNV hash functions.
 * This class contains E2E tests that can be run against both Java and native implementations.
 */
public abstract class AbstractTestBinaryFunctions
        extends AbstractTestQueryFramework
{
    /**
     * Test FNV-1 32-bit hash function.
     * Ground truth results are generated via https://nqv.github.io/fnv/
     */
    @Test
    public void testFnv1_32()
    {
        // Test empty input - should return offset basis
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex(''))", "SELECT -2128831035");

        // Test single byte inputs
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('19'))", "SELECT 84696326");

        // Test for sign extension bug with high byte value (0xF5)
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('F5'))", "SELECT 84696554");

        // Test for byte ordering with multi-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('0919'))", "SELECT 141986235");

        // Test three-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('F50919'))", "SELECT 1739062764");

        // Test eight-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('232706FC6BF50919'))", "SELECT -1625136141");

        // Test with NULL input
        assertQueryWithSameQueryRunner("SELECT fnv1_32(NULL)", "SELECT CAST(NULL AS INTEGER)");

        // Test with empty string
        assertQueryWithSameQueryRunner("SELECT fnv1_32(to_utf8(''))", "SELECT -2128831035");
    }

    /**
     * Test FNV-1 64-bit hash function.
     * Ground truth results are generated via https://nqv.github.io/fnv/
     */
    @Test
    public void testFnv1_64()
    {
        // Test empty input - should return offset basis
        assertQueryWithSameQueryRunner("SELECT fnv1_64(from_hex(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");

        // Test eight-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1_64(from_hex('232706FC6BF50919'))", "SELECT CAST(5360971952898613043 AS BIGINT)");

        // Test with NULL input
        assertQueryWithSameQueryRunner("SELECT fnv1_64(NULL)", "SELECT CAST(NULL AS BIGINT)");

        // Test with string converted to binary
        assertQueryWithSameQueryRunner("SELECT fnv1_64(to_utf8('hello'))", "SELECT CAST(8883723591023973575 AS BIGINT)");

        // Test with empty string
        assertQueryWithSameQueryRunner("SELECT fnv1_64(to_utf8(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");

        // Test single byte
        assertQueryWithSameQueryRunner("SELECT fnv1_64(from_hex('19'))", "SELECT CAST(-5808590958014384186 AS BIGINT)");
    }

    /**
     * Test FNV-1a 32-bit hash function.
     * FNV-1a differs from FNV-1 by XORing before multiplying instead of after.
     * Ground truth results are generated via https://nqv.github.io/fnv/
     */
    @Test
    public void testFnv1a_32()
    {
        // Test empty input - should return offset basis
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex(''))", "SELECT -2128831035");

        // Test single byte inputs
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('19'))", "SELECT 470581588");

        // Test for sign extension bug with high byte value (0xF5)
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('F5'))", "SELECT 1879798416");

        // Test for byte ordering with multi-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('0919'))", "SELECT 881334279");

        // Test three-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('F50919'))", "SELECT -343882906");

        // Test eight-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('232706FC6BF50919'))", "SELECT 156357983");

        // Test with NULL input
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(NULL)", "SELECT CAST(NULL AS INTEGER)");

        // Test with empty string
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(to_utf8(''))", "SELECT -2128831035");
    }

    /**
     * Test FNV-1a 64-bit hash function.
     * FNV-1a differs from FNV-1 by XORing before multiplying instead of after.
     * Ground truth results are generated via https://nqv.github.io/fnv/
     */
    @Test
    public void testFnv1a_64()
    {
        // Test empty input - should return offset basis
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(from_hex(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");

        // Test eight-byte input
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(from_hex('232706FC6BF50919'))", "SELECT CAST(7542926890985303135 AS BIGINT)");

        // Test with NULL input
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(NULL)", "SELECT CAST(NULL AS BIGINT)");

        // Test with string converted to binary
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(to_utf8('hello'))", "SELECT CAST(-6615550055289275125 AS BIGINT)");

        // Test with empty string
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(to_utf8(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");

        // Test single byte
        assertQueryWithSameQueryRunner("SELECT fnv1a_64(from_hex('19'))", "SELECT CAST(-5808565669246935308 AS BIGINT)");
    }

    /**
     * Test FNV hash functions with various edge cases and data types.
     */
    @Test
    public void testFnvHashEdgeCases()
    {
        // Test with longer strings
        assertQueryWithSameQueryRunner(
                "SELECT fnv1_32(to_utf8('The quick brown fox jumps over the lazy dog'))",
                "SELECT CAST(-372741010 AS INTEGER)");
        assertQueryWithSameQueryRunner(
                "SELECT fnv1a_32(to_utf8('The quick brown fox jumps over the lazy dog'))",
                "SELECT CAST(76545936 AS INTEGER)");

        // Test consistency - same input should produce same output
        assertQueryWithSameQueryRunner(
                "SELECT fnv1_32(from_hex('DEADBEEF')) = fnv1_32(from_hex('DEADBEEF'))",
                "SELECT true");

        assertQueryWithSameQueryRunner(
                "SELECT fnv1a_64(from_hex('CAFEBABE')) = fnv1a_64(from_hex('CAFEBABE'))",
                "SELECT true");

        // Test that FNV-1 and FNV-1a produce different results for non-empty input
        assertQueryWithSameQueryRunner(
                "SELECT fnv1_32(from_hex('19')) <> fnv1a_32(from_hex('19'))",
                "SELECT true");

        assertQueryWithSameQueryRunner(
                "SELECT fnv1_64(from_hex('19')) <> fnv1a_64(from_hex('19'))",
                "SELECT true");

        // Test that both variants return the same offset basis for empty input
        assertQueryWithSameQueryRunner(
                "SELECT fnv1_32(from_hex('')) = fnv1a_32(from_hex(''))",
                "SELECT true");

        assertQueryWithSameQueryRunner(
                "SELECT fnv1_64(from_hex('')) = fnv1a_64(from_hex(''))",
                "SELECT true");
    }

    /**
     * Test FNV hash functions in aggregate contexts.
     */
    @Test
    public void testFnvHashInAggregations()
    {
        // Test with array of values
        assertQueryWithSameQueryRunner(
                "SELECT COUNT(DISTINCT fnv1_32(to_utf8(x))) FROM (VALUES 'a', 'b', 'c', 'a') t(x)",
                "SELECT CAST(3 AS BIGINT)");

        // Test grouping by hash values
        assertQueryWithSameQueryRunner(
                "SELECT fnv1a_32(to_utf8(x)), COUNT(*) FROM (VALUES 'a', 'b', 'a', 'c', 'b') t(x) GROUP BY fnv1a_32(to_utf8(x)) ORDER BY COUNT(*) DESC, fnv1a_32(to_utf8(x))",
                "VALUES (-468965076, CAST(2 AS BIGINT)), (-418632219, CAST(2 AS BIGINT)), (-435409838, CAST(1 AS BIGINT))");
    }
    /**
     * Test FNV hash functions with binary data from various sources.
     */
    @Test
    public void testFnvHashWithBinaryData()
    {
        // Test with concatenated binary data
        assertQueryWithSameQueryRunner(
                "SELECT fnv1_32(from_hex('AB') || from_hex('CD'))",
                "SELECT fnv1_32(from_hex('ABCD'))");

        // Test with repeated patterns
        assertQueryWithSameQueryRunner(
                "SELECT fnv1a_64(from_hex('FF') || from_hex('FF') || from_hex('FF'))",
                "SELECT fnv1a_64(from_hex('FFFFFF'))");

        // Test with all zeros - expected value computed from FNV-1 32-bit algorithm
        assertQueryWithSameQueryRunner("SELECT fnv1_32(from_hex('00000000'))", "SELECT 1268118805");

        // Test with all ones - expected value computed from FNV-1a 32-bit algorithm
        assertQueryWithSameQueryRunner("SELECT fnv1a_32(from_hex('FFFFFFFF'))", "SELECT -485093455");
    }
}
