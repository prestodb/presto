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

public abstract class AbstractTestBinaryFunctions
        extends AbstractTestQueryFramework
{
    @Test
    public void testFnv1_32()
    {
        assertQuery("SELECT fnv1_32(from_hex(''))", "SELECT -2128831035");
        assertQuery("SELECT fnv1_32(from_hex('19'))", "SELECT 84696326");
        assertQuery("SELECT fnv1_32(from_hex('F5'))", "SELECT 84696554");
        assertQuery("SELECT fnv1_32(from_hex('0919'))", "SELECT 141986235");
        assertQuery("SELECT fnv1_32(from_hex('F50919'))", "SELECT 1739062764");
        assertQuery("SELECT fnv1_32(from_hex('232706FC6BF50919'))", "SELECT -1625136141");
        assertQuery("SELECT fnv1_32(NULL)", "SELECT CAST(NULL AS INTEGER)");
        assertQuery("SELECT fnv1_32(to_utf8(''))", "SELECT -2128831035");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1_32(data) FROM (VALUES from_hex(''), from_hex('19'), from_hex('F5'), from_hex('0919')) t(data)",
                "VALUES -2128831035, 84696326, 84696554, 141986235");
        assertQuery(
                "SELECT fnv1_32(data) FROM (VALUES from_hex('F50919'), from_hex('232706FC6BF50919'), NULL, to_utf8('')) t(data)",
                "VALUES 1739062764, -1625136141, CAST(NULL AS INTEGER), -2128831035");
    }

    @Test
    public void testFnv1_64()
    {
        assertQuery("SELECT fnv1_64(from_hex(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");
        assertQuery("SELECT fnv1_64(from_hex('232706FC6BF50919'))", "SELECT CAST(5360971952898613043 AS BIGINT)");
        assertQuery("SELECT fnv1_64(NULL)", "SELECT CAST(NULL AS BIGINT)");
        assertQuery("SELECT fnv1_64(to_utf8('hello'))", "SELECT CAST(8883723591023973575 AS BIGINT)");
        assertQuery("SELECT fnv1_64(to_utf8(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");
        assertQuery("SELECT fnv1_64(from_hex('19'))", "SELECT CAST(-5808590958014384186 AS BIGINT)");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1_64(data) FROM (VALUES from_hex(''), from_hex('232706FC6BF50919'), from_hex('19')) t(data)",
                "VALUES CAST(-3750763034362895579 AS BIGINT), CAST(5360971952898613043 AS BIGINT), CAST(-5808590958014384186 AS BIGINT)");
        assertQuery(
                "SELECT fnv1_64(data) FROM (VALUES to_utf8('hello'), to_utf8(''), NULL) t(data)",
                "VALUES CAST(8883723591023973575 AS BIGINT), CAST(-3750763034362895579 AS BIGINT), CAST(NULL AS BIGINT)");
    }

    @Test
    public void testFnv1a_32()
    {
        assertQuery("SELECT fnv1a_32(from_hex(''))", "SELECT -2128831035");
        assertQuery("SELECT fnv1a_32(from_hex('19'))", "SELECT 470581588");
        assertQuery("SELECT fnv1a_32(from_hex('F5'))", "SELECT 1879798416");
        assertQuery("SELECT fnv1a_32(from_hex('0919'))", "SELECT 881334279");
        assertQuery("SELECT fnv1a_32(from_hex('F50919'))", "SELECT -343882906");
        assertQuery("SELECT fnv1a_32(from_hex('232706FC6BF50919'))", "SELECT 156357983");
        assertQuery("SELECT fnv1a_32(NULL)", "SELECT CAST(NULL AS INTEGER)");
        assertQuery("SELECT fnv1a_32(to_utf8(''))", "SELECT -2128831035");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1a_32(data) FROM (VALUES from_hex(''), from_hex('19'), from_hex('F5'), from_hex('0919')) t(data)",
                "VALUES -2128831035, 470581588, 1879798416, 881334279");
        assertQuery(
                "SELECT fnv1a_32(data) FROM (VALUES from_hex('F50919'), from_hex('232706FC6BF50919'), NULL, to_utf8('')) t(data)",
                "VALUES -343882906, 156357983, CAST(NULL AS INTEGER), -2128831035");
    }

    @Test
    public void testFnv1a_64()
    {
        assertQuery("SELECT fnv1a_64(from_hex(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");
        assertQuery("SELECT fnv1a_64(from_hex('232706FC6BF50919'))", "SELECT CAST(7542926890985303135 AS BIGINT)");
        assertQuery("SELECT fnv1a_64(NULL)", "SELECT CAST(NULL AS BIGINT)");
        assertQuery("SELECT fnv1a_64(to_utf8('hello'))", "SELECT CAST(-6615550055289275125 AS BIGINT)");
        assertQuery("SELECT fnv1a_64(to_utf8(''))", "SELECT CAST(-3750763034362895579 AS BIGINT)");
        assertQuery("SELECT fnv1a_64(from_hex('19'))", "SELECT CAST(-5808565669246935308 AS BIGINT)");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1a_64(data) FROM (VALUES from_hex(''), from_hex('232706FC6BF50919'), from_hex('19')) t(data)",
                "VALUES CAST(-3750763034362895579 AS BIGINT), CAST(7542926890985303135 AS BIGINT), CAST(-5808565669246935308 AS BIGINT)");
        assertQuery(
                "SELECT fnv1a_64(data) FROM (VALUES to_utf8('hello'), to_utf8(''), NULL) t(data)",
                "VALUES CAST(-6615550055289275125 AS BIGINT), CAST(-3750763034362895579 AS BIGINT), CAST(NULL AS BIGINT)");
    }

    @Test
    public void testFnvHashEdgeCases()
    {
        assertQuery(
                "SELECT fnv1_32(to_utf8('The quick brown fox jumps over the lazy dog'))",
                "SELECT CAST(-372741010 AS INTEGER)");
        assertQuery(
                "SELECT fnv1a_32(to_utf8('The quick brown fox jumps over the lazy dog'))",
                "SELECT CAST(76545936 AS INTEGER)");

        assertQuery(
                "SELECT fnv1_32(from_hex('DEADBEEF')) = fnv1_32(from_hex('DEADBEEF'))",
                "SELECT true");
        assertQuery(
                "SELECT fnv1a_64(from_hex('CAFEBABE')) = fnv1a_64(from_hex('CAFEBABE'))",
                "SELECT true");

        assertQuery(
                "SELECT fnv1_32(from_hex('19')) <> fnv1a_32(from_hex('19'))",
                "SELECT true");
        assertQuery(
                "SELECT fnv1_64(from_hex('19')) <> fnv1a_64(from_hex('19'))",
                "SELECT true");

        assertQuery(
                "SELECT fnv1_32(from_hex('')) = fnv1a_32(from_hex(''))",
                "SELECT true");
        assertQuery(
                "SELECT fnv1_64(from_hex('')) = fnv1a_64(from_hex(''))",
                "SELECT true");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1_32(data), fnv1a_32(data) FROM (VALUES to_utf8('The quick brown fox jumps over the lazy dog')) t(data)",
                "VALUES (CAST(-372741010 AS INTEGER), CAST(76545936 AS INTEGER))");
        assertQuery(
                "SELECT fnv1_32(data) = fnv1_32(data), fnv1a_64(data) = fnv1a_64(data) FROM (VALUES from_hex('DEADBEEF'), from_hex('CAFEBABE')) t(data)",
                "VALUES (true, true), (true, true)");
    }

    @Test
    public void testFnvHashInAggregations()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT fnv1_32(to_utf8(x))) FROM (VALUES 'a', 'b', 'c', 'a') t(x)",
                "SELECT CAST(3 AS BIGINT)");
        assertQuery(
                "SELECT fnv1a_32(to_utf8(x)), COUNT(*) FROM (VALUES 'a', 'b', 'a', 'c', 'b') t(x) GROUP BY fnv1a_32(to_utf8(x)) ORDER BY COUNT(*) DESC, fnv1a_32(to_utf8(x))",
                "VALUES (-468965076, CAST(2 AS BIGINT)), (-418632219, CAST(2 AS BIGINT)), (-435409838, CAST(1 AS BIGINT))");
    }

    @Test
    public void testFnvHashWithBinaryData()
    {
        assertQuery(
                "SELECT fnv1_32(from_hex('AB') || from_hex('CD')) = fnv1_32(from_hex('ABCD'))",
                "SELECT true");
        assertQuery(
                "SELECT fnv1a_64(from_hex('FF') || from_hex('FF') || from_hex('FF')) = fnv1a_64(from_hex('FFFFFF'))",
                "SELECT true");

        assertQuery("SELECT fnv1_32(from_hex('00000000'))", "SELECT 1268118805");
        assertQuery("SELECT fnv1a_32(from_hex('FFFFFFFF'))", "SELECT -485093455");

        // Test with VALUES to exercise non-constant-folded execution
        assertQuery(
                "SELECT fnv1_32(data1 || data2) = fnv1_32(from_hex('ABCD')) FROM (VALUES (from_hex('AB'), from_hex('CD'))) t(data1, data2)",
                "SELECT true");
        assertQuery(
                "SELECT fnv1a_64(data || data || data) = fnv1a_64(from_hex('FFFFFF')) FROM (VALUES from_hex('FF')) t(data)",
                "SELECT true");

        assertQuery(
                "SELECT fnv1_32(data) FROM (VALUES from_hex('00000000')) t(data)",
                "VALUES 1268118805");
        assertQuery(
                "SELECT fnv1a_32(data) FROM (VALUES from_hex('FFFFFFFF')) t(data)",
                "VALUES -485093455");
    }
}
