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

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestRepartitionQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testBoolean()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        IF (mod(orderkey + linenumber, 11) = 0, FALSE, TRUE) AS flag\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.flag)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {126, 85, 59, 72, -71, -42, 49, 21});
    }

    @Test
    public void testSmallInt()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        CAST(linenumber AS SMALLINT) AS l_linenumber\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_linenumber)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {-87, -84, -37, 75, -52, 22, 87, -29});
    }

    @Test
    public void testInteger()
    {
        assertQuery("SELECT\n" +
                        "    CHECKSUM(l.linenumber)\n" +
                        "    FROM lineitem l, partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {-87, -84, -37, 75, -52, 22, 87, -29});
    }

    @Test
    public void testBigInt()
    {
        assertQuery("SELECT\n" +
                        "    CHECKSUM(l.partkey)\n" +
                        "    FROM lineitem l, partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {122, -36, -24, 126, -7, 61, -78, 5});
    }

    @Test
    public void testIpAddress()
    {
        assertQuery("WITH lineitem_ex AS \n" +
                        "(\n" +
                        "SELECT\n" +
                        "    partkey,\n" +
                        "    CAST(\n" +
                        "        CONCAT(\n" +
                        "            CONCAT(\n" +
                        "                CONCAT(\n" +
                        "                    CONCAT(\n" +
                        "                        CONCAT(\n" +
                        "                            CONCAT(CAST((orderkey % 255) AS VARCHAR), '.'),\n" +
                        "                            CAST((partkey % 255) AS VARCHAR)\n" +
                        "                        ),\n" +
                        "                        '.'\n" +
                        "                    ),\n" +
                        "                    CAST(suppkey AS VARCHAR)\n" +
                        "                ),\n" +
                        "                '.'\n" +
                        "            ),\n" +
                        "            CAST(linenumber AS VARCHAR)\n" +
                        "        ) AS ipaddress\n" +
                        "    ) AS ip\n" +
                        "    FROM lineitem\n" +
                        "    )\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.ip) \n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey",
                new byte[] {92, -57, -31, -119, -122, 9, 118, -31});
    }

    @Test
    public void testBigintWithNulls()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        IF (mod(orderkey + linenumber, 11) = 0, NULL, partkey) AS l_partkey\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_partkey)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.l_partkey = p.partkey",
                new byte[] {-4, -54, 21, 27, 121, 66, 3, 35});
    }

    @Test
    public void testVarchar()
    {
        assertQuery("SELECT\n" +
                        "    CHECKSUM(l.comment)\n" +
                        "    FROM lineitem l, partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {14, -4, 62, 87, 52, 53, -101, -100});
    }

    @Test
    public void testDictionaryOfVarcharWithNulls()
    {
        assertQuery("SELECT\n" +
                        "    CHECKSUM(l.shipmode)\n" +
                        "    FROM lineitem l, partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {127, 108, -69, -115, -43, 44, -54, 88});
    }

    @Test
    public void testArrayOfBigInt()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        ARRAY[orderkey, partkey, suppkey] AS l_array\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_array)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {10, -59, 46, 87, 22, -93, 58, -16});
    }

    @Test
    public void testArrayOfArray()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        ARRAY[ARRAY[orderkey, partkey], ARRAY[suppkey]] AS l_array\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_array)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {10, -59, 46, 87, 22, -93, 58, -16});
    }

    @Test
    public void testStruct()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        CAST(\n" +
                        "            ROW (\n" +
                        "                partkey,\n" +
                        "                suppkey,\n" +
                        "                extendedprice,\n" +
                        "                discount,\n" +
                        "                quantity,\n" +
                        "                shipdate,\n" +
                        "                receiptdate,\n" +
                        "                commitdate,\n" +
                        "                comment\n" +
                        "            ) AS ROW(\n" +
                        "                l_partkey BIGINT,\n" +
                        "                l_suppkey BIGINT,\n" +
                        "                l_extendedprice DOUBLE,\n" +
                        "                l_discount DOUBLE,\n" +
                        "                l_quantity DOUBLE,\n" +
                        "                l_shipdate DATE,\n" +
                        "                l_receiptdate DATE,\n" +
                        "                l_commitdate DATE,\n" +
                        "                l_comment VARCHAR(44)\n" +
                        "            )\n" +
                        "        ) AS l_shipment\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_shipment)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    orders o\n" +
                        "WHERE\n" +
                        "    l.orderkey = o.orderkey",
                new byte[] {-56, 110, 18, -107, -123, 121, 87, 79});
    }

    @Test
    public void testStructWithNulls()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        CAST(\n" +
                        "            IF (\n" +
                        "                mod(partkey, 5) = 0,\n" +
                        "                NULL,\n" +
                        "                ROW(\n" +
                        "                    COMMENT,\n" +
                        "                    IF (\n" +
                        "                        mod(partkey, 13) = 0,\n" +
                        "                        NULL,\n" +
                        "                        CONCAT(CAST(partkey AS VARCHAR), COMMENT)\n" +
                        "                    )\n" +
                        "                )\n" +
                        "            ) AS ROW(s1 VARCHAR, s2 VARCHAR)\n" +
                        "        ) AS l_partkey_struct\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_partkey_struct)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    orders o\n" +
                        "WHERE\n" +
                        "    l.orderkey = o.orderkey",
                new byte[] {67, 108, 83, 92, 16, -5, 66, 65});
    }

    @Test
    public void testMaps()
    {
        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        MAP(ARRAY[1, 2, 3], ARRAY[orderkey, partkey, suppkey]) AS l_map\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_map)\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000",
                new byte[] {-28, 76, -12, -42, 116, -118, -9, 46});

        assertQuery("WITH lineitem_ex AS (\n" +
                        "    SELECT\n" +
                        "        partkey,\n" +
                        "        suppkey,\n" +
                        "        MAP(ARRAY[1, 2, 3], ARRAY[orderkey, partkey, suppkey]) AS l_map\n" +
                        "    FROM lineitem\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CHECKSUM(l.l_map[1])\n" +
                        "FROM lineitem_ex l,\n" +
                        "    partsupp p\n" +
                        "WHERE\n" +
                        "    l.partkey = p.partkey\n" +
                        "    AND l.suppkey = p.suppkey\n" +
                        "    AND p.availqty < 1000\n",
                new byte[] {121, 123, -114, -120, -18, 98, -124, 105});
    }

    private void assertQuery(String query, byte[] checksum)
    {
        assertEquals(computeActual(query).getOnlyValue(), checksum);
    }
}
