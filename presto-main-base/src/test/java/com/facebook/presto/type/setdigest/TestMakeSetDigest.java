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
package com.facebook.presto.type.setdigest;

import com.facebook.presto.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestMakeSetDigest
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testMakeSetDigestInteger()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES 1, 1, 1, 2, 2) T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 0c 00 00 00 02 0b 02   00 80 03 44 00 80 20 08   de 00 20 00 00 02 00 00   00 a8 c0 76 6c a0 20 08   de 4a c4 05 fb b7 03 44   00 02 00 03 00");
    }

    @Test
    public void testMakeSetDigestLong()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES -398741129664271, 9000000125356546) T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 0c 00 00 00 02 0b 02   00 02 5f e3 93 40 15 34   fa 00 20 00 00 02 00 00   00 e0 7f 7d fb 0d 5f e3   93 67 9b 35 b1 60 15 34   fa 01 00 01 00");
    }

    @Test
    public void testMakeSetDigestFloat()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES 1.2, 2.3) T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 0c 00 00 00 02 0b 02   00 00 fd db 53 80 bb 20   9d 00 20 00 00 02 00 00   00 f6 96 17 64 b0 bb 20   9d 19 15 c6 ec 29 fd db   53 01 00 01 00");
    }

    @Test
    public void testMakeSetDigestChar()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES 'c') T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 08 00 00 00 02 0b 01   00 40 df 38 8e 00 20 00   00 01 00 00 00 d7 74 1f   4a 6c df 38 8e 01 00");
    }

    @Test
    public void testMakeSetDigestVarchar()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES 'abc', 'def') T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 0c 00 00 00 02 0b 02   00 40 a5 e1 87 00 3f 96   b4 00 20 00 00 02 00 00   00 9b db 75 37 71 a5 e1   87 67 78 ad 3f 3f 3f 96   b4 01 00 01 00");
    }

    @Test
    public void testMakeSetDigestDate()
    {
        assert assertions.getQueryRunner().execute(
                        "SELECT make_set_digest(value) " +
                                "FROM (VALUES 2009-09-15, 1998-11-30) T(value)").getMaterializedRows().get(0).getField(0).toString().replaceAll("\n", "   ")
                .equals("01 0c 00 00 00 02 0b 02   00 41 50 11 6d 82 6f 8c   78 00 20 00 00 02 00 00   00 36 63 98 5e 5e 50 11   6d c2 d0 81 a4 8d 6f 8c   78 01 00 01 00");
    }
}
