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
package com.facebook.presto.metadata;

import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestScratchTrimQuery
{
    @Test
    public void testTrimOverUnboundedVarchar()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().build());
        for (String sql : ImmutableList.of(
                "SELECT trim(CAST('  abc  ' AS varchar))",
                "SELECT ltrim(CAST('  abc  ' AS varchar))",
                "SELECT rtrim(CAST('  abc  ' AS varchar))",
                "SELECT ltrim(CAST('  abc  ' AS varchar), ' ')",
                "SELECT trim(LEADING ' ' FROM CAST('  abc  ' AS varchar))",
                "SELECT ltrim(CAST('  abc  ' AS varchar(10)))")) {
            try {
                queryRunner.execute(sql);
                System.out.println("OK    : " + sql);
            }
            catch (RuntimeException e) {
                System.out.println("FAILED: " + sql + "\n        " + e);
            }
        }
    }
}
