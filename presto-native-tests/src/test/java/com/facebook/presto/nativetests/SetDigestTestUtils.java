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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;

public class SetDigestTestUtils
{
    private SetDigestTestUtils() {};
    public static void createJaccardIndexValues(QueryRunner queryRunner) {
        queryRunner.execute("DROP TABLE IF EXISTS jaccardIndexTable");
        queryRunner.execute("CREATE TABLE jaccardIndexTable (v1 integer, v2 integer, u1 integer, u2 integer)");
        queryRunner.execute("INSERT INTO jaccardIndexTable VALUES " +
                "(1, 1, NULL, NULL)," +
                "(NULL, 2, NULL, NULL)," +
                "(2, 3, NULL, NULL)," +
                "(NULL, 4, NULL, NULL)," +
                "(1, 10, NULL, NULL)," +
                "(2, 11, NULL, NULL)," +
                "(3, 12, NULL, NULL)," +
                "(4, 13, NULL, NULL)," +
                "(5, 14, NULL, NULL)," +
                "(1, 101, NULL, NULL)," +
                "(2, 102, NULL, NULL)," +
                "(3, 103, NULL, NULL)," +
                "(4, 104, NULL, NULL)," +
                "(5, 105, NULL, NULL)," +
                "(1, 50, NULL, NULL)," +
                "(25, 75, NULL, NULL)," +
                "(50, 100, NULL, NULL)," +
                "(75, 125, NULL, NULL)," +
                "(100, 150, NULL, NULL)," +
                "(1, 25, NULL, NULL)," +
                "(25, 50, NULL, NULL)," +
                "(50, 75, NULL, NULL)," +
                "(75, 100, NULL, NULL)," +
                "(100, 25, NULL, NULL)," +
                "(1, 95, NULL, NULL)," +
                "(50, 100, NULL, NULL)," +
                "(100, 150, NULL, NULL)," +
                "(95, 200, NULL, NULL)," +
                "(1, 3, NULL, NULL)," +
                "(2, 4, NULL, NULL)," +
                "(3, 5, NULL, NULL)," +
                "(4, 6, NULL, NULL)," +
                "(5, 7, NULL, NULL)," +
                "(1, 2, NULL, NULL)," +
                "(2, 3, NULL, NULL)," +
                "(3, 4, NULL, NULL)," +
                "(4, 2, NULL, NULL)," +
                "(5, 3, NULL, NULL)," +
                "(6, 4, NULL, NULL)," +
                "(7, 2, NULL, NULL)," +
                "(8, 3, NULL, NULL)," +
                "(9, 4, NULL, NULL)," +
                "(10, 2, NULL, NULL)," +
                "(NULL, 1, NULL, NULL)," +
                "(NULL, 2, NULL, NULL)," +
                "(NULL, 3, NULL, NULL)," +
                "(1, NULL, NULL, NULL)," +
                "(2, NULL, NULL, NULL)," +
                "(3, NULL, NULL, NULL)," +
                "(NULL, NULL, NULL, NULL)," +
                "(1, 1, 101, 101)," +
                "(2, 2, 102, 102)," +
                "(3, 30, 103, 130)," +
                "(4, 40, 104, 140)," +
                "(1, 10, 101, 201)," +
                "(2, 20, 102, 202)," +
                "(3, 30, 103, 203)," +
                "(4, 40, 104, 204)," +
                "(NULL, 1, 101, 201)," +
                "(1, NULL, 101, 201)," +
                "(NULL, NULL, NULL, NULL)");
    }
}
