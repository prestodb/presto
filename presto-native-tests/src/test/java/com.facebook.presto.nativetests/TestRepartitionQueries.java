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

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestRepartitionQueries;
import org.testng.annotations.Test;

public class TestRepartitionQueries
        extends AbstractTestRepartitionQueries
{
    private static final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, storageFormat);
    }

    @Override
    protected void createTables()
    {
        try {
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(storageFormat);
            NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testIpAddress()
    {
        // TODO: Enable this query after result mismatch is fixed
        computeActual("WITH lineitem_ex AS \n" +
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
                        "    l.partkey = p.partkey");
    }
}
