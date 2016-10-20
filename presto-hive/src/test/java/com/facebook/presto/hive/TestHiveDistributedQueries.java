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
package com.facebook.presto.hive;

import com.facebook.presto.tests.AbstractTestDistributedQueries;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static io.airlift.tpch.TpchTable.getTables;

public class TestHiveDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestHiveDistributedQueries()
            throws Exception
    {
        super(createQueryRunner(getTables()));
    }

    @Override
    public void testDelete()
            throws Exception
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testAddColumn()
            throws Exception
    {
        // Hive connector currently does not support schema change
    }

    @Override
    public void testRenameColumn()
            throws Exception
    {
        // Hive connector currently does not support schema change
    }

    @Override
    public void testRenameTable()
            throws Exception
    {
        // Hive connector currently does not support table rename
    }
}
