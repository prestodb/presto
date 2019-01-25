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
package com.facebook.presto.tests.localtestdriver;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;

public class TestLocalTestDriverClient
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testList()
    {
        MaterializedResult result = queryRunner.execute("select array_agg(name) from tpch.tiny.nation");
        Object value = result.getOnlyValue();
        System.out.println(value.getClass().getName());
    }

    @Test
    public void testClient()
            throws IOException
    {
        LocalTestDriverClient client = new LocalTestDriverClient(queryRunner.getCoordinator());
        client.execute(queryRunner.getDefaultSession(), "select array_agg(name) from tpch.tiny.nation", Optional.of(Paths.get("/tmp/result.out")));
    }
}
