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
package com.facebook.presto.iceberg.security;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestIcebergFileBasedSecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        String path = this.getClass().getResource("security.json").getPath();
        queryRunner = createIcebergQueryRunner(ImmutableMap.of("hive.security", "file", "security.config-file", path));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testAdminCanRead()
    {
        Session admin = getSession("admin");
        queryRunner.execute(admin, "SELECT * FROM test_table");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot select .*")
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("non_admin");
        queryRunner.execute(bob, "SELECT * FROM test_table");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get())
                .setIdentity(new Identity(user, Optional.empty())).build();
    }
}
