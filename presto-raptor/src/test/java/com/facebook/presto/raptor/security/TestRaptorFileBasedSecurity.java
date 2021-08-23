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
package com.facebook.presto.raptor.security;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestRaptorFileBasedSecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        String path = this.getClass().getResource("security.json").getPath();
        queryRunner = createRaptorQueryRunner(
                ImmutableMap.of(),
                true,
                false,
                false,
                ImmutableMap.of("security.config-file", path, "raptor.security", "file"));
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
        Session admin = getSession("user");
        queryRunner.execute(admin, "SELECT * FROM orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot select from table tpch.orders.*")
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("bob");
        queryRunner.execute(bob, "SELECT * FROM orders");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get())
                .setIdentity(new Identity(user, Optional.empty())).build();
    }
}
