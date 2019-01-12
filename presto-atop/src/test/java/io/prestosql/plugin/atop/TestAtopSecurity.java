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
package io.prestosql.plugin.atop;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.atop.LocalAtopQueryRunner.createQueryRunner;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestAtopSecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        String path = this.getClass().getResource("security.json").getPath();
        queryRunner = createQueryRunner(ImmutableMap.of("atop.security", "file", "security.config-file", path), TestingAtopFactory.class);
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
        queryRunner.execute(admin, "SELECT * FROM disks");
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("bob");
        queryRunner.execute(bob, "SELECT * FROM disks");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get())
                .setIdentity(new Identity(user, Optional.empty())).build();
    }
}
