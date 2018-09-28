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
package com.facebook.presto.ranger;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.Identity;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.HashMap;
import java.util.Optional;

public class TestRangerSystemAccessControl
{
    private PrestoAuthorizer prestoAuthorizer;
    private RangerSystemAccessControl normalRangerSystemAccessControl;
    private RangerSystemAccessControl writeableRangerSystemAccessControl;
    private RangerSystemAccessControl powerUserRangerSystemAccessControl;
    private RangerSystemAccessControl powerPrincipalRangerSystemAccessControl;
    @BeforeTest
    public void setUp()
    {
        UserGroups userGroups = new UserGroups(new HashMap<String, String>());

        RangerPrestoPlugin rangerPrestoPlugin = new RangerPrestoPlugin("presto", "presto");
        this.prestoAuthorizer = new PrestoAuthorizer(userGroups, rangerPrestoPlugin);

        this.normalRangerSystemAccessControl = new RangerSystemAccessControl(prestoAuthorizer, new HashMap<String, String>());

        this.writeableRangerSystemAccessControl = new RangerSystemAccessControl(prestoAuthorizer, new HashMap<String, String>()
        {
            {
                put("writeable-catalogs", "writeable1,writeable2");
            }
        });

        this.powerUserRangerSystemAccessControl = new RangerSystemAccessControl(prestoAuthorizer, new HashMap<String, String>()
        {
            {
                put("power-users", "user1,user2");
            }
        });

        this.powerPrincipalRangerSystemAccessControl = new RangerSystemAccessControl(prestoAuthorizer, new HashMap<String, String>()
        {
            {
                put("power-principals", "principal1,principal2");
            }
        });
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testCheckCanSetUser() throws Exception
    {
        Principal principal = new BasicPrincipal("principal1");
        normalRangerSystemAccessControl.checkCanSetUser(Optional.of(principal), "user1");
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testCheckCanSetSystemSessionProperty() throws Exception
    {
        Identity identity = new Identity("user1", Optional.empty());
        normalRangerSystemAccessControl.checkCanSetSystemSessionProperty(identity, "property");
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testCheckCanSetCatalogSessionProperty() throws Exception
    {
        Identity identity = new Identity("user1", Optional.empty());
        normalRangerSystemAccessControl.checkCanSetCatalogSessionProperty(identity, "catalog", "property");
    }

    @Test
    public void testCheckCanShowTablesMetadata() throws Exception
    {
        Identity identity = new Identity("user1", Optional.empty());
        CatalogSchemaName catalogSchemaName = new CatalogSchemaName("catalog", "database");
        normalRangerSystemAccessControl.checkCanShowTablesMetadata(identity, catalogSchemaName);
        writeableRangerSystemAccessControl.checkCanShowTablesMetadata(identity, catalogSchemaName);
        powerUserRangerSystemAccessControl.checkCanShowTablesMetadata(identity, catalogSchemaName);
        powerPrincipalRangerSystemAccessControl.checkCanShowTablesMetadata(identity, catalogSchemaName);
    }
}
