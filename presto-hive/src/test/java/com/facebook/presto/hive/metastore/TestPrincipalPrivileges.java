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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrincipalPrivileges
{
    @Test
    public void testGetTablePrincipalPrivileges()
    {
        PrincipalPrivileges principalPrivileges = PrincipalPrivileges.fromHivePrivilegeInfos(ImmutableSet.of(
                hivePrivilegeInfo(PrincipalType.USER, "user001"),
                hivePrivilegeInfo(PrincipalType.USER, "user002"),
                hivePrivilegeInfo(PrincipalType.ROLE, "role001")));

        assertNotNull(principalPrivileges);
        assertEquals(principalPrivileges.getUserPrivileges().size(), 2);
        assertEquals(principalPrivileges.getRolePrivileges().size(), 1);
    }

    private HivePrivilegeInfo hivePrivilegeInfo(PrincipalType type, String key)
    {
        if (type == USER) {
            return new HivePrivilegeInfo(SELECT, false, new PrestoPrincipal(USER, key), new PrestoPrincipal(USER, key));
        }
        else {
            return new HivePrivilegeInfo(SELECT, false, new PrestoPrincipal(ROLE, key), new PrestoPrincipal(ROLE, key));
        }
    }
}
