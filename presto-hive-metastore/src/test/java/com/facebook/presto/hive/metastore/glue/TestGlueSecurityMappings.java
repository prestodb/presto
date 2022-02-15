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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.glue.TestGlueSecurityMappings.MappingSelector.empty;
import static com.facebook.presto.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestGlueSecurityMappings
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testMapping()
    {
        String glueSecurityMappingConfigPath = this.getClass().getClassLoader().getResource("com.facebook.presto.hive.metastore.glue/security-mapping.json").getPath();

        GlueSecurityMappings mappings = parseJson(new File(glueSecurityMappingConfigPath).toPath(), GlueSecurityMappings.class);

        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/admin_role").getIamRole(),
                mappings.getMapping(empty().withUser("admin").getMetastoreContext()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/analyst_role").getIamRole(),
                mappings.getMapping(empty().withUser("analyst").getMetastoreContext()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/default_role").getIamRole(),
                mappings.getMapping(empty().getMetastoreContext()).get().getIamRole());
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private final String user;

        public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        public MetastoreContext getMetastoreContext()
        {
            return new MetastoreContext(
                    new ConnectorIdentity(user, Optional.empty(), Optional.empty(), Collections.emptyMap(), Collections.emptyMap()),
                    PRESTO_QUERY_ID_NAME, Optional.empty(), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        }
    }

    public static class MappingResult
    {
        public static MappingResult role(String role)
        {
            return new MappingResult(role);
        }

        private final String iamRole;

        private MappingResult(String iamRole)
        {
            this.iamRole = requireNonNull(iamRole, "role is null");
        }

        public String getIamRole()
        {
            return iamRole;
        }
    }
}
