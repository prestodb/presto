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
package com.facebook.presto.hive.aws.security;

import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.hive.aws.security.TestAWSSecurityMappings.MappingSelector.empty;
import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestAWSSecurityMappings
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testAWSLakeFormationMapping()
    {
        String lakeFormationSecurityMappingConfigPath = this.getClass().getClassLoader().getResource("com.facebook.presto.hive.aws.security/aws-security-mapping.json").getPath();

        AWSSecurityMappings mappings = parseJson(new File(lakeFormationSecurityMappingConfigPath).toPath(), AWSSecurityMappings.class);

        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/admin_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(empty().withUser("admin").getUser()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/analyst_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(empty().withUser("analyst").getUser()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/default_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(empty().getUser()).get().getIamRole());
    }

    protected static class MappingSelector
    {
        protected static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private final String user;

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        protected MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        protected String getUser()
        {
            return user;
        }
    }

    protected static class MappingResult
    {
        protected static MappingResult role(String role)
        {
            return new MappingResult(role);
        }

        private final String iamRole;

        private MappingResult(String iamRole)
        {
            this.iamRole = requireNonNull(iamRole, "role is null");
        }

        protected String getIamRole()
        {
            return iamRole;
        }
    }
}
