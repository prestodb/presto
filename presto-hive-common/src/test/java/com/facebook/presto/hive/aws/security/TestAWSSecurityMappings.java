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

import com.google.common.base.VerifyException;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestAWSSecurityMappings
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testValidAWSLakeFormationMapping()
    {
        String lakeFormationSecurityMappingConfigPath =
                this.getClass().getClassLoader().getResource("com.facebook.presto.hive.aws.security/aws-security-mapping-lakeformation-valid.json").getPath();

        AWSSecurityMappings mappings = parseJson(new File(lakeFormationSecurityMappingConfigPath).toPath(), AWSSecurityMappings.class);

        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/admin_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("admin").getUser()).getIamRole().get());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/analyst_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("analyst").getUser()).getIamRole().get());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/default_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().getUser()).getIamRole().get());
    }

    @Test(
            expectedExceptions = VerifyException.class,
            expectedExceptionsMessageRegExp =
                    "(iamRole is mandatory for AWS Lake Formation Security Mapping|Basic AWS Credentials are not supported for AWS Lake Formation Security Mapping)")
    public void testInvalidAWSLakeFormationMapping()
    {
        String lakeFormationSecurityMappingConfigPath =
                this.getClass().getClassLoader().getResource("com.facebook.presto.hive.aws.security/aws-security-mapping-lakeformation-invalid.json").getPath();

        AWSSecurityMappings mappings = parseJson(new File(lakeFormationSecurityMappingConfigPath).toPath(), AWSSecurityMappings.class);

        // Fails with VerifyException: iamRole is mandatory for AWS Lake Formation Security Mapping
        mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("admin").getUser());

        // Fails with VerifyException: Basic AWS Credentials are not supported for AWS Lake Formation Security Mapping
        mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("analyst").getUser());
    }

    private static class MappingSelector
    {
        private static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private final String user;

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        private MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        private String getUser()
        {
            return user;
        }
    }

    private static class MappingResult
    {
        private static MappingResult role(String role)
        {
            return new MappingResult(role);
        }

        private final String iamRole;

        private MappingResult(String iamRole)
        {
            this.iamRole = requireNonNull(iamRole, "role is null");
        }

        private String getIamRole()
        {
            return iamRole;
        }
    }
}
