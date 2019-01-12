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
package io.prestosql.plugin.hive.s3;

import com.amazonaws.services.s3.model.CannedAccessControlList;

import static java.util.Objects.requireNonNull;

public enum PrestoS3AclType
{
    AUTHENTICATED_READ(CannedAccessControlList.AuthenticatedRead),
    AWS_EXEC_READ(CannedAccessControlList.AwsExecRead),
    BUCKET_OWNER_FULL_CONTROL(CannedAccessControlList.BucketOwnerFullControl),
    BUCKET_OWNER_READ(CannedAccessControlList.BucketOwnerRead),
    LOG_DELIVERY_WRITE(CannedAccessControlList.LogDeliveryWrite),
    PRIVATE(CannedAccessControlList.Private),
    PUBLIC_READ(CannedAccessControlList.PublicRead),
    PUBLIC_READ_WRITE(CannedAccessControlList.PublicReadWrite);

    private final CannedAccessControlList cannedACL;

    PrestoS3AclType(CannedAccessControlList cannedACL)
    {
        this.cannedACL = requireNonNull(cannedACL, "cannedACL is null");
    }

    CannedAccessControlList getCannedACL()
    {
        return cannedACL;
    }
}
