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
package com.facebook.presto.hive.s3;

import com.amazonaws.services.s3.model.CannedAccessControlList;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("EnumeratedConstantNamingConvention")
public enum PrestoS3AclType
{
    AuthenticatedRead(CannedAccessControlList.AuthenticatedRead),
    AwsExecRead(CannedAccessControlList.AwsExecRead),
    BucketOwnerFullControl(CannedAccessControlList.BucketOwnerFullControl),
    BucketOwnerRead(CannedAccessControlList.BucketOwnerRead),
    LogDeliveryWrite(CannedAccessControlList.LogDeliveryWrite),
    Private(CannedAccessControlList.Private),
    PublicRead(CannedAccessControlList.PublicRead),
    PublicReadWrite(CannedAccessControlList.PublicReadWrite);

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
