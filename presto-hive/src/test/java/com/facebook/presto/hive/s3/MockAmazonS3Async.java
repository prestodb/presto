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

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.concurrent.CompletableFuture;

public class MockAmazonS3Async
        implements S3AsyncClient
{
    private ObjectCannedACL acl;

    public ObjectCannedACL getAcl()
    {
        return this.acl;
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(PutObjectRequest putObjectRequest, AsyncRequestBody requestBody)
    {
        // Capture ACL
        this.acl = putObjectRequest.acl();

        return CompletableFuture.completedFuture(
                PutObjectResponse.builder()
                        .eTag("mock-etag-12345")
                        .build());
    }

    @Override
    public String serviceName()
    {
        return "S3Async";
    }

    @Override
    public void close()
    {
        // No-op for mock
    }
}
