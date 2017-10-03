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

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

import static org.apache.http.HttpStatus.SC_OK;

public class MockAmazonS3
        extends AbstractAmazonS3
{
    private int getObjectHttpCode = SC_OK;
    private int getObjectMetadataHttpCode = SC_OK;
    private GetObjectMetadataRequest getObjectMetadataRequest;

    public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode)
    {
        this.getObjectHttpCode = getObjectHttpErrorCode;
    }

    public void setGetObjectMetadataHttpCode(int getObjectMetadataHttpCode)
    {
        this.getObjectMetadataHttpCode = getObjectMetadataHttpCode;
    }

    public GetObjectMetadataRequest getGetObjectMetadataRequest()
    {
        return getObjectMetadataRequest;
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
    {
        this.getObjectMetadataRequest = getObjectMetadataRequest;
        if (getObjectMetadataHttpCode != SC_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObjectMetadata call with " + getObjectMetadataHttpCode);
            exception.setStatusCode(getObjectMetadataHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
    {
        if (getObjectHttpCode != SC_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with " + getObjectHttpCode);
            exception.setStatusCode(getObjectHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
    {
        return new PutObjectResult();
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content)
    {
        return new PutObjectResult();
    }

    @Override
    public void shutdown()
    {
    }
}
