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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.facebook.presto.hive.HiveClientConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static com.amazonaws.services.s3.model.SelectObjectContentEvent.EndEvent;
import static java.util.Objects.requireNonNull;

public class PrestoS3SelectClient
        implements Closeable
{
    private final AmazonS3 s3Client;
    private boolean requestComplete;
    private SelectObjectContentRequest selectObjectRequest;
    private SelectObjectContentResult selectObjectContentResult;

    public PrestoS3SelectClient(Configuration config, HiveClientConfig clientConfig, PrestoS3ClientFactory s3ClientFactory)
    {
        requireNonNull(config, "config is null");
        requireNonNull(clientConfig, "clientConfig is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");
        this.s3Client = s3ClientFactory.getS3Client(config, clientConfig);
    }

    public InputStream getRecordsContent(SelectObjectContentRequest selectObjectRequest)
    {
        this.selectObjectRequest = requireNonNull(selectObjectRequest, "selectObjectRequest is null");
        this.selectObjectContentResult = s3Client.selectObjectContent(selectObjectRequest);
        return selectObjectContentResult.getPayload()
                .getRecordsInputStream(
                        new SelectObjectContentEventVisitor()
                        {
                            @Override
                            public void visit(EndEvent endEvent)
                            {
                                requestComplete = true;
                            }
                        });
    }

    @Override
    public void close()
            throws IOException
    {
        selectObjectContentResult.close();
    }

    public String getKeyName()
    {
        return selectObjectRequest.getKey();
    }

    public String getBucketName()
    {
        return selectObjectRequest.getBucketName();
    }

    /**
     * The End Event indicates all matching records have been transmitted.
     * If the End Event is not received, the results may be incomplete.
     */
    public boolean isRequestComplete()
    {
        return requestComplete;
    }
}
