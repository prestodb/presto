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
package com.facebook.presto.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper
{
    private BigQueryReadClient client;
    private final String streamName;
    private int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryReadClient client, String streamName, int maxReadRowsRetries)
    {
        this.client = requireNonNull(client, "client cannot be null");
        this.streamName = requireNonNull(streamName, "streamName cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    // TODO: iterator based implementation, instead of fetching all result at once
    public Iterator<ReadRowsResponse> readRows()
    {
        List<ReadRowsResponse> readRowResponses = new ArrayList<>();
        long nextOffset = 0;
        int retries = 0;
        Iterator<ReadRowsResponse> serverResponses = fetchResponses(0);
        while (serverResponses.hasNext()) {
            try {
                ReadRowsResponse response = serverResponses.next();
                nextOffset += response.getRowCount();
                readRowResponses.add(response);
            }
            catch (RuntimeException e) {
                // if relevant, retry the read, from the last read position
                if (BigQueryUtil.isRetryable(e) && retries < maxReadRowsRetries) {
                    serverResponses = fetchResponses(nextOffset);
                    retries++;
                }
                else {
                    // to safely close the client
                    try (BigQueryReadClient ignored = client) {
                        throw e;
                    }
                }
            }
        }
        return readRowResponses.iterator();
    }

    // for testing
    protected Iterator<ReadRowsResponse> fetchResponses(long offset)
    {
        return client.readRowsCallable()
                .call(ReadRowsRequest.newBuilder()
                        .setReadStream(streamName)
                        .setOffset(offset)
                        .build())
                .iterator();
    }
}
