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
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Test
public class TestReadRowsHelper
{
    // it is not used, we just need the reference
    BigQueryReadClient client = mock(BigQueryReadClient.class);

    @Test
    void testNoFailures()
    {
        MockResponsesBatch batch1 = new MockResponsesBatch();
        batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
        batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());

        // so we can run multiple tests
        ImmutableList<ReadRowsResponse> responses = ImmutableList.copyOf(
                new MockReadRowsHelper(client, "test", 3, ImmutableList.of(batch1))
                        .readRows());

        assertThat(responses.size()).isEqualTo(2);
        assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);
    }

    @Test
    void testRetryOfSingleFailure()
    {
        MockResponsesBatch batch1 = new MockResponsesBatch();
        batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
        batch1.addException(new StatusRuntimeException(Status.INTERNAL.withDescription(
                "Received unexpected EOS on DATA frame from server.")));
        MockResponsesBatch batch2 = new MockResponsesBatch();
        batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());

        ImmutableList<ReadRowsResponse> responses = ImmutableList.copyOf(
                new MockReadRowsHelper(client, "test", 3, ImmutableList.of(batch1, batch2))
                        .readRows());

        assertThat(responses.size()).isEqualTo(2);
        assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);
    }

    private static final class MockReadRowsHelper
            extends ReadRowsHelper
    {
        Iterator<MockResponsesBatch> responses;

        MockReadRowsHelper(BigQueryReadClient client, String stream, int maxReadRowsRetries, Iterable<MockResponsesBatch> responses)
        {
            super(client, stream, maxReadRowsRetries);
            this.responses = responses.iterator();
        }

        @Override
        protected Iterator<ReadRowsResponse> fetchResponses(long offset)
        {
            return responses.next();
        }
    }
}
