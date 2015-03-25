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
package com.facebook.presto.client;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UniversalStatementClientTest
{
    QueryHttpClient client;
    URI nextUri;

    @BeforeClass
    public void setUp() throws URISyntaxException
    {
        client = mock(QueryHttpClient.class);
        nextUri = new URI("http://nextUri");
    }

    @Test
    public void testStatementClient() throws URISyntaxException
    {
        QueryResults expected1 = new QueryResults(
                "id1",
                new URI("http://infoUri"),
                new URI("http://partialCancelUri"),
                nextUri,
                null,
                null,
                new StatementStats("?", true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                null,
                null);
        QueryResults expected2 = new QueryResults(
                "id2",
                new URI("http://infoUri"),
                new URI("http://partialCancelUri"),
                null,
                null,
                null,
                new StatementStats("?", true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                null,
                null);

        when(client.startQuery(any(ClientSession.class), eq("test"))).thenReturn(expected1);
        when(client.execute(eq(nextUri))).thenReturn(expected2);

        UniversalStatementClient statementClient = new UniversalStatementClient(client, mock(ClientSession.class), "test");
        assertEquals(statementClient.current().getId(), expected1.getId());
        assertTrue(statementClient.isValid());
        assertTrue(statementClient.advance());
        assertEquals(statementClient.current().getId(), expected2.getId());
    }
}
