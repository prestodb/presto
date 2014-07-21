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
package com.facebook.presto.cassandra;

import com.google.common.collect.Lists;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.List;

public class CassandraThriftClient
{
    private final CassandraThriftConnectionFactory connectionFactory;

    public CassandraThriftClient(CassandraThriftConnectionFactory connectionFactory)
    {
        this.connectionFactory = connectionFactory;
    }

    public List<TokenRange> getRangeMap(String keyspace)
    {
        Client client = connectionFactory.create();
        try {
            return client.describe_ring(keyspace);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        finally {
            closeQuietly(client);
        }
    }

    public List<CfSplit> getSubSplits(String keyspace, String columnFamily, TokenRange range, int splitSize)
    {
        Client client = connectionFactory.create();
        try {
            client.set_keyspace(keyspace);
            try {
                return client.describe_splits_ex(columnFamily, range.start_token, range.end_token, splitSize);
            }
            catch (TApplicationException e) {
                // fallback to guessing split size if talking to a server without describe_splits_ex method
                if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                    List<String> splitPoints = client.describe_splits(columnFamily, range.start_token, range.end_token, splitSize);
                    return tokenListToSplits(splitPoints, splitSize);
                }
                throw e;
            }
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        finally {
            closeQuietly(client);
        }
    }

    private static List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitSize)
    {
        List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
        for (int index = 0; index < splitTokens.size() - 1; index++) {
            splits.add(new CfSplit(splitTokens.get(index), splitTokens.get(index + 1), splitSize));
        }
        return splits;
    }

    public static void closeQuietly(Client client)
    {
        try {
            TProtocol inputProtocol = client.getInputProtocol();
            if (inputProtocol == null) {
                return;
            }
            TTransport transport = inputProtocol.getTransport();
            if (transport == null) {
                return;
            }
            transport.close();
        }
        catch (Exception ignored) {
        }
    }
}
