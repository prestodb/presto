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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.plugin.arrow.ArrowFlightConfig;
import com.facebook.plugin.arrow.ArrowTableHandle;
import com.facebook.plugin.arrow.ArrowTableLayoutHandle;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightRequest;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightResponse;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class TestingArrowFlightClientHandler
        extends BaseArrowFlightClientHandler
{
    private final JsonCodec<TestingArrowFlightRequest> requestCodec;
    private final JsonCodec<TestingArrowFlightResponse> responseCodec;
    private boolean caseSensitiveNameMatchingEnabled;

    @Inject
    public TestingArrowFlightClientHandler(
            BufferAllocator allocator,
            ArrowFlightConfig config,
            JsonCodec<TestingArrowFlightRequest> requestCodec,
            JsonCodec<TestingArrowFlightResponse> responseCodec)
    {
        super(allocator, config);
        this.requestCodec = requireNonNull(requestCodec, "requestCodec is null");
        this.responseCodec = requireNonNull(responseCodec, "responseCodec is null");
        this.caseSensitiveNameMatchingEnabled = config.isCaseSensitiveNameMatching();
    }

    @Override
    public CallOption[] getCallOptions(ConnectorSession connectorSession)
    {
        return new CallOption[] {
                new CredentialCallOption(new BearerCredentialWriter(null)),
                CallOptions.timeout(300, TimeUnit.SECONDS)
        };
    }

    @Override
    public FlightDescriptor getFlightDescriptorForSchema(ConnectorSession session, String schemaName, String tableName)
    {
        TestingArrowFlightRequest request = TestingArrowFlightRequest.createDescribeTableRequest(schemaName, tableName);
        return FlightDescriptor.command(requestCodec.toBytes(request));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> res;
        try (FlightClient client = createFlightClient()) {
            List<String> names1 = new ArrayList<>();
            TestingArrowFlightRequest request = TestingArrowFlightRequest.createListSchemaRequest();
            Iterator<Result> iterator = client.doAction(new Action("discovery", requestCodec.toJsonBytes(request)), getCallOptions(session));
            while (iterator.hasNext()) {
                Result result = iterator.next();
                TestingArrowFlightResponse response = responseCodec.fromJson(result.getBody());
                checkArgument(response != null, "response is null");
                checkArgument(response.getSchemaNames() != null, "response.getSchemaNames() is null");
                names1.addAll(response.getSchemaNames());
            }
            res = names1;
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<String> listSchemas = res;
        List<String> names = new ArrayList<>();
        for (String value : listSchemas) {
            names.add(normalizeIdentifier(value));
        }
        return ImmutableList.copyOf(names);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schemaValue = schemaName.orElse("");
        List<String> res;
        try (FlightClient client = createFlightClient()) {
            List<String> names = new ArrayList<>();
            TestingArrowFlightRequest request = TestingArrowFlightRequest.createListTablesRequest(schemaName.orElse(""));
            Iterator<Result> iterator = client.doAction(new Action("discovery", requestCodec.toJsonBytes(request)), getCallOptions(session));
            while (iterator.hasNext()) {
                Result result = iterator.next();
                TestingArrowFlightResponse response = responseCodec.fromJson(result.getBody());
                checkArgument(response != null, "response is null");
                checkArgument(response.getTableNames() != null, "response.getTableNames() is null");
                names.addAll(response.getTableNames());
            }
            res = names;
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<String> listTables = res;
        List<SchemaTableName> tables = new ArrayList<>();
        for (String value : listTables) {
            tables.add(new SchemaTableName(normalizeIdentifier(schemaValue), normalizeIdentifier(value)));
        }

        return tables;
    }

    @Override
    public FlightDescriptor getFlightDescriptorForTableScan(ConnectorSession session, ArrowTableLayoutHandle tableLayoutHandle)
    {
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        String query = new TestingArrowQueryBuilder().buildSql(
                tableHandle.getSchema(),
                tableHandle.getTable(),
                tableLayoutHandle.getColumnHandles(), ImmutableMap.of(),
                tableLayoutHandle.getTupleDomain());
        TestingArrowFlightRequest request = TestingArrowFlightRequest.createQueryRequest(tableHandle.getSchema(), tableHandle.getTable(), query);
        return FlightDescriptor.command(requestCodec.toBytes(request));
    }

    private String normalizeIdentifier(String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ROOT);
    }
}
