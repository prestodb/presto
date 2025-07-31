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
package com.facebook.presto.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.connector.ConnectorCommitHandle.EMPTY_COMMIT_OUTPUT;
import static org.testng.Assert.assertEquals;

public class TestOutput
{
    private static final JsonCodec<Output> codec = JsonCodec.jsonCodec(Output.class);

    @Test
    public void testRoundTrip()
    {
        Output expected = new Output(
                new ConnectorId("connectorId"),
                "schema",
                "table",
                EMPTY_COMMIT_OUTPUT,
                Optional.of(
                        ImmutableList.of(
                                new OutputColumnMetadata(
                                        "column", "type",
                                        ImmutableSet.of(
                                                new SourceColumn(QualifiedObjectName.valueOf("catalog.schema.table"), "column"))))));

        String json = codec.toJson(expected);
        Output actual = codec.fromJson(json);

        assertEquals(actual, expected);
    }
}
