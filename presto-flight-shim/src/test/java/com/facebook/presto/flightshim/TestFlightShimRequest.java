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
package com.facebook.presto.flightshim;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Optional;

import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.COLUMN_HANDLE_JSON_CODEC;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.LINESTATUS_COLUMN;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.ORDERKEY_COLUMN;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.TPCH_TABLE;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.createJdbcSplit;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestFlightShimRequest
{
    @Test
    public void testJsonRoundTrip()
    {
        FlightShimRequest expected = createTpchCustomerRequest();
        String json = AbstractTestFlightShimPlugins.REQUEST_JSON_CODEC.toJson(expected);
        FlightShimRequest copy = AbstractTestFlightShimPlugins.REQUEST_JSON_CODEC.fromJson(json);
        assertEquals(copy.getConnectorId(), expected.getConnectorId());
        assertEquals(copy.getSplitBytes(), expected.getSplitBytes());
        assertArrayEquals(copy.getColumnHandlesBytes().toArray(), expected.getColumnHandlesBytes().toArray());
    }

    FlightShimRequest createTpchCustomerRequest()
    {
        String split = createJdbcSplit("postgresql", "tpch", TPCH_TABLE);
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        JdbcColumnHandle custkeyHandle = new JdbcColumnHandle(
                "postgresql",
                ORDERKEY_COLUMN,
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty());
        byte[] custkeyBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(custkeyHandle);

        JdbcColumnHandle nameHandle = new JdbcColumnHandle(
                "postgresql",
                LINESTATUS_COLUMN,
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty());
        byte[] nameBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(nameHandle);

        return new FlightShimRequest(
                "postgresql",
                splitBytes,
                ImmutableList.of(custkeyBytes, nameBytes));
    }
}
