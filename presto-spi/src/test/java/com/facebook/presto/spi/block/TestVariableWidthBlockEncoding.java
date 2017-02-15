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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestVariableWidthBlockEncoding
{
    private static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "test_query_id";
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return new ConnectorIdentity("user", Optional.empty(), Optional.empty());
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public boolean isLegacyTimestamp()
        {
            return false;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
    };

    @Test
    public void testRoundTrip()
    {
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 4);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new VariableWidthBlockEncoding();
        blockEncoding.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
