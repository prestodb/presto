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
package com.facebook.presto.spiller;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.TestingPagesSerdeFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.google.common.collect.ImmutableList;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.page.PageCodecMarker.ENCRYPTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestSpillCipherPagesSerde
{
    @Test
    public void test()
    {
        SpillCipher cipher = new AesSpillCipher();
        PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerdeForSpill(Optional.of(cipher));
        List<Type> types = ImmutableList.of(VARCHAR);
        Page emptyPage = new Page(VARCHAR.createBlockBuilder(null, 0).build());
        assertPageEquals(types, serde.deserialize(serde.serialize(emptyPage)), emptyPage);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeString(blockBuilder, "hello");
        VARCHAR.writeString(blockBuilder, "world");
        Page helloWorldPage = new Page(blockBuilder.build());

        SerializedPage serialized = serde.serialize(helloWorldPage);
        assertPageEquals(types, serde.deserialize(serialized), helloWorldPage);
        assertTrue(ENCRYPTED.isSet(serialized.getPageCodecMarkers()), "page should be encrypted");

        cipher.destroy();

        assertFailure(() -> serde.serialize(helloWorldPage), "Spill cipher already destroyed");
        assertFailure(() -> serde.deserialize(serialized), "Spill cipher already destroyed");
    }

    private static void assertFailure(ThrowingRunnable runnable, String expectedErrorMessage)
    {
        PrestoException exception = expectThrows(PrestoException.class, runnable);
        assertEquals(exception.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(exception.getMessage(), expectedErrorMessage);
    }
}
