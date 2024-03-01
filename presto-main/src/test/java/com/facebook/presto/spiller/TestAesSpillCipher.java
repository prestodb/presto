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

import com.facebook.presto.spi.PrestoException;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestAesSpillCipher
{
    @Test
    public void test()
    {
        AesSpillCipher spillCipher = new AesSpillCipher();
        // test [0, 257] buffer sizes to check off all padding cases
        for (int size = 0; size <= 257; size++) {
            byte[] data = randomBytes(size);
            // .clone() to prevent cipher from modifying the content we assert against
            assertEquals(data, spillCipher.decrypt(spillCipher.encrypt(data.clone())));
            assertEquals(ByteBuffer.wrap(data), spillCipher.decrypt(spillCipher.encrypt(ByteBuffer.wrap(data.clone()))));
        }
        // verify that initialization vector is not re-used
        assertNotEquals(spillCipher.encrypt(new byte[0]), spillCipher.encrypt(new byte[0]), "IV values must not be reused");

        // cleanup
        assertFalse(spillCipher.isDestroyed());
        byte[] encrypted = spillCipher.encrypt(randomBytes(1));
        spillCipher.destroy();
        assertTrue(spillCipher.isDestroyed());

        spillCipher.destroy(); // should not throw an exception

        assertFailure(() -> spillCipher.decrypt(encrypted), "Spill cipher already destroyed");
        assertFailure(() -> spillCipher.encrypt(randomBytes(1)), "Spill cipher already destroyed");
    }

    private static void assertFailure(ThrowingRunnable runnable, String expectedErrorMessage)
    {
        PrestoException exception = expectThrows(PrestoException.class, runnable);
        assertEquals(exception.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(exception.getMessage(), expectedErrorMessage);
    }

    private static byte[] randomBytes(int size)
    {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
