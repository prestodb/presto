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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.CORRUPT_PAGE;

public enum PageEncryption
{
    UNENCRYPTED((byte) 0),
    ENCRYPTED((byte) 1);

    private final byte marker;

    PageEncryption(byte marker)
    {
        this.marker = marker;
    }

    public byte getMarker()
    {
        return marker;
    }

    public static PageEncryption lookupEncryptionCodecFromMarker(byte marker)
    {
        if (marker != UNENCRYPTED.getMarker() && marker != ENCRYPTED.getMarker()) {
            throw new PrestoException(CORRUPT_PAGE, "Page marker did not contain expected value");
        }
        return UNENCRYPTED.getMarker() == marker ? UNENCRYPTED : ENCRYPTED;
    }
}
