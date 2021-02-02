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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;

import java.util.Optional;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;

public interface LongOutputStream
        extends ValueOutputStream<LongStreamCheckpoint>
{
    static LongOutputStream createLengthOutputStream(CompressionParameters compressionParameters, Optional<DwrfDataEncryptor> dwrfEncryptor, OrcEncoding orcEncoding)
    {
        if (orcEncoding == DWRF) {
            return new LongOutputStreamV1(compressionParameters, dwrfEncryptor, false, LENGTH);
        }
        else {
            return new LongOutputStreamV2(compressionParameters, false, LENGTH);
        }
    }

    void writeLong(long value);
}
