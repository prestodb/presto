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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.StripeInformation;

public interface WriterStats
{
    enum FlushReason
    {
        MAX_ROWS, MAX_BYTES, DICTIONARY_FULL, CLOSED
    }

    void recordStripeWritten(
            int stripeMinBytes,
            int stripeMaxBytes,
            int dictionaryMaxMemoryBytes,
            FlushReason flushReason,
            int dictionaryBytes,
            StripeInformation stripeInformation);

    void updateSizeInBytes(long deltaInBytes);
}
