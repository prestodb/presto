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
package com.facebook.presto.kinesis.decoder.dummy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.facebook.presto.kinesis.decoder.KinesisRowDecoder;

public class DummyKinesisRowDecoder
        implements KinesisRowDecoder
{
    public static final String NAME = "dummy";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data,
            Set<KinesisFieldValueProvider> fieldValueProviders,
            List<KinesisColumnHandle> columnHandles,
            Map<KinesisColumnHandle, KinesisFieldDecoder<?>> fieldDecoders)
    {
        return true;
    }
}
