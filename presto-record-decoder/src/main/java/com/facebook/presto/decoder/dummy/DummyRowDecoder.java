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
package com.facebook.presto.decoder.dummy;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The row decoder for the 'dummy' format. For an unstructured row format (bag
 * of bytes), a specific decoder for a topic must exist. To start developing
 * such a decoder, it is beneficial to be able to configure an arbitrary topic
 * to be available through presto without any decoding at all (not even line
 * parsing) and examine the internal rows that are exposed by Presto. By adding
 * a table name to the catalog configuration file and not having and specific
 * topic description JSON file (or by omitting the 'dataFormat' field in the
 * table description file), this decoder should be selected by the appropriate
 * connector.
 */
public class DummyRowDecoder
        implements RowDecoder
{
    public static final String NAME = "dummy";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data,
            Map<String, String> dataMap,
            Set<FieldValueProvider> fieldValueProviders,
            List<DecoderColumnHandle> columnHandles,
            Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        return false;
    }
}
