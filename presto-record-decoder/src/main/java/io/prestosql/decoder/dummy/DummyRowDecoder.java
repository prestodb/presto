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
package io.prestosql.decoder.dummy;

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;

import java.util.Map;
import java.util.Optional;

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
    private static final Optional<Map<DecoderColumnHandle, FieldValueProvider>> ALL_NULLS = Optional.of(ImmutableMap.of());

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data,
            Map<String, String> dataMap)
    {
        return ALL_NULLS;
    }
}
