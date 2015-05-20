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
package com.facebook.presto.decoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementations decode a row from bytes and add field value providers for all decodable columns.
 */
public interface RowDecoder
{
    /**
     * Returns the row decoder specific name.
     */
    String getName();

    /**
     * Decodes a given set of bytes into field values.
     *
     * @param data The row data to decode.
     * @param dataMap The row  data with as fields map
     * @param fieldValueProviders Must be a mutable set. Any field value provider created by this row decoder is put into this set.
     * @param columnHandles List of column handles for which field values are required.
     * @param fieldDecoders Map from column handles to decoders. This map should be used to look up the field decoder that generates the field value provider for a given column handle.    @return false if the row was decoded successfully, true if it could not be decoded (was corrupt). TODO - reverse this boolean.
     */
    boolean decodeRow(
            byte[] data,
            Map<String, String> dataMap,
            Set<FieldValueProvider> fieldValueProviders,
            List<DecoderColumnHandle> columnHandles,
            Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders);
}
