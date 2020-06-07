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
package com.facebook.presto.elasticsearch.decoders;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.elasticsearch.search.SearchHit;

import java.util.Base64;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_TYPE_MISMATCH;

public class VarbinaryDecoder
        implements Decoder
{
    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String) {
            VARBINARY.writeSlice(output, Slices.wrappedBuffer(Base64.getDecoder().decode(value.toString())));
        }
        else {
            throw new PrestoException(ELASTICSEARCH_TYPE_MISMATCH, "Expected a string value for VARCHAR field");
        }
    }
}
