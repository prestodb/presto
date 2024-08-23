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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_TYPE_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

public class IpAddressDecoder
        implements Decoder
{
    private final String path;
    private final Type ipAddressType;

    public IpAddressDecoder(String path, Type type)
    {
        this.path = requireNonNull(path, "path is null");
        this.ipAddressType = requireNonNull(type, "type is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String) {
            String address = (String) value;
            Slice slice = castToIpAddress(Slices.utf8Slice(address));
            ipAddressType.writeSlice(output, slice);
        }
        else {
            throw new PrestoException(ELASTICSEARCH_TYPE_MISMATCH, format("Expected a string value for field '%s' of type IP: %s [%s]", path, value, value.getClass().getSimpleName()));
        }
    }

    // This is a copy of IpAddressOperators.castFromVarcharToIpAddress method
    private Slice castToIpAddress(Slice slice)
    {
        byte[] address;
        try {
            address = InetAddresses.forString(slice.toStringUtf8()).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS: " + slice.toStringUtf8());
        }

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else if (address.length == 16) {
            bytes = address;
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }
}
