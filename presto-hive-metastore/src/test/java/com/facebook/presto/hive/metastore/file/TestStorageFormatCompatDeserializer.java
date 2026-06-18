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

package com.facebook.presto.hive.metastore.file;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.testng.annotations.Test;

import java.util.Objects;

import static com.facebook.presto.hive.HiveStorageFormat.JSON;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestStorageFormatCompatDeserializer
{
    @Test
    public void testStorageFormatCompatDeserializer()
    {
        JsonCodec<ProtocolV1> codecV1 = JsonCodec.jsonCodec(ProtocolV1.class);
        JsonCodec<ProtocolV2> codecV2 = JsonCodec.jsonCodec(ProtocolV2.class);

        ProtocolV1 v1 = new ProtocolV1(1234, JSON);
        ProtocolV2 v2 = new ProtocolV2(1234, fromHiveStorageFormat(JSON));

        ProtocolV2 v2FromV1 = codecV2.fromJson(codecV1.toJson(v1));
        assertEquals(v2FromV1, v2);
        ProtocolV2 v2FromV2 = codecV2.fromJson(codecV2.toJson(v2));
        assertEquals(v2FromV2, v2);
    }

    public static class ProtocolV1
    {
        private final int id;
        private final HiveStorageFormat format;

        @JsonCreator
        public ProtocolV1(@JsonProperty("id") int id, @JsonProperty("format") HiveStorageFormat format)
        {
            this.id = id;
            this.format = requireNonNull(format, "format is null");
        }

        @JsonProperty
        public int getId()
        {
            return id;
        }

        @JsonProperty
        public HiveStorageFormat getFormat()
        {
            return format;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProtocolV1 that = (ProtocolV1) o;
            return id == that.id && format == that.format;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, format);
        }
    }

    public static class ProtocolV2
    {
        private final int id;
        private final StorageFormat format;

        @JsonCreator
        public ProtocolV2(@JsonProperty("id") int id,
                @JsonDeserialize(using = StorageFormatCompatDeserializer.class)
                @JsonProperty("format") StorageFormat format)
        {
            this.id = id;
            this.format = requireNonNull(format, "format is null");
        }

        @JsonProperty
        public int getId()
        {
            return id;
        }

        @JsonProperty
        public StorageFormat getFormat()
        {
            return format;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProtocolV2 that = (ProtocolV2) o;
            return id == that.id && format.equals(that.format);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, format);
        }
    }
}
