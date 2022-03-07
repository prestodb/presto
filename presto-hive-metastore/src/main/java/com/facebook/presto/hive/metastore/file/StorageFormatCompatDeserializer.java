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

import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;

public class StorageFormatCompatDeserializer
        extends JsonDeserializer<StorageFormat>
{
    @Override
    public StorageFormat deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        // Prior to version 0.271, HiveStorageFormat was used for storage format;
        // this deserializer is to ensure backward compatibility
        if (p.currentToken() == VALUE_STRING) {
            HiveStorageFormat format = p.readValueAs(HiveStorageFormat.class);
            return fromHiveStorageFormat(format);
        }
        return p.readValueAs(StorageFormat.class);
    }
}
