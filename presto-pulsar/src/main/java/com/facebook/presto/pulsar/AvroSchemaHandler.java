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
package com.facebook.presto.pulsar;

import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.List;

public class AvroSchemaHandler implements SchemaHandler {

    private final DatumReader<GenericRecord> datumReader;

    private final List<PulsarColumnHandle> columnHandles;

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    public AvroSchemaHandler(Schema schema, List<PulsarColumnHandle> columnHandles) {
        this.datumReader = new GenericDatumReader<>(schema);
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try {
            return this.datumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
            GenericRecord record = (GenericRecord) currentRecord;
            return record.get(this.columnHandles.get(index).getPositionIndex());
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }
}
