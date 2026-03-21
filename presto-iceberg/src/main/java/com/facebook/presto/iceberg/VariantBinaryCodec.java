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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Encoder/decoder for the Apache Variant binary format as used by Iceberg V3.
 *
 * <p>The Variant binary format encodes semi-structured (JSON-like) data in a compact
 * binary representation with two components:
 * <ul>
 *   <li><b>Metadata</b>: A dictionary of field names (keys) used in objects</li>
 *   <li><b>Value</b>: The encoded data using type-tagged values</li>
 * </ul>
 *
 * <p>This codec supports encoding JSON strings to Variant binary and decoding
 * Variant binary back to JSON strings. It implements the Apache Variant spec
 * (version 1) covering:
 * <ul>
 *   <li>Primitives: null, boolean, int8/16/32/64, float, double, string</li>
 *   <li>Short strings (0-63 bytes, inlined in header)</li>
 *   <li>Objects (key-value maps with metadata dictionary references)</li>
 *   <li>Arrays (ordered value lists)</li>
 * </ul>
 *
 * @see <a href="https://iceberg.apache.org/spec/#variant">Iceberg V3 Variant Spec</a>
 */
public final class VariantBinaryCodec
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    // Basic type codes (bits 7-6 of header byte)
    static final int BASIC_TYPE_PRIMITIVE = 0;
    static final int BASIC_TYPE_SHORT_STRING = 1;
    static final int BASIC_TYPE_OBJECT = 2;
    static final int BASIC_TYPE_ARRAY = 3;

    // Primitive type_info values (bits 5-0 when basic_type=0)
    static final int PRIMITIVE_NULL = 0;
    static final int PRIMITIVE_TRUE = 1;
    static final int PRIMITIVE_FALSE = 2;
    static final int PRIMITIVE_INT8 = 5;
    static final int PRIMITIVE_INT16 = 6;
    static final int PRIMITIVE_INT32 = 7;
    static final int PRIMITIVE_INT64 = 8;
    static final int PRIMITIVE_FLOAT = 9;
    static final int PRIMITIVE_DOUBLE = 10;
    static final int PRIMITIVE_STRING = 19;

    // Metadata format version
    static final int METADATA_VERSION = 1;

    // Maximum short string length (6 bits = 63)
    static final int MAX_SHORT_STRING_LENGTH = 63;

    private VariantBinaryCodec() {}

    /**
     * Holds the two components of a Variant binary encoding.
     */
    public static final class VariantBinary
    {
        private final byte[] metadata;
        private final byte[] value;

        public VariantBinary(byte[] metadata, byte[] value)
        {
            this.metadata = metadata;
            this.value = value;
        }

        public byte[] getMetadata()
        {
            return metadata;
        }

        public byte[] getValue()
        {
            return value;
        }
    }

    /**
     * Encodes a JSON string into Variant binary format.
     *
     * @param json a valid JSON string
     * @return the Variant binary encoding (metadata + value)
     * @throws IllegalArgumentException if the JSON is malformed
     */
    public static VariantBinary fromJson(String json)
    {
        try {
            MetadataBuilder metadataBuilder = new MetadataBuilder();

            // First pass: collect all object keys into the metadata dictionary
            try (JsonParser parser = JSON_FACTORY.createParser(json)) {
                collectKeys(parser, metadataBuilder);
            }

            // Build the metadata dictionary
            byte[] metadata = metadataBuilder.build();
            Map<String, Integer> keyIndex = metadataBuilder.getKeyIndex();

            // Second pass: encode the value
            try (JsonParser parser = JSON_FACTORY.createParser(json)) {
                parser.nextToken();
                byte[] value = encodeValue(parser, keyIndex);
                return new VariantBinary(metadata, value);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to encode JSON to Variant binary: " + json, e);
        }
    }

    /**
     * Decodes Variant binary (metadata + value) back to a JSON string.
     *
     * @param metadata the metadata dictionary bytes
     * @param value the encoded value bytes
     * @return the JSON string representation
     */
    public static String toJson(byte[] metadata, byte[] value)
    {
        try {
            String[] dictionary = decodeMetadata(metadata);
            StringWriter writer = new StringWriter();
            try (JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                decodeValue(value, 0, dictionary, gen);
            }
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to decode Variant binary to JSON", e);
        }
    }

    // ---- Metadata encoding/decoding ----

    /**
     * Builds the metadata dictionary (sorted key names with byte offsets).
     */
    static final class MetadataBuilder
    {
        private final TreeMap<String, Integer> keys = new TreeMap<>();

        void addKey(String key)
        {
            if (!keys.containsKey(key)) {
                keys.put(key, keys.size());
            }
        }

        Map<String, Integer> getKeyIndex()
        {
            Map<String, Integer> index = new LinkedHashMap<>();
            int i = 0;
            for (String key : keys.keySet()) {
                index.put(key, i++);
            }
            return index;
        }

        byte[] build()
        {
            List<byte[]> keyBytes = new ArrayList<>();
            for (String key : keys.keySet()) {
                keyBytes.add(key.getBytes(StandardCharsets.UTF_8));
            }

            int numKeys = keyBytes.size();

            // Calculate total key data size
            int keyDataSize = 0;
            for (byte[] kb : keyBytes) {
                keyDataSize += kb.length;
            }

            // Metadata format:
            // [1 byte] version
            // [4 bytes] numKeys (uint32 LE)
            // [4 bytes * numKeys] byte offsets to each key
            // [keyDataSize bytes] concatenated key strings
            int totalSize = 1 + 4 + (4 * numKeys) + keyDataSize;
            ByteBuffer buf = ByteBuffer.allocate(totalSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            buf.put((byte) METADATA_VERSION);
            buf.putInt(numKeys);

            // Write offsets
            int offset = 0;
            for (byte[] kb : keyBytes) {
                buf.putInt(offset);
                offset += kb.length;
            }

            // Write key strings
            for (byte[] kb : keyBytes) {
                buf.put(kb);
            }

            return buf.array();
        }
    }

    /**
     * Decodes the metadata dictionary from binary.
     */
    static String[] decodeMetadata(byte[] metadata)
    {
        if (metadata == null || metadata.length == 0) {
            return new String[0];
        }

        ByteBuffer buf = ByteBuffer.wrap(metadata);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        int version = buf.get() & 0xFF;
        if (version != METADATA_VERSION) {
            throw new IllegalArgumentException("Unsupported Variant metadata version: " + version);
        }

        int numKeys = buf.getInt();
        if (numKeys == 0) {
            return new String[0];
        }

        int[] offsets = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            offsets[i] = buf.getInt();
        }

        int keyDataStart = buf.position();
        int keyDataEnd = metadata.length;

        String[] keys = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            int start = keyDataStart + offsets[i];
            int end = (i + 1 < numKeys) ? keyDataStart + offsets[i + 1] : keyDataEnd;
            keys[i] = new String(metadata, start, end - start, StandardCharsets.UTF_8);
        }

        return keys;
    }

    // ---- Value encoding ----

    private static void collectKeys(JsonParser parser, MetadataBuilder metadataBuilder) throws IOException
    {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME) {
                metadataBuilder.addKey(parser.getCurrentName());
            }
        }
    }

    private static byte[] encodeValue(JsonParser parser, Map<String, Integer> keyIndex) throws IOException
    {
        JsonToken token = parser.currentToken();
        if (token == null) {
            return encodePrimitive(PRIMITIVE_NULL);
        }

        switch (token) {
            case VALUE_NULL:
                return encodePrimitive(PRIMITIVE_NULL);
            case VALUE_TRUE:
                return encodePrimitive(PRIMITIVE_TRUE);
            case VALUE_FALSE:
                return encodePrimitive(PRIMITIVE_FALSE);
            case VALUE_NUMBER_INT:
                return encodeInteger(parser.getLongValue());
            case VALUE_NUMBER_FLOAT:
                return encodeDouble(parser.getDoubleValue());
            case VALUE_STRING:
                return encodeString(parser.getText());
            case START_OBJECT:
                return encodeObject(parser, keyIndex);
            case START_ARRAY:
                return encodeArray(parser, keyIndex);
            default:
                throw new IllegalArgumentException("Unexpected JSON token: " + token);
        }
    }

    private static byte[] encodePrimitive(int typeInfo)
    {
        return new byte[] {makeHeader(BASIC_TYPE_PRIMITIVE, typeInfo)};
    }

    private static byte[] encodeInteger(long value)
    {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return new byte[] {makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_INT8), (byte) value};
        }
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            ByteBuffer buf = ByteBuffer.allocate(3);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.put(makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_INT16));
            buf.putShort((short) value);
            return buf.array();
        }
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            ByteBuffer buf = ByteBuffer.allocate(5);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.put(makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_INT32));
            buf.putInt((int) value);
            return buf.array();
        }
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.put(makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_INT64));
        buf.putLong(value);
        return buf.array();
    }

    private static byte[] encodeDouble(double value)
    {
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.put(makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_DOUBLE));
        buf.putDouble(value);
        return buf.array();
    }

    private static byte[] encodeString(String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_SHORT_STRING_LENGTH) {
            byte[] result = new byte[1 + bytes.length];
            result[0] = makeHeader(BASIC_TYPE_SHORT_STRING, bytes.length);
            System.arraycopy(bytes, 0, result, 1, bytes.length);
            return result;
        }

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + bytes.length);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.put(makeHeader(BASIC_TYPE_PRIMITIVE, PRIMITIVE_STRING));
        buf.putInt(bytes.length);
        buf.put(bytes);
        return buf.array();
    }

    private static byte[] encodeObject(JsonParser parser, Map<String, Integer> keyIndex) throws IOException
    {
        List<Integer> fieldKeyIds = new ArrayList<>();
        List<byte[]> fieldValues = new ArrayList<>();

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.getCurrentName();
            parser.nextToken();

            Integer keyId = keyIndex.get(fieldName);
            if (keyId == null) {
                throw new IllegalStateException("Key not found in metadata dictionary: " + fieldName);
            }

            fieldKeyIds.add(keyId);
            fieldValues.add(encodeValue(parser, keyIndex));
        }

        int numFields = fieldKeyIds.size();

        // Determine offset size needed (1, 2, or 4 bytes)
        int totalValueSize = 0;
        for (byte[] fv : fieldValues) {
            totalValueSize += fv.length;
        }

        int offsetSize = getOffsetSize(totalValueSize);
        int offsetSizeBits = offsetSizeToBits(offsetSize);

        // Object binary format:
        // [1 byte] header (basic_type=2, type_info encodes offset size + field_id size)
        // [4 bytes] numFields (uint32 LE)
        // [field_id_size * numFields] field key IDs
        // [offsetSize * numFields] offsets to field values (relative to start of value data)
        // [totalValueSize bytes] concatenated field values
        int fieldIdSize = getFieldIdSize(keyIndex.size());
        int fieldIdSizeBits = offsetSizeToBits(fieldIdSize);

        // type_info encodes: bits 0-1 = value_offset_size_minus_1, bits 2-3 = field_id_size_minus_1
        int typeInfo = (offsetSizeBits & 0x03) | ((fieldIdSizeBits & 0x03) << 2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(makeHeader(BASIC_TYPE_OBJECT, typeInfo));
        writeLittleEndianInt(out, numFields);

        // Write field key IDs
        for (int keyId : fieldKeyIds) {
            writeLittleEndianN(out, keyId, fieldIdSize);
        }

        // Write field value offsets
        int offset = 0;
        for (byte[] fv : fieldValues) {
            writeLittleEndianN(out, offset, offsetSize);
            offset += fv.length;
        }

        // Write field values
        for (byte[] fv : fieldValues) {
            out.write(fv);
        }

        return out.toByteArray();
    }

    private static byte[] encodeArray(JsonParser parser, Map<String, Integer> keyIndex) throws IOException
    {
        List<byte[]> elements = new ArrayList<>();

        while (parser.nextToken() != JsonToken.END_ARRAY) {
            elements.add(encodeValue(parser, keyIndex));
        }

        int numElements = elements.size();

        int totalValueSize = 0;
        for (byte[] el : elements) {
            totalValueSize += el.length;
        }

        int offsetSize = getOffsetSize(totalValueSize);
        int offsetSizeBits = offsetSizeToBits(offsetSize);

        // type_info encodes: bits 0-1 = offset_size_minus_1
        int typeInfo = offsetSizeBits & 0x03;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(makeHeader(BASIC_TYPE_ARRAY, typeInfo));
        writeLittleEndianInt(out, numElements);

        // Write element offsets
        int offset = 0;
        for (byte[] el : elements) {
            writeLittleEndianN(out, offset, offsetSize);
            offset += el.length;
        }

        // Write element values
        for (byte[] el : elements) {
            out.write(el);
        }

        return out.toByteArray();
    }

    // ---- Value decoding ----

    private static void decodeValue(byte[] data, int pos, String[] dictionary, JsonGenerator gen) throws IOException
    {
        if (pos >= data.length) {
            gen.writeNull();
            return;
        }

        int header = data[pos] & 0xFF;
        int basicType = header >> 6;
        int typeInfo = header & 0x3F;

        switch (basicType) {
            case BASIC_TYPE_PRIMITIVE:
                decodePrimitive(data, pos, typeInfo, gen);
                break;
            case BASIC_TYPE_SHORT_STRING:
                decodeShortString(data, pos, typeInfo, gen);
                break;
            case BASIC_TYPE_OBJECT:
                decodeObject(data, pos, typeInfo, dictionary, gen);
                break;
            case BASIC_TYPE_ARRAY:
                decodeArray(data, pos, typeInfo, dictionary, gen);
                break;
            default:
                throw new IllegalArgumentException("Unknown Variant basic type: " + basicType);
        }
    }

    private static void decodePrimitive(byte[] data, int pos, int typeInfo, JsonGenerator gen) throws IOException
    {
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        switch (typeInfo) {
            case PRIMITIVE_NULL:
                gen.writeNull();
                break;
            case PRIMITIVE_TRUE:
                gen.writeBoolean(true);
                break;
            case PRIMITIVE_FALSE:
                gen.writeBoolean(false);
                break;
            case PRIMITIVE_INT8:
                gen.writeNumber(data[pos + 1]);
                break;
            case PRIMITIVE_INT16:
                buf.position(pos + 1);
                gen.writeNumber(buf.getShort());
                break;
            case PRIMITIVE_INT32:
                buf.position(pos + 1);
                gen.writeNumber(buf.getInt());
                break;
            case PRIMITIVE_INT64:
                buf.position(pos + 1);
                gen.writeNumber(buf.getLong());
                break;
            case PRIMITIVE_FLOAT:
                buf.position(pos + 1);
                gen.writeNumber(buf.getFloat());
                break;
            case PRIMITIVE_DOUBLE:
                buf.position(pos + 1);
                gen.writeNumber(buf.getDouble());
                break;
            case PRIMITIVE_STRING: {
                buf.position(pos + 1);
                int len = buf.getInt();
                String str = new String(data, pos + 5, len, StandardCharsets.UTF_8);
                gen.writeString(str);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown Variant primitive type_info: " + typeInfo);
        }
    }

    private static void decodeShortString(byte[] data, int pos, int typeInfo, JsonGenerator gen) throws IOException
    {
        int length = typeInfo;
        String str = new String(data, pos + 1, length, StandardCharsets.UTF_8);
        gen.writeString(str);
    }

    private static void decodeObject(byte[] data, int pos, int typeInfo, String[] dictionary, JsonGenerator gen) throws IOException
    {
        int offsetSize = (typeInfo & 0x03) + 1;
        int fieldIdSize = ((typeInfo >> 2) & 0x03) + 1;

        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.position(pos + 1);

        int numFields = buf.getInt();

        int[] keyIds = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            keyIds[i] = readLittleEndianN(data, buf.position(), fieldIdSize);
            buf.position(buf.position() + fieldIdSize);
        }

        int[] offsets = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            offsets[i] = readLittleEndianN(data, buf.position(), offsetSize);
            buf.position(buf.position() + offsetSize);
        }

        int valueDataStart = buf.position();

        gen.writeStartObject();
        for (int i = 0; i < numFields; i++) {
            String key = dictionary[keyIds[i]];
            gen.writeFieldName(key);
            decodeValue(data, valueDataStart + offsets[i], dictionary, gen);
        }
        gen.writeEndObject();
    }

    private static void decodeArray(byte[] data, int pos, int typeInfo, String[] dictionary, JsonGenerator gen) throws IOException
    {
        int offsetSize = (typeInfo & 0x03) + 1;

        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.position(pos + 1);

        int numElements = buf.getInt();

        int[] offsets = new int[numElements];
        for (int i = 0; i < numElements; i++) {
            offsets[i] = readLittleEndianN(data, buf.position(), offsetSize);
            buf.position(buf.position() + offsetSize);
        }

        int valueDataStart = buf.position();

        gen.writeStartArray();
        for (int i = 0; i < numElements; i++) {
            decodeValue(data, valueDataStart + offsets[i], dictionary, gen);
        }
        gen.writeEndArray();
    }

    // ---- Helper methods ----

    static byte makeHeader(int basicType, int typeInfo)
    {
        return (byte) ((basicType << 6) | (typeInfo & 0x3F));
    }

    private static int getOffsetSize(int maxOffset)
    {
        if (maxOffset <= 0xFF) {
            return 1;
        }
        if (maxOffset <= 0xFFFF) {
            return 2;
        }
        return 4;
    }

    private static int getFieldIdSize(int numKeys)
    {
        if (numKeys <= 0xFF) {
            return 1;
        }
        if (numKeys <= 0xFFFF) {
            return 2;
        }
        return 4;
    }

    private static int offsetSizeToBits(int offsetSize)
    {
        switch (offsetSize) {
            case 1: return 0;
            case 2: return 1;
            case 4: return 3;
            default: throw new IllegalArgumentException("Invalid offset size: " + offsetSize);
        }
    }

    private static void writeLittleEndianInt(ByteArrayOutputStream out, int value)
    {
        out.write(value & 0xFF);
        out.write((value >> 8) & 0xFF);
        out.write((value >> 16) & 0xFF);
        out.write((value >> 24) & 0xFF);
    }

    private static void writeLittleEndianN(ByteArrayOutputStream out, int value, int size)
    {
        for (int i = 0; i < size; i++) {
            out.write((value >> (i * 8)) & 0xFF);
        }
    }

    private static int readLittleEndianN(byte[] data, int pos, int size)
    {
        int value = 0;
        for (int i = 0; i < size; i++) {
            value |= (data[pos + i] & 0xFF) << (i * 8);
        }
        return value;
    }

    // ---- Phase 2: Binary format detection and auto-decode ----

    /**
     * Checks if the given metadata and value byte arrays form a valid Variant binary encoding.
     * Validates the metadata version byte and value header basic type.
     *
     * @param metadata the metadata dictionary bytes
     * @param value the encoded value bytes
     * @return true if the data is valid Variant binary format
     */
    public static boolean isVariantBinary(byte[] metadata, byte[] value)
    {
        if (metadata == null || metadata.length < 5 || value == null || value.length == 0) {
            return false;
        }
        int version = metadata[0] & 0xFF;
        if (version != METADATA_VERSION) {
            return false;
        }
        int header = value[0] & 0xFF;
        int basicType = header >> 6;
        return basicType >= BASIC_TYPE_PRIMITIVE && basicType <= BASIC_TYPE_ARRAY;
    }

    /**
     * Returns the Variant type name from a binary value header byte.
     * Used for type introspection of Variant binary data.
     *
     * @param value the encoded value bytes
     * @return type name: "null", "boolean", "integer", "float", "double", "string", "object", "array"
     */
    public static String getValueTypeName(byte[] value)
    {
        if (value == null || value.length == 0) {
            return "null";
        }

        int header = value[0] & 0xFF;
        int basicType = header >> 6;
        int typeInfo = header & 0x3F;

        switch (basicType) {
            case BASIC_TYPE_PRIMITIVE:
                switch (typeInfo) {
                    case PRIMITIVE_NULL: return "null";
                    case PRIMITIVE_TRUE:
                    case PRIMITIVE_FALSE: return "boolean";
                    case PRIMITIVE_INT8:
                    case PRIMITIVE_INT16:
                    case PRIMITIVE_INT32:
                    case PRIMITIVE_INT64:
                        return "integer";
                    case PRIMITIVE_FLOAT: return "float";
                    case PRIMITIVE_DOUBLE: return "double";
                    case PRIMITIVE_STRING: return "string";
                    default: return "unknown";
                }
            case BASIC_TYPE_SHORT_STRING: return "string";
            case BASIC_TYPE_OBJECT: return "object";
            case BASIC_TYPE_ARRAY: return "array";
            default: return "unknown";
        }
    }

    /**
     * Attempts to decode raw bytes as Variant data, handling both JSON text and binary format.
     * If the data starts with a valid JSON character ({, [, ", t, f, n, digit, -),
     * it's treated as UTF-8 JSON text. Otherwise, it's treated as binary Variant value
     * with empty metadata (suitable for primitives and strings).
     *
     * <p>For full binary Variant decoding with metadata dictionary support,
     * use {@link #toJson(byte[], byte[])} directly with separate metadata and value arrays.
     *
     * @param data raw bytes that may be JSON or binary Variant
     * @return JSON string representation
     */
    public static String decodeVariantAuto(byte[] data)
    {
        if (data == null || data.length == 0) {
            return "null";
        }
        byte first = data[0];
        if (first == '{' || first == '[' || first == '"' || first == 't' ||
                first == 'f' || first == 'n' || (first >= '0' && first <= '9') || first == '-' || first == ' ') {
            return new String(data, StandardCharsets.UTF_8);
        }
        // Try binary Variant decode with empty metadata
        try {
            byte[] emptyMetadata = new MetadataBuilder().build();
            return toJson(emptyMetadata, data);
        }
        catch (Exception e) {
            return new String(data, StandardCharsets.UTF_8);
        }
    }
}
