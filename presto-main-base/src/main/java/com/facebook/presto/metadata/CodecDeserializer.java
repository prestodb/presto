package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorId;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class CodecDeserializer<T>
        extends JsonDeserializer<T>
{
    private final Function<String, Class<? extends T>> classResolver;
    private final Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor;
    private final String typePropertyName;
    private final String dataPropertyName;

    public CodecDeserializer(
            String typePropertyName,
            String dataPropertyName,
            Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor,
            Function<String, Class<? extends T>> classResolver)
    {
        this.classResolver = requireNonNull(classResolver, "classResolver is null");
        this.codecExtractor = requireNonNull(codecExtractor, "codecExtractor is null");
        this.typePropertyName = requireNonNull(typePropertyName, "typePropertyName is null");
        this.dataPropertyName = requireNonNull(dataPropertyName, "dataPropertyName is null");
    }

    @Override
    public T deserialize(JsonParser parser, DeserializationContext context)
            throws IOException
    {
        if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
            return null;
        }

        if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT, got " + parser.getCurrentToken());
        }

        // Parse the JSON tree
        TreeNode tree = parser.readValueAsTree();

        if (tree instanceof ObjectNode) {
            ObjectNode node = (ObjectNode) tree;

            // Get the @type field
            if (!node.has(typePropertyName)) {
                throw new IOException("Missing " + typePropertyName + " field");
            }
            String connectorIdString = node.get(typePropertyName).asText();
            // Check if @data field is present (binary serialization)
            if (node.has(dataPropertyName)) {
                // Binary data is present, we need a codec to deserialize it
                // Special handling for internal handles like "$remote"
                if (!connectorIdString.startsWith("$")) {
                    ConnectorId connectorId = new ConnectorId(connectorIdString);
                    Optional<ConnectorCodec<T>> codec = codecExtractor.apply(connectorId);
                    if (codec.isPresent()) {
                        String base64Data = node.get(dataPropertyName).asText();
                        byte[] data = Base64.getDecoder().decode(base64Data);
                        return codec.get().deserialize(data);
                    }
                }
                // @data field present but no codec available or internal handle
                throw new IOException("Type " + connectorIdString + " has binary data (customSerializedValue field) but no codec available to deserialize it");
            }

            // No @data field - use standard JSON deserialization
            Class<? extends T> handleClass = classResolver.apply(connectorIdString);

            // Remove the @type field and deserialize the remaining content
            node.remove(typePropertyName);
            return context.readTreeAsValue(node, handleClass);
        }

        throw new IOException("Unable to deserialize");
    }

    @Override
    public T deserializeWithType(JsonParser p, DeserializationContext ctxt,
            TypeDeserializer typeDeserializer)
            throws IOException
    {
        // We handle the type ourselves
        return deserialize(p, ctxt);
    }

    @Override
    public T deserializeWithType(JsonParser p, DeserializationContext ctxt,
            TypeDeserializer typeDeserializer, T intoValue)
            throws IOException
    {
        // We handle the type ourselves
        return deserialize(p, ctxt);
    }
}
