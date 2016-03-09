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
package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class SessionPropertyManager
{
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    private final ConcurrentMap<String, SessionProperty<?>> allSessionProperties = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<PropertyMetadata<?>>> catalogSessionProperties = new ConcurrentHashMap<>();

    public SessionPropertyManager()
    {
        this(new SystemSessionProperties());
    }

    @Inject
    public SessionPropertyManager(SystemSessionProperties systemSessionProperties)
    {
        this(systemSessionProperties.getSessionProperties());
    }

    public SessionPropertyManager(List<PropertyMetadata<?>> systemSessionProperties)
    {
        addSystemSessionProperties(systemSessionProperties);
    }

    public void addSystemSessionProperties(List<PropertyMetadata<?>> systemSessionProperties)
    {
        systemSessionProperties
                .forEach(this::addSystemSessionProperty);
    }

    public void addSystemSessionProperty(PropertyMetadata<?> sessionProperty)
    {
        requireNonNull(sessionProperty, "sessionProperty is null");
        addSessionPropertyInternal(Optional.empty(), sessionProperty);
    }

    public void addConnectorSessionProperties(String catalog, List<PropertyMetadata<?>> sessionProperties)
    {
        requireNonNull(catalog, "catalog is null");
        checkArgument(!catalog.isEmpty() && catalog.trim().toLowerCase(ENGLISH).equals(catalog), "Invalid catalog name '%s'", catalog);
        requireNonNull(sessionProperties, "sessionProperties is null");

        checkState(catalogSessionProperties.putIfAbsent(catalog, sessionProperties) == null, "SessionProperties for catalog '%s' are already registered", catalog);
        for (PropertyMetadata<?> sessionProperty : sessionProperties) {
            addSessionPropertyInternal(Optional.of(catalog), sessionProperty);
        }
    }

    private <T> void addSessionPropertyInternal(Optional<String> catalogName, PropertyMetadata<T> sessionProperty)
    {
        SessionProperty<T> value = new SessionProperty<>(catalogName, sessionProperty);
        allSessionProperties.put(value.getFullyQualifiedName(), value);
    }

    public PropertyMetadata<?> getSessionPropertyMetadata(String name)
    {
        requireNonNull(name, "name is null");

        SessionProperty<?> sessionProperty = allSessionProperties.get(name);
        if (sessionProperty == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
        return sessionProperty.getMetadata();
    }

    public List<SessionPropertyValue> getAllSessionProperties(Session session)
    {
        requireNonNull(session, "session is null");

        List<SessionPropertyValue> properties = new ArrayList<>();
        for (SessionProperty<?> sessionProperty : allSessionProperties.values()) {
            PropertyMetadata<?> propertyMetadata = sessionProperty.getMetadata();
            String defaultValue = firstNonNull(propertyMetadata.getDefaultValue(), "").toString();

            Map<String, String> values;
            if (sessionProperty.getCatalogName().isPresent()) {
                values = session.getCatalogProperties(sessionProperty.getCatalogName().get());
            }
            else {
                values = session.getSystemProperties();
            }
            String value = values.getOrDefault(sessionProperty.getPropertyName(), defaultValue);

            properties.add(new SessionPropertyValue(
                    value,
                    defaultValue,
                    sessionProperty.getFullyQualifiedName(),
                    sessionProperty.getCatalogName(),
                    sessionProperty.getPropertyName(),
                    propertyMetadata.getDescription(),
                    propertyMetadata.getSqlType().getDisplayName(),
                    propertyMetadata.isHidden()));
        }

        // sort properties by catalog then property
        Collections.sort(properties, (left, right) -> ComparisonChain.start()
                .compare(left.getCatalogName().orElse(null), right.getCatalogName().orElse(null), Ordering.natural().nullsFirst())
                .compare(left.getPropertyName(), right.getPropertyName())
                .result());

        return ImmutableList.copyOf(properties);
    }

    public <T> T decodeProperty(String name, @Nullable String value, Class<T> type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        SessionProperty<?> sessionProperty = allSessionProperties.get(name);
        if (sessionProperty == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
        PropertyMetadata<?> metadata = sessionProperty.getMetadata();
        if (metadata.getJavaType() != type) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("Property %s is type %s, but requested type was %s", name,
                    metadata.getJavaType().getName(),
                    type.getName()));
        }
        if (value == null) {
            return type.cast(metadata.getDefaultValue());
        }
        Object objectValue = deserializeSessionProperty(metadata.getSqlType(), value);
        try {
            return type.cast(metadata.decode(objectValue));
        }
        catch (PrestoException e) {
            throw e;
        }
        catch (Exception e) {
            // the system property decoder can throw any exception
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s is invalid: %s", name, value), e);
        }
    }

    public static Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, Metadata metadata)
    {
        Object value = evaluateConstantExpression(expression, expectedType, metadata, session);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Session property value must not be null");
        }
        return objectValue;
    }

    public static String serializeSessionProperty(Type type, Object value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Session property can not be null");
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return value.toString();
        }
        if (BigintType.BIGINT.equals(type)) {
            return value.toString();
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return value.toString();
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value.toString();
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).toJson(value);
        }
        throw new PrestoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static Object deserializeSessionProperty(Type type, String value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Session property can not be null");
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.valueOf(value);
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.valueOf(value);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.valueOf(value);
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).fromJson(value);
        }
        throw new PrestoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static <T> JsonCodec<T> getJsonCodecForType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(String.class);
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Boolean.class);
        }
        if (BigintType.BIGINT.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Long.class);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Double.class);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.listJsonCodec(getJsonCodecForType(elementType));
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.mapJsonCodec(getMapKeyType(keyType), getJsonCodecForType(valueType));
        }
        throw new PrestoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static Class<?> getMapKeyType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return String.class;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.class;
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.class;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.class;
        }
        throw new PrestoException(INVALID_SESSION_PROPERTY, format("Session property map key type %s is not supported", type));
    }

    public static class SessionPropertyValue
    {
        private final String fullyQualifiedName;
        private final Optional<String> catalogName;
        private final String propertyName;
        private final String description;
        private final String type;
        private final String value;
        private final String defaultValue;
        private final boolean hidden;

        private SessionPropertyValue(String value,
                String defaultValue,
                String fullyQualifiedName,
                Optional<String> catalogName,
                String propertyName,
                String description,
                String type,
                boolean hidden)
        {
            this.fullyQualifiedName = fullyQualifiedName;
            this.catalogName = catalogName;
            this.propertyName = propertyName;
            this.description = description;
            this.type = type;
            this.value = value;
            this.defaultValue = defaultValue;
            this.hidden = hidden;
        }

        public String getFullyQualifiedName()
        {
            return fullyQualifiedName;
        }

        public Optional<String> getCatalogName()
        {
            return catalogName;
        }

        public String getPropertyName()
        {
            return propertyName;
        }

        public String getDescription()
        {
            return description;
        }

        public String getType()
        {
            return type;
        }

        public String getValue()
        {
            return value;
        }

        public String getDefaultValue()
        {
            return defaultValue;
        }

        public boolean isHidden()
        {
            return hidden;
        }
    }

    private final class SessionProperty<T>
    {
        private final Optional<String> catalogName;
        private final PropertyMetadata<T> propertyMetadata;
        private final String propertyName;

        private SessionProperty(Optional<String> catalogName, PropertyMetadata<T> propertyMetadata)
        {
            this.catalogName = catalogName;
            this.propertyName = propertyMetadata.getName();

            String fullName = propertyMetadata.getName();
            if (catalogName.isPresent()) {
                fullName = catalogName.get() + "." + fullName;
            }

            this.propertyMetadata = new PropertyMetadata<>(
                    fullName,
                    propertyMetadata.getDescription(),
                    propertyMetadata.getSqlType(),
                    propertyMetadata.getJavaType(),
                    propertyMetadata.getDefaultValue(),
                    propertyMetadata.isHidden(),
                    propertyMetadata::decode);
        }

        public String getFullyQualifiedName()
        {
            return propertyMetadata.getName();
        }

        public Optional<String> getCatalogName()
        {
            return catalogName;
        }

        public String getPropertyName()
        {
            return propertyName;
        }

        public PropertyMetadata<T> getMetadata()
        {
            return propertyMetadata;
        }
    }
}
