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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.sessionpropertyproviders.JavaWorkerSessionPropertyProvider;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.SessionPropertyContext;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.sql.planner.ParameterRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public final class SessionPropertyManager
{
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    private static final Logger log = Logger.get(SessionPropertyManager.class);
    private static final String SESSION_PROPERTY_PROVIDER_NAME = "session-property-provider.name";

    private final ConcurrentMap<String, PropertyMetadata<?>> systemSessionProperties = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectorId, Map<String, PropertyMetadata<?>>> connectorSessionProperties = new ConcurrentHashMap<>();
    private final Map<String, WorkerSessionPropertyProvider> workerSessionPropertyProviders;
    private final Map<String, WorkerSessionPropertyProviderFactory> workerSessionPropertyProviderFactories = new ConcurrentHashMap<>();
    private final Supplier<Map<String, PropertyMetadata<?>>> memoizedWorkerSessionProperties;
    private final Optional<NodeManager> nodeManager;
    private final Optional<TypeManager> functionAndTypeManager;
    private final File configDir;
    private final AtomicBoolean sessionPropertyProvidersLoading = new AtomicBoolean();

    @Inject
    public SessionPropertyManager(
            SystemSessionProperties systemSessionProperties,
            Map<String, WorkerSessionPropertyProvider> workerSessionPropertyProviders,
            FunctionAndTypeManager functionAndTypeManager,
            NodeManager nodeManager,
            SessionPropertyProviderConfig config)
    {
        this(systemSessionProperties.getSessionProperties(), workerSessionPropertyProviders, Optional.ofNullable(functionAndTypeManager), Optional.ofNullable(nodeManager), config);
    }

    public SessionPropertyManager(
            List<PropertyMetadata<?>> sessionProperties,
            Map<String, WorkerSessionPropertyProvider> workerSessionPropertyProviders,
            Optional<TypeManager> functionAndTypeManager,
            Optional<NodeManager> nodeManager,
            SessionPropertyProviderConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.memoizedWorkerSessionProperties = Suppliers.memoizeWithExpiration(this::getWorkerSessionProperties,
                1, HOURS);
        this.workerSessionPropertyProviders = new ConcurrentHashMap<>(workerSessionPropertyProviders);
        this.configDir = requireNonNull(config, "config is null").getSessionPropertyProvidersConfigurationDir();
        addSystemSessionProperties(sessionProperties);
    }

    public static SessionPropertyManager createTestingSessionPropertyManager()
    {
        return createTestingSessionPropertyManager(new SystemSessionProperties().getSessionProperties(), new JavaFeaturesConfig(), new NodeSpillConfig());
    }

    public static SessionPropertyManager createTestingSessionPropertyManager(SystemSessionProperties systemSessionProperties)
    {
        return createTestingSessionPropertyManager(systemSessionProperties.getSessionProperties(), new JavaFeaturesConfig(), new NodeSpillConfig());
    }

    public static SessionPropertyManager createTestingSessionPropertyManager(List<PropertyMetadata<?>> sessionProperties)
    {
        return createTestingSessionPropertyManager(sessionProperties, new JavaFeaturesConfig(), new NodeSpillConfig());
    }

    public static SessionPropertyManager createTestingSessionPropertyManager(
            List<PropertyMetadata<?>> sessionProperties,
            JavaFeaturesConfig javaFeaturesConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        return new SessionPropertyManager(
                sessionProperties,
                ImmutableMap.of(
                        "java-worker",
                        new JavaWorkerSessionPropertyProvider(
                                new FeaturesConfig(),
                                javaFeaturesConfig,
                                nodeSpillConfig)),
                Optional.empty(),
                Optional.empty(),
                new SessionPropertyProviderConfig());
    }

    public void loadSessionPropertyProviders()
            throws Exception
    {
        if (!sessionPropertyProvidersLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(configDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String sessionPropertyProviderName = getNameWithoutExtension(file.getName());
                Map<String, String> properties = loadProperties(file);
                checkState(!isNullOrEmpty(properties.get(SESSION_PROPERTY_PROVIDER_NAME)),
                        "Session property manager configuration %s does not contain %s",
                        file.getAbsoluteFile(),
                        SESSION_PROPERTY_PROVIDER_NAME);
                properties = new HashMap<>(properties);
                properties.remove(SESSION_PROPERTY_PROVIDER_NAME);
                loadSessionPropertyProvider(sessionPropertyProviderName, properties, functionAndTypeManager, nodeManager);
            }
        }
    }

    public void loadSessionPropertyProvider(String sessionPropertyProviderName, Map<String, String> properties, Optional<TypeManager> typeManager, Optional<NodeManager> nodeManager)
    {
        log.info("-- Loading %s session property provider --", sessionPropertyProviderName);
        WorkerSessionPropertyProviderFactory factory = workerSessionPropertyProviderFactories.get(sessionPropertyProviderName);
        checkState(factory != null, "No factory for session property provider : " + sessionPropertyProviderName);
        WorkerSessionPropertyProvider sessionPropertyProvider = factory.create(new SessionPropertyContext(typeManager, nodeManager));
        if (workerSessionPropertyProviders.putIfAbsent(sessionPropertyProviderName, sessionPropertyProvider) != null) {
            throw new IllegalArgumentException("System session property provider is already registered for property provider : " + sessionPropertyProviderName);
        }
        log.info("-- Added session property provider [%s] --", sessionPropertyProviderName);
    }

    public void addSessionPropertyProviderFactory(WorkerSessionPropertyProviderFactory factory)
    {
        if (workerSessionPropertyProviderFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("System Session property provider factory" + factory.getName() + "is already registered"));
        }
    }

    public void addSystemSessionProperties(List<PropertyMetadata<?>> systemSessionProperties)
    {
        systemSessionProperties
                .forEach(this::addSystemSessionProperty);
    }

    public void addSystemSessionProperty(PropertyMetadata<?> sessionProperty)
    {
        requireNonNull(sessionProperty, "sessionProperty is null");
        checkState(systemSessionProperties.put(sessionProperty.getName(), sessionProperty) == null,
                "System session property '%s' are already registered", sessionProperty.getName());
    }

    public void addConnectorSessionProperties(ConnectorId connectorId, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);
        checkState(connectorSessionProperties.putIfAbsent(connectorId, propertiesByName) == null, "Session properties for connectorId '%s' are already registered", connectorId);
    }

    public void removeConnectorSessionProperties(ConnectorId connectorId)
    {
        connectorSessionProperties.remove(connectorId);
    }

    public Optional<PropertyMetadata<?>> getSystemSessionPropertyMetadata(String name)
    {
        requireNonNull(name, "name is null");
        if (systemSessionProperties.get(name) == null) {
            return Optional.ofNullable(memoizedWorkerSessionProperties.get().get(name));
        }
        return Optional.ofNullable(systemSessionProperties.get(name));
    }

    public Optional<PropertyMetadata<?>> getConnectorSessionPropertyMetadata(ConnectorId connectorId, String propertyName)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(propertyName, "propertyName is null");
        Map<String, PropertyMetadata<?>> properties = connectorSessionProperties.get(connectorId);
        if (properties == null || properties.isEmpty()) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown connector " + connectorId);
        }

        return Optional.ofNullable(properties.get(propertyName));
    }

    private Map<String, PropertyMetadata<?>> getWorkerSessionProperties()
    {
        List<PropertyMetadata<?>> workerSessionPropertiesList = workerSessionPropertyProviders.values().stream()
                .flatMap(manager -> manager.getSessionProperties().stream())
                .collect(toImmutableList());
        Map<String, PropertyMetadata<?>> workerSessionProperties = new ConcurrentHashMap<>();
        workerSessionPropertiesList.forEach(sessionProperty -> {
            requireNonNull(sessionProperty, "sessionProperty is null");
            // TODO: Implement fail fast in case of duplicate entries.
            workerSessionProperties.put(sessionProperty.getName(), sessionProperty);
        });
        return workerSessionProperties;
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    public List<SessionPropertyValue> getAllSessionProperties(Session session, Map<String, ConnectorId> catalogs)
    {
        requireNonNull(session, "session is null");

        ImmutableList.Builder<SessionPropertyValue> sessionPropertyValues = ImmutableList.builder();
        Map<String, String> systemProperties = session.getSystemProperties();
        for (PropertyMetadata<?> property : new TreeMap<>(systemSessionProperties).values()) {
            String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
            String value = systemProperties.getOrDefault(property.getName(), defaultValue);
            sessionPropertyValues.add(new SessionPropertyValue(
                    value,
                    defaultValue,
                    property.getName(),
                    Optional.empty(),
                    property.getName(),
                    property.getDescription(),
                    property.getSqlType().getDisplayName(),
                    property.isHidden()));
        }

        for (Entry<String, ConnectorId> entry : new TreeMap<>(catalogs).entrySet()) {
            String catalog = entry.getKey();
            ConnectorId connectorId = entry.getValue();
            Map<String, String> connectorProperties = session.getConnectorProperties(connectorId);

            for (PropertyMetadata<?> property : new TreeMap<>(connectorSessionProperties.get(connectorId)).values()) {
                String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
                String value = connectorProperties.getOrDefault(property.getName(), defaultValue);

                sessionPropertyValues.add(new SessionPropertyValue(
                        value,
                        defaultValue,
                        catalog + "." + property.getName(),
                        Optional.of(catalog),
                        property.getName(),
                        property.getDescription(),
                        property.getSqlType().getDisplayName(),
                        property.isHidden()));
            }
        }

        for (PropertyMetadata<?> property : new TreeMap<>(memoizedWorkerSessionProperties.get()).values()) {
            String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
            String value = systemProperties.getOrDefault(property.getName(), defaultValue);
            sessionPropertyValues.add(new SessionPropertyValue(
                    value,
                    defaultValue,
                    property.getName(),
                    Optional.empty(),
                    property.getName(),
                    property.getDescription(),
                    property.getSqlType().getDisplayName(),
                    property.isHidden()));
        }
        return sessionPropertyValues.build();
    }

    public <T> T decodeSystemPropertyValue(String name, @Nullable String value, Class<T> type)
    {
        PropertyMetadata<?> property = getSystemSessionPropertyMetadata(name)
                .orElseThrow(() -> new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name));

        return decodePropertyValue(name, value, type, property);
    }

    public <T> T decodeCatalogPropertyValue(ConnectorId connectorId, String catalogName, String propertyName, @Nullable String propertyValue, Class<T> type)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> property = getConnectorSessionPropertyMetadata(connectorId, propertyName)
                .orElseThrow(() -> new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + fullPropertyName));

        return decodePropertyValue(fullPropertyName, propertyValue, type, property);
    }

    public void validateSystemSessionProperty(String propertyName, String propertyValue)
    {
        PropertyMetadata<?> propertyMetadata = getSystemSessionPropertyMetadata(propertyName)
                .orElseThrow(() -> new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + propertyName));

        decodePropertyValue(propertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    public void validateCatalogSessionProperty(ConnectorId connectorId, String catalogName, String propertyName, String propertyValue)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> propertyMetadata = getConnectorSessionPropertyMetadata(connectorId, propertyName)
                .orElseThrow(() -> new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + fullPropertyName));

        decodePropertyValue(fullPropertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    private static <T> T decodePropertyValue(String fullPropertyName, @Nullable String propertyValue, Class<T> type, PropertyMetadata<?> metadata)
    {
        if (metadata.getJavaType() != type) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("Property %s is type %s, but requested type was %s", fullPropertyName,
                    metadata.getJavaType().getName(),
                    type.getName()));
        }
        if (propertyValue == null) {
            return type.cast(metadata.getDefaultValue());
        }

        try {
            Object objectValue = deserializeSessionProperty(metadata.getSqlType(), propertyValue);
            return type.cast(metadata.decode(objectValue));
        }
        catch (PrestoException e) {
            throw e;
        }
        catch (Exception e) {
            // the system property decoder can throw any exception
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s is invalid: %s", fullPropertyName, propertyValue), e);
        }
    }

    public static Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, Metadata metadata, Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, metadata, session, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.getSqlFunctionProperties(), blockBuilder, 0);

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
        if (IntegerType.INTEGER.equals(type)) {
            return value.toString();
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return value.toString();
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value.toString();
        }
        if (TinyintType.TINYINT.equals(type)) {
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
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.valueOf(value);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.valueOf(value);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return Byte.valueOf(value);
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
        if (IntegerType.INTEGER.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Integer.class);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Double.class);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Byte.class);
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
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.class;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.class;
        }
        if (TinyintType.TINYINT.equals(type)) {
            return Byte.class;
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
}
