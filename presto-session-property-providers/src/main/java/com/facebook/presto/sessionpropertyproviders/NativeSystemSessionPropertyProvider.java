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

package com.facebook.presto.sessionpropertyproviders;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class NativeSystemSessionPropertyProvider
        implements SystemSessionPropertyProvider
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;

    private static final List<PropertyMetadata<?>> propertyMetadataList = new ArrayList<>();

    @Inject
    public NativeSystemSessionPropertyProvider(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    // TODO : Access specifier need made private once NodeManager is made working.
    protected List<PropertyMetadata<?>> deserializeSessionProperties(String responseBody)
    {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        JsonCodec<List<SessionPropertyMetadata>> sessionPropertiesJsonCodec = codecFactory.listJsonCodec(SessionPropertyMetadata.class);
        try {
            List<SessionPropertyMetadata> sessionProperties = sessionPropertiesJsonCodec.fromJson(responseBody);
            propertyMetadataList.clear();
            for (SessionPropertyMetadata sessionProperty : sessionProperties) {
                PropertyMetadata<?> propertyMetadata = toPropertyMetadata(sessionProperty);
                propertyMetadataList.add(propertyMetadata);
            }
            return propertyMetadataList;
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to deserialize the RPC response body.");
        }
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        if (!propertyMetadataList.isEmpty()) {
            return propertyMetadataList;
        }
        // Todo: Make RPC call and get session properties from prestissimo.
        try {
            String responseBody = "";
            return deserializeSessionProperties(responseBody);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get sessions definition for NativeSystemSessionPropertyProvider." + e.getMessage());
        }
    }

    public PropertyMetadata<?> toPropertyMetadata(SessionPropertyMetadata data)
    {
        requireNonNull(data, "data is null");
        PropertyMetadata<?> propertyMetadata;
        Type type = typeManager.getType(data.getTypeSignature());
        if (type == BOOLEAN) {
            propertyMetadata = booleanProperty(data.getName(), data.getDescription(), Boolean.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else if (type == INTEGER) {
            propertyMetadata = integerProperty(data.getName(), data.getDescription(), Integer.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else if (type == BIGINT) {
            propertyMetadata = longProperty(data.getName(), data.getDescription(), Long.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else if (type == VARCHAR) {
            propertyMetadata = stringProperty(data.getName(), data.getDescription(), String.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else if (type == DOUBLE) {
            propertyMetadata = doubleProperty(data.getName(), data.getDescription(), Double.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else {
            // TODO: Custom types need to be converted
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown type");
        }
        return propertyMetadata;
    }
}
