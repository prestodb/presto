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

package com.facebook.presto.sidecar.sessionpropertyproviders;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.tinyIntProperty;
import static java.util.Objects.requireNonNull;

public class NativeSystemSessionPropertyProvider
        implements WorkerSessionPropertyProvider
{
    private static final Logger log = Logger.get(NativeSystemSessionPropertyProvider.class);
    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final JsonCodec<List<SessionPropertyMetadata>> nativeSessionPropertiesJsonCodec;

    @Inject
    public NativeSystemSessionPropertyProvider(
            JsonCodec<List<SessionPropertyMetadata>> nativeSessionPropertiesJsonCodec,
            NodeManager nodeManager,
            TypeManager typeManager)
    {
        this.nativeSessionPropertiesJsonCodec = requireNonNull(nativeSessionPropertiesJsonCodec, "nativeSessionPropertiesJsonCodec is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return fetchSessionProperties();
    }

    private List<PropertyMetadata<?>> fetchSessionProperties()
    {
        try {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get session properties from sidecar.");
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
        else if (type == TINYINT) {
            propertyMetadata = tinyIntProperty(data.getName(), data.getDescription(), Byte.valueOf(data.getDefaultValue()), data.isHidden());
        }
        else {
            // TODO: Custom types need to be converted
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown type");
        }
        return propertyMetadata;
    }
}
