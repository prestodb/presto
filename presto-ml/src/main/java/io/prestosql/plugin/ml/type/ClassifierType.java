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
package com.facebook.presto.ml.type;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

// Layout is <size>:<model>, where
//   size: is an int describing the length of the model bytes
//   model: is the serialized model
public class ClassifierType
        extends ModelType
{
    public static final ClassifierType BIGINT_CLASSIFIER = new ClassifierType(BIGINT);
    public static final ClassifierType VARCHAR_CLASSIFIER = new ClassifierType(VARCHAR);

    private final Type labelType;

    public ClassifierType(Type type)
    {
        super(new TypeSignature(ClassifierParametricType.NAME, TypeSignatureParameter.of(type.getTypeSignature())));
        checkArgument(type.isComparable(), "type must be comparable");
        labelType = type;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(labelType);
    }
}
