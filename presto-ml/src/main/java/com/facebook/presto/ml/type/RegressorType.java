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

import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

// Layout is <size>:<model>, where
//   size: is an int describing the length of the model bytes
//   model: is the serialized model
public class RegressorType
        extends ModelType
{
    public static final RegressorType REGRESSOR = new RegressorType();
    public static final String NAME = "Regressor";
    private static final TypeSignature SIGNATURE = parseTypeSignature(NAME);

    @JsonCreator
    public RegressorType()
    {
    }

    @Override
    public TypeSignature getTypeSignature()
    {
        return SIGNATURE;
    }
}
