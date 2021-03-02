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
package com.facebook.presto.bloomfilter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BloomFilterForDynamicFilterImpl.class, name = "bloomFilter")})
public abstract class BloomFilterForDynamicFilter
        implements Serializable
{
    public abstract void put(Object value);
    public abstract boolean contain(Object value);
    public abstract BloomFilterForDynamicFilter merge(BloomFilterForDynamicFilter bloomFilter);
    public abstract void writeObject(ObjectOutputStream outputStream) throws IOException;
    public abstract void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException;
}
