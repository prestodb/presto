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
package com.facebook.presto.spi;

/**
 * A ColumnIdentity keeps all the information needed to determine if a column
 * has changed for a certain table. Different columns for different tables may
 * have the same ColumnIdentities.
 * <p>
 * The ColumnIdentity is saved as serialized bytes.
 * <p>
 * ColumnIdentity can be used to ensure that the columns used in view
 * definition has not changed since the view creation.
 */
public interface ColumnIdentity
{
    /**
     * This must produce a data that is backwards compatible across releases.
     * The ConnectorMetadata must always be able to deserialize these bytes.
     */
    byte[] serialize();

    /**
     * Must properly compare the equality of ColumnIdentity across releases.
     */
    boolean equals(Object o);

    /**
     * Must be implemented to properly compare the equality of ColumnIdentity.
     */
    int hashCode();
}
