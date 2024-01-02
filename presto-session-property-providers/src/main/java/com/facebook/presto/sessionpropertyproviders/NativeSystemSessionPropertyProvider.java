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

import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class NativeSystemSessionPropertyProvider
        implements SystemSessionPropertyProvider
{
    @Override
    public List<SessionPropertyMetadata> getSessionProperties()
    {
        // TODO: Make RPC call and get session properties from prestissimo.
        // Lets return hardcoded list of properties for validation
        return ImmutableList.of(
                new SessionPropertyMetadata(
                        "property_name_1",
                        "description of property 1",
                        BOOLEAN,
                        "false",
                        false),
                new SessionPropertyMetadata(
                        "property_name_2",
                        "description of property 2",
                        INTEGER,
                        "0",
                        false),
                new SessionPropertyMetadata(
                        "property_name_3",
                        "description of property 3",
                        BIGINT,
                        "100000",
                        false));
    }
}
