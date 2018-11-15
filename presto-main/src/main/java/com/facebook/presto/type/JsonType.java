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
package com.facebook.presto.type;

/**
 * The stack representation for JSON objects must have the keys in natural sorted order.
 */
public class JsonType
        extends com.facebook.presto.spi.type.JsonType
{
    public static final com.facebook.presto.spi.type.JsonType JSON = com.facebook.presto.spi.type.JsonType.JSON;
}
