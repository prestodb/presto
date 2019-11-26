/**
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
@file:Suppress("ConvertSecondaryConstructorToPrimary")

package com.victoriametrics.presto.model

import com.facebook.presto.spi.ColumnHandle
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class VmColumnHandle : ColumnHandle {
    // val tableName: SchemaTableName
    //     @JsonProperty("tableName") get // For Jackson

    val columnName: String
        @JsonProperty("columnName") get // For Jackson

    @JsonCreator
    constructor(@JsonProperty("columnName") columnName: String) {
        // this.tableName = tableName
        this.columnName = columnName
    }


    override fun toString(): String {
        return "VmColumnHandle('$columnName')"
    }
}

// Doesn't work, still wants default constructor on VmColumnHandle
// @JsonDeserialize(using = VmColumnHandleDeserializer::class)
// @Serializable
// data class VmColumnHandle(
//         val columnName: String
// ) : ColumnHandle {
//     companion object {
//         private val json = Json(JsonConfiguration.Stable)
//         private val serializer = serializer()
//         fun deserialize(content: String): VmColumnHandle {
//             return json.parse(serializer, content)
//         }
//     }
// }
// class VmColumnHandleDeserializer : StdDeserializer<VmColumnHandle>(VmColumnHandle::class.java) {
//     @Throws(IOException::class, JsonProcessingException::class)
//     override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): VmColumnHandle {
//         val sw = StringWriter()
//         val jsonGenerator = jp.codec.factory.createGenerator(sw)
//         jp.codec.writeValue(jsonGenerator, jp)
//         return VmColumnHandle.deserialize(sw.toString())
//     }
// }
