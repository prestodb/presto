
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


#include "SessionPropertyReporter.h"

namespace facebook::presto {
    json SessionPropertyReporter::getJsonMetaDataSessionProperty()
    {
        SystemSessionProperties systemSessionProperties;
        json j = json::array();
        for (const auto& sessionProperty : systemSessionProperties.getSessionProperties()) {
            json sessionPropertyJson;
            sessionPropertyJson["name"] = sessionProperty->getName();
            sessionPropertyJson["description"] = sessionProperty->getDescription();
            sessionPropertyJson["typeSignature"] = getSqlType(*sessionProperty);
            sessionPropertyJson["defaultValue"] = getDefault(*sessionProperty);
            sessionPropertyJson["hidden"] = sessionProperty->isHidden();
            j.emplace_back(sessionPropertyJson);
        }
        return j;
    }

    std::string SessionPropertyReporter::getSqlType(const SessionProperty& sessionPropertyType)
    {
        auto result = sessionPropertyType.getType();

        if (result == PropertyType::kInt) {
            return "integer";
        } else if (result == PropertyType::kBool) {
            return "boolean";
        } else if (result == PropertyType::kLong) {
            return "bigint";
        } else {
            return "UnknownType";
        }
    }

    std::string SessionPropertyReporter::getDefault(const SessionProperty& sessionPropertyType)
    {
        auto result = sessionPropertyType.getDefaultValue();

        if (sessionPropertyType.getType() == PropertyType::kBool)
        {
            if (result == "0") {
                result = "false";
            } else {
                result = "true";
            } 
        }
        return result;
    }
} // namespace facebook::presto

