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
#include "velox/exec/tests/utils/SignatureBinder.h"
#include <boost/algorithm/string.hpp>

namespace facebook::velox::exec::test {

namespace {
bool isAny(const TypeSignature& typeSignature) {
  return typeSignature.baseType() == "any";
}
} // namespace

bool SignatureBinder::tryBind() {
  const auto& formalArgs = signature_.argumentTypes();
  auto formalArgsCnt = formalArgs.size();

  if (signature_.variableArity()) {
    if (actualTypes_.size() < formalArgsCnt - 1) {
      return false;
    }

    if (!isAny(signature_.argumentTypes().back())) {
      if (actualTypes_.size() > formalArgsCnt) {
        auto& type = actualTypes_[formalArgsCnt - 1];
        for (auto i = formalArgsCnt; i < actualTypes_.size(); i++) {
          if (!type->kindEquals(actualTypes_[i]) &&
              actualTypes_[i]->kind() != TypeKind::UNKNOWN) {
            return false;
          }
        }
      }
    }
  } else {
    if (formalArgsCnt != actualTypes_.size()) {
      return false;
    }
  }

  for (auto i = 0; i < formalArgsCnt && i < actualTypes_.size(); i++) {
    if (!tryBind(formalArgs[i], actualTypes_[i])) {
      return false;
    }
  }
  return true;
}

bool SignatureBinder::tryBind(
    const exec::TypeSignature& typeSignature,
    const TypePtr& actualType) {
  if (isAny(typeSignature)) {
    return true;
  }

  auto it = bindings_.find(typeSignature.baseType());
  if (it == bindings_.end()) {
    // concrete type
    if (boost::algorithm::to_upper_copy(typeSignature.baseType()) !=
        actualType->kindName()) {
      return false;
    }

    const auto& params = typeSignature.parameters();
    if (params.size() != actualType->size()) {
      return false;
    }

    for (auto i = 0; i < params.size(); i++) {
      if (!tryBind(params[i], actualType->childAt(i))) {
        return false;
      }
    }

    return true;
  }

  // generic type
  VELOX_CHECK_EQ(
      typeSignature.parameters().size(),
      0,
      "Generic types with parameters are not supported");
  if (it->second == nullptr) {
    it->second = actualType;
    return true;
  }

  return it->second->kindEquals(actualType);
}

TypePtr SignatureBinder::tryResolveType(
    const exec::TypeSignature& typeSignature) const {
  const auto& params = typeSignature.parameters();

  std::vector<TypePtr> children;
  children.reserve(params.size());
  for (auto& param : params) {
    auto type = tryResolveType(param);
    if (!type) {
      return nullptr;
    }
    children.emplace_back(type);
  }

  auto it = bindings_.find(typeSignature.baseType());
  if (it == bindings_.end()) {
    // concrete type
    auto typeKind = mapNameToTypeKind(
        boost::algorithm::to_upper_copy(typeSignature.baseType()));

    // createType(kind) function doesn't support ROW, UNKNOWN and OPAQUE type
    // kinds.
    if (typeKind == TypeKind::ROW) {
      return ROW(std::move(children));
    }
    if (typeKind == TypeKind::UNKNOWN) {
      return UNKNOWN();
    }
    if (typeKind == TypeKind::OPAQUE) {
      return OpaqueType::create<void>();
    }
    return createType(typeKind, std::move(children));
  }

  return it->second;
}
} // namespace facebook::velox::exec::test
