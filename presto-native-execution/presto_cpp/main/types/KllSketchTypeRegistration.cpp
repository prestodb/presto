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

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/type/Type.h"

namespace facebook::presto::functions {

namespace {
class KllSketchTypeFactory : public velox::CustomTypeFactory {
 public:
  velox::TypePtr getType(
      const std::vector<velox::TypeParameter>& parameters) const override {
    VELOX_CHECK_EQ(parameters.size(), 1);
    VELOX_CHECK(parameters[0].kind == velox::TypeParameterKind::kType);
    return KLLSKETCH(parameters[0].type);
  }

  // KllSketch should be treated as Varbinary during type castings.
  velox::exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  velox::AbstractInputGeneratorPtr getInputGenerator(
      const velox::InputGeneratorConfig& config) const override {
    // KllSketch doesn't need a custom input generator for fuzzing
    return nullptr;
  }
};
} // namespace

void registerKllSketchType() {
  if (velox::customTypeExists("kllsketch")) {
    // Something is already registered under "kllsketch". Verify it is our
    // factory by asking it to produce a type and checking the concrete class.
    // If a different, conflicting factory got there first we must fail loudly
    // rather than silently use the wrong type.
    auto existing = velox::getCustomType(
        "kllsketch", {velox::TypeParameter{velox::BIGINT()}});
    VELOX_CHECK(
        isKllSketchType(existing),
        "A type named 'kllsketch' is already registered but is not a "
        "KllSketchType. There is a conflicting registration from another "
        "module.");
    return;
  }
  VELOX_CHECK(
      velox::registerCustomType(
          "kllsketch", std::make_unique<const KllSketchTypeFactory>()),
      "Failed to register kllsketch custom type despite it not existing. "
      "This is an unexpected internal error.");
}

} // namespace facebook::presto::functions
