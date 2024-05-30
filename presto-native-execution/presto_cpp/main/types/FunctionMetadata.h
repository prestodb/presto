#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/CoverageUtil.h"
#include "velox/functions/FunctionRegistry.h"

#include "presto_cpp/presto_protocol/presto_protocol.h"

namespace facebook::presto {

void getJsonMetadataForFunction(
    const std::string& functionName,
    nlohmann::json& jsonMetadataList);

json getJsonFunctionMetadata();

} // namespace facebook::presto
