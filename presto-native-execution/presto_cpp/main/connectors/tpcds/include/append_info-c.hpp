#pragma once

#include "append_info-c.hpp"
#include <memory>
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "presto_cpp/main/connectors/tpcds/dsdgen/include/dsdgen-c/dist.h"

using namespace facebook::velox;
namespace facebook::velox::tpcds {

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
	int first_column;
	int colIndex = 0;
	int rowIndex = 0;
	DSDGenContext* dsdGenContext;
	std::vector<VectorPtr> children;
	bool IsNull(int32_t column);
};
} // namespace facebook::velox::tpcds