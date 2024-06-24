#pragma once

#include "append_info-c.hpp"
#include <memory>
#include <cassert>
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;
namespace facebook::velox::tpcds {

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
	int first_column;
	int colIndex = 0;
	int rowIndex = 0;
        std::vector<VectorPtr> children;

        bool IsNull();
};
} // namespace facebook::velox::tpcds