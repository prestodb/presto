#include "presto_cpp/main/governance/wkc_functions/QaagFunctionsRegistration.h"
#include "presto_cpp/main/governance/wkc_functions/Qaag.h"

namespace facebook::presto::governance {

using namespace facebook::velox;

void registerQaagFunctions(const std::string& prefix) {
  registerFunction<
      Mask,
      int64_t,
      int64_t,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_bigint"});
  registerFunction<
      Mask,
      bool,
      bool,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_boolean"});
  registerFunction<
      Mask,
      double,
      double,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_double"});
  registerFunction<
      Mask,
      float,
      float,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_real"});
  registerFunction<
      MaskDecimal,
      ShortDecimal<P1, S1>,
      ShortDecimal<P1, S1>,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t,
      int32_t,
      Varchar>({prefix + "wkcgovernmask_decimal_short"});
  registerFunction<
      MaskDecimal,
      LongDecimal<P1, S1>,
      LongDecimal<P1, S1>,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t,
      int32_t,
      Varchar>({prefix + "wkcgovernmask_decimal_long"});

  registerFunction<
      Mask,
      int32_t,
      int32_t,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_integer"});
  registerFunction<
      Mask,
      int16_t,
      int16_t,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_smallint"});
  registerFunction<
      Mask,
      int8_t,
      int8_t,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_tinyint"});
  registerFunction<
      Mask,
      Varchar,
      Varchar,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_varchar"});
  registerFunction<
      Mask,
      Varbinary,
      Varbinary,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_varbinary"});
  registerFunction<
      MaskDate,
      Date,
      Date,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_date"});
  registerFunction<
      MaskDate,
      Date,
      Varchar,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_date"});
  registerFunction<
      MaskTimestamp,
      Timestamp,
      Timestamp,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_timestamp"});
  registerFunction<
      MaskTimestamp,
      Timestamp,
      Varchar,
      int32_t,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "wkcgovernmask_timestamp"});
}

}; // namespace facebook::presto::governance
