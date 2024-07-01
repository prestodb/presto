#ifndef R_SKIP_DAYS_H
#define R_SKIP_DAYS_H

#include "config.h"
#include "dist.h"
#include "porting.h"

ds_key_t
skipDays(int nTable, ds_key_t* pRemainder, DSDGenContext& dsdGenContext);

#endif
