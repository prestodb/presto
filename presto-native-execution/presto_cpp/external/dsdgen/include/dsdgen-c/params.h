/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef QGEN_PARAMS_H
#define QGEN_PARAMS_H

#include "build_support.h"
#include "r_params.h"

#ifdef DECLARER
#else
extern option_t options[];
extern char* params[];
extern char* szTableNames[];
#endif

#endif
