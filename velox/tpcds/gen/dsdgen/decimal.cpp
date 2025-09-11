/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/mathops.h"

/*
 * Routine: itodec(int src, decimal_t *dest)
 * Purpose: convert an integer to a decimal_t
 * Algorithm:
 * Data Structures:
 *
 * Params: source integer
 * Returns: decimal_t *
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 * 20000104 need to set errno on error
 */
int itodec(decimal_t* dest, int src) {
  int scale = 1, bound = 1;

  while ((bound * 10) <= src) {
    scale += 1;
    bound *= 10;
  }

  dest->precision = 0;
  dest->scale = scale;
  dest->number = src;

  return (0);
}

/*
 * Routine: strtodec()
 * Purpose: Convert an ascii string to a decimal_t structure
 * Algorithm:
 * Data Structures:
 *
 * Params: char *s
 * Returns: decimal_t *
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int strtodec(decimal_t* dest, const char* s) {
  int i;
  char* d_pt;
  std::vector<char> valbuf(20);

  if (strlen(s) < 20) {
    strcpy(valbuf.data(), s);
  }
  dest->flags = 0;
  if ((d_pt = strchr(valbuf.data(), '.')) == NULL) {
    dest->scale = valbuf.size();
    dest->number = atoi(valbuf.data());
    dest->precision = 0;
  } else {
    *d_pt = '\0';
    d_pt += 1;
    dest->scale = valbuf.size();
    dest->number = atoi(valbuf.data());
    dest->precision = strlen(d_pt);
    for (i = 0; i < dest->precision; i++)
      dest->number *= 10;
    dest->number += atoi(d_pt);
  }

  if (*s == '-' && dest->number > 0)
    dest->number *= -1;

  return (0);
}

/*
 * Routine: decimal_t_op(int op, decimal_t *operand1, decimal_t *operand2)
 * Purpose: execute arbitrary binary operations on decimal_t's
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int decimal_t_op(decimal_t* dest, int op, decimal_t* d1, decimal_t* d2) {
  int res, np;
  float f1, f2;

  if ((d1 == NULL) || (d2 == NULL))
    return (-1);

  dest->scale = (d1->scale > d2->scale) ? d1->scale : d2->scale;
  if (d1->precision > d2->precision) {
    dest->precision = d1->precision;
  } else {
    dest->precision = d2->precision;
  }

  switch (op) {
    case OP_PLUS:
      dest->number = d1->number + d2->number;
      break;
    case OP_MINUS:
      dest->number = d1->number - d2->number;
      break;
    case OP_MULT:
      res = d1->precision + d2->precision;
      dest->number = d1->number * d2->number;
      while (res-- > dest->precision)
        dest->number /= 10;
      break;
    case OP_DIV:
      f1 = static_cast<float>(d1->number);
      np = d1->precision;
      while (np < dest->precision) {
        f1 *= 10.0;
        np += 1;
      }
      np = 0;
      while (np < dest->precision) {
        f1 *= 10.0;
        np += 1;
      }
      f2 = static_cast<float>(d2->number);
      np = d2->precision;
      while (np < dest->precision) {
        f2 *= 10.0;
        np += 1;
      }

      dest->number = static_cast<int>(f1 / f2);
      break;
    default: {
      auto result = printf("decimal_t_op does not support op %d\n", op);
      if (result < 0)
        perror("sprintf failed");
      exit(1);
      break;
    }
  }

  return (0);
}
