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

#include "config.h"
#include "porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <stdio.h>
#include "decimal.h"
#include "dist.h"
#include "mathops.h"

/*
 * Routine: set_precision(decimal_t *dest, int size, int precision)
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: None
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void set_precision(decimal_t* dest, int scale, int precision) {
  dest->scale = scale;
  dest->precision = precision;
  dest->number = 0;
  dest->flags = 0;

  return;
}

/*
 * Routine: mk_decimal(int size, int precision)
 * Purpose: initialize a decimal_t
 * Algorithm:
 * Data Structures:
 *
 * Params:	int size:		total number of places in the decimal
 *			int precision:	number of places in the fraction
 * Returns: decimal_t *
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
decimal_t* mk_decimal(int s, int p) {
  auto res = (decimal_t*)malloc(sizeof(struct DECIMAL_T));

  if ((s < 0) || (p < 0))
    return (NULL);

  MALLOC_CHECK(res);

  res->flags = 0;
  res->scale = s;
  res->precision = p;
  res->flags =
      static_cast<int>(static_cast<unsigned int>(res->flags) | FL_INIT);

  return (res);
}

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
 * Routine: ftodec(double f, decimal_t *dec)
 * Purpose: Convert a double to a decimal_t
 * Algorithm:
 * Data Structures:
 *
 * Params: double f
 * Returns: decimal_t *
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ftodec(decimal_t* dest, double f) {
  static char valbuf[20];

  auto result = snprintf(valbuf, sizeof(valbuf), "%f", f);
  if (result < 0)
    perror("sprintf failed");

  return (strtodec(dest, valbuf));
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
int strtodec(decimal_t* dest, char* s) {
  int i;
  char* d_pt;
  char valbuf[20];

  if (strlen(s) < 20) {
    strcpy(valbuf, s);
  }
  dest->flags = 0;
  if ((d_pt = strchr(valbuf, '.')) == NULL) {
    dest->scale = strlen(valbuf);
    dest->number = atoi(valbuf);
    dest->precision = 0;
  } else {
    *d_pt = '\0';
    d_pt += 1;
    dest->scale = strlen(valbuf);
    dest->number = atoi(valbuf);
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
 * Routine: dectostr(decimal_t *d, char *buf)
 * Purpose: convert a decimal structure to a string
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: char *; NULL on success
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int dectostr(char* dest, decimal_t* d, DSDGenContext& dsdGenContext) {
  ds_key_t number;
  int i;
  static char szFormat[80];

  if (!dsdGenContext.dectostr_init) {
    auto result = sprintf(szFormat, "%s.%s", HUGE_FORMAT, HUGE_FORMAT);
    if (result < 0)
      perror("sprintf failed");
    dsdGenContext.dectostr_init = 1;
  }

  if (d == NULL || dest == NULL)
    return (-1);
  for (number = d->number, i = 0; i < d->precision; i++)
    number /= 10;

  dest = (char*)malloc(160);
  auto result = sprintf(dest, szFormat, number, d->number - number);
  if (result < 0)
    perror("sprintf failed");

  return (0);
}

/*
 * Routine: dectof(float *dest, decimal_t *d)
 * Purpose: convert a decimal structure to a double
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: char *; NULL on success
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int dectoflt(double* dest, decimal_t* d) {
  if ((dest == NULL) || (d == NULL))
    return (-1);
#ifdef WIN32
#pragma warning(disable : 4244)
#endif
  *dest = d->number;
#ifdef WIN32
#pragma warning(default : 4244)
#endif
  while (--d->precision > 0)
    *dest /= 10.0;

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

#ifdef TEST
main() {
  decimal_t* res;
  int code;

  /* mk_decimal */
  res = mk_decimal(5, 2);
  if (res == NULL) {
    printf("mk_decimal returned NULL\n");
    exit(-1);
  }

  /* itodec */
  itodec(res, 0);
  code = dectoi(res);
  if (code) {
    printf("r_decimal:itodec(0, res) != 0 (%d)\n", code);
    exit(-1);
  }

  itodec(res, 999);
  code = dectoi(res);
  if (code != 999) {
    printf("r_decimal:itodec(999, res) != 0 (%d)\n", code);
    exit(-1);
  }

  exit(0);
}
#endif /* TEST */
