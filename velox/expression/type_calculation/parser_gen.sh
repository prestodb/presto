#!/usr/bin/env bash
set -e

FLEX="$(which flex)"
BISON="$(which bison)"

$FLEX --prefix=veloxtc --outfile=Scanner.cpp TypeCalculation.ll
$BISON --defines=Parser.h --output=Parser.cpp TypeCalculation.yy

# Fix include path
sed -i 's|Parser.h|velox/expression/type_calculation/Parser.h|g' Parser.cpp

cp Scanner.cpp Parser.cpp Parser.h stack.hh "$INSTALL_DIR"/
