grammar TypeSignature;

@parser::header {
    #include <boost/algorithm/string.hpp>
    #include "presto_cpp/main/types/TypeSignatureTypeConverter.h"
}

@parser::declarations {
    #define Token()         (getCurrentToken()->getText())
    #define UpCase(str)     (boost::to_upper_copy(str))
    #define isRowToken()         (UpCase(Token()) == "ROW")
    #define isMapToken()         (UpCase(Token()) == "MAP")
    #define isArrayToken()       (UpCase(Token()) == "ARRAY")
    #define isVarToken()         (UpCase(Token()) == "VARCHAR" || UpCase(Token()) == "VARBINARY")
    #define isDecimalToken()     (UpCase(Token()) == "DECIMAL")
}

start : type_spec EOF ;

type_spec :
        named_type
      | type
      ;

named_type :
      identifier type
      ;

type :
      simple_type
    | decimal_type
    | variable_type
    | array_type
    | map_type
    | row_type
    ;

simple_type :
      WORD
    | TYPE_WITH_SPACES;

variable_type :
      { isVarToken() }? WORD
    | { isVarToken() }? WORD '(' NUMBER* ')'
    ;

decimal_type :
      { isDecimalToken() }? WORD '(' NUMBER* ',' NUMBER* ')'
    ;

type_list :
      type_spec (',' type_spec)*
    ;

row_type :
    { isRowToken() }? WORD '(' type_list ')' ;

map_type :
    { isMapToken() }? WORD '(' type ',' type ')' ;

array_type :
    { isArrayToken() }? WORD '(' type ')' ;

identifier :
      QUOTED_ID
    | WORD
    ;

TYPE_WITH_SPACES:
      D O U B L E ' ' P R E C I S I O N
    | T I M E ' ' W I T H ' ' T I M E ' ' Z O N E
    | T I M E S T A M P ' ' W I T H ' ' T I M E ' ' Z O N E
    | I N T E R V A L ' ' Y E A R ' ' T O ' ' M O N T H
    | I N T E R V A L ' ' D A Y ' ' T O ' ' S E C O N D
    ;

WORD: [A-Za-z_] [A-Za-z0-9_:@]* ;
QUOTED_ID: '"' [A-Za-z_] [ A-Za-z0-9_:@]* '"' ;
NUMBER: [0-9]+ ;

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

WHITESPACE  : [ \t\n] -> skip ;
