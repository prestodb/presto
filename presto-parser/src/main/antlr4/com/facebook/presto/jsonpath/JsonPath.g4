/*
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

grammar JsonPath;

tokens {
    DELIMITER
}

path
    : pathMode pathExpression EOF
    ;

pathMode
    : LAX
    | STRICT
    ;

pathExpression
    : accessorExpression                                                    #expressionDefault
    | sign=('+' | '-') pathExpression                                       #signedUnary
    | left=pathExpression operator=('*' | '/' | '%') right=pathExpression   #binary
    | left=pathExpression operator=('+' | '-') right=pathExpression         #binary
    ;

accessorExpression
    : pathPrimary                                               #accessorExpressionDefault
    | accessorExpression '.' identifier                         #memberAccessor
    | accessorExpression '.' stringLiteral                      #memberAccessor
    | accessorExpression '.' '*'                                #wildcardMemberAccessor
    | accessorExpression '[' subscript (',' subscript)* ']'     #arrayAccessor
    | accessorExpression '[' '*' ']'                            #wildcardArrayAccessor
    | accessorExpression '?' '(' predicate ')'                  #filter
    | accessorExpression '.' TYPE '(' ')'                       #typeMethod
    | accessorExpression '.' SIZE '(' ')'                       #sizeMethod
    | accessorExpression '.' DOUBLE '(' ')'                     #doubleMethod
    | accessorExpression '.' CEILING '(' ')'                    #ceilingMethod
    | accessorExpression '.' FLOOR '(' ')'                      #floorMethod
    | accessorExpression '.' ABS '(' ')'                        #absMethod
    | accessorExpression '.' DATETIME '(' stringLiteral?  ')'   #datetimeMethod
    | accessorExpression '.' KEYVALUE '(' ')'                   #keyValueMethod
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

subscript
    : singleton=pathExpression
    | from=pathExpression TO to=pathExpression
    ;

pathPrimary
    : literal                   #literalPrimary
    | variable                  #variablePrimary
    | '(' pathExpression ')'    #parenthesizedPath
    ;

literal
    : numericLiteral
    | stringLiteral
    | nullLiteral
    | booleanLiteral
    ;

numericLiteral
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

stringLiteral
    : STRING // add unicode (like SqlBase.g4), add quoting in single quotes (')
    ;

nullLiteral
    : NULL
    ;

booleanLiteral
    : TRUE | FALSE
    ;

variable
    : '$'               #contextVariable
    | NAMED_VARIABLE    #namedVariable
    | LAST              #lastIndexVariable
    | '@'               #predicateCurrentItemVariable
    ;

// the following part is dedicated to JSON path predicate
predicate
    : predicatePrimary                      #predicateDefault
    | '!' delimitedPredicate                #negationPredicate
    | left=predicate '&&' right=predicate   #conjunctionPredicate
    | left=predicate '||' right=predicate   #disjunctionPredicate
    ;

predicatePrimary
    : delimitedPredicate                                                                    #predicatePrimaryDefault
    | left=pathExpression comparisonOperator right=pathExpression                           #comparisonPredicate
    | base=pathExpression LIKE_REGEX pattern=stringLiteral ( FLAG flag=stringLiteral )?     #likeRegexPredicate
    | whole=pathExpression STARTS WITH (string=stringLiteral | NAMED_VARIABLE)              #startsWithPredicate
    | '(' predicate ')' IS UNKNOWN                                                          #isUnknownPredicate
    ;

delimitedPredicate
    : EXISTS '(' pathExpression ')'     #existsPredicate
    | '(' predicate ')'                 #parenthesizedPredicate
    ;

comparisonOperator
    : '==' | '<>' | '!=' | '<' | '>' | '<=' | '>='
    ;

// there shall be no reserved words in JSON path
nonReserved
    : ABS | CEILING | DATETIME | DOUBLE | EXISTS | FALSE | FLAG | FLOOR | IS | KEYVALUE | LAST | LAX | LIKE_REGEX | MINUS | NULL | SIZE | STARTS | STRICT | TO | TRUE | TYPE | UNKNOWN | WITH
    ;

ABS: 'abs';
CEILING: 'ceiling';
DATETIME: 'datetime';
DOUBLE: 'double';
EXISTS: 'exists';
FALSE: 'false';
FLAG: 'flag';
FLOOR: 'floor';
IS: 'is';
KEYVALUE: 'keyvalue';
LAST: 'last';
LAX: 'lax';
LIKE_REGEX: 'like_regex';
MINUS: '-';
NULL: 'null';
SIZE: 'size';
STARTS: 'starts';
STRICT: 'strict';
TO: 'to';
TRUE: 'true';
TYPE: 'type';
UNKNOWN: 'unknown';
WITH: 'with';

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

INTEGER_VALUE
    : DIGIT+
    ;

STRING
    : '"' ( ~'"' | '""' )* '"'
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

NAMED_VARIABLE
    : '$' IDENTIFIER
    ;

fragment EXPONENT
    : ('E' | 'e') [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-z] | [A-Z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
UNRECOGNIZED: .;
