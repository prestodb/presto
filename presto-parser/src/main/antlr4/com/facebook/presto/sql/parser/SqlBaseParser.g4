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

parser grammar SqlBaseParser;
options { tokenVocab=SqlBaseLexer; }

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                            #statementDefault
    | USE schema=identifier                                            #use
    | USE catalog=identifier  schema=identifier                        #use
    | CREATE TABLE qualifiedName AS query                              #createTableAsSelect
    | DROP TABLE qualifiedName                                         #dropTable
    | INSERT INTO qualifiedName query                                  #insertInto
    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName        #renameTable
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query                 #createView
    | DROP VIEW qualifiedName                                          #dropView
    | EXPLAIN (LPAREN explainOption (COMMA explainOption)* RPAREN)? statement  #explain
    | SHOW TABLES ((FROM | IN) qualifiedName)? (LIKE pattern=STRING)?  #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier)?                           #showSchemas
    | SHOW CATALOGS                                                    #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName                           #showColumns
    | DESCRIBE qualifiedName                                           #showColumns
    | DESC qualifiedName                                               #showColumns
    | SHOW FUNCTIONS                                                   #showFunctions
    | SHOW SESSION                                                     #showSession
    | SET SESSION qualifiedName EQ STRING                              #setSession
    | RESET SESSION qualifiedName                                      #resetSession
    | SHOW PARTITIONS (FROM | IN) qualifiedName
        (WHERE booleanExpression)?
        (ORDER BY sortItem (COMMA sortItem)*)?
        (LIMIT limit=INTEGER_VALUE)?                                   #showPartitions
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (COMMA sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
      (APPROXIMATE AT confidence=number CONFIDENCE)?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (COMMA expression)*  #inlineTable
    | LPAREN queryNoWith  RPAREN                 #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (COMMA selectItem)*
      (FROM relation (COMMA relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy+=expression (COMMA groupBy+=expression)*)?
      (HAVING having=booleanExpression)?
    ;

namedQuery
    : name=identifier (columnAliases)? AS LPAREN query RPAREN
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName DOT ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=relation
      | joinType JOIN right=relation joinCriteria
      | NATURAL joinType JOIN right=relation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING LPAREN identifier (COMMA identifier)* RPAREN
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType LPAREN percentage=expression RPAREN
        RESCALED?
        (STRATIFY ON LPAREN stratify+=expression (COMMA stratify+=expression)* RPAREN)?
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : LPAREN identifier (COMMA identifier)* RPAREN
    ;

relationPrimary
    : qualifiedName                                #tableName
    | LPAREN query RPAREN                                #subqueryRelation
    | UNNEST LPAREN expression (COMMA expression)* RPAREN  #unnest
    | LPAREN relation RPAREN                             #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : predicated                                                   #booleanDefault
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    | EXISTS LPAREN query RPAREN                                         #exists
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN LPAREN expression (COMMA expression)* RPAREN                        #inList
    | NOT? IN LPAREN query RPAREN                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                           #nullLiteral
    | interval                                                                       #intervalLiteral
    | identifier STRING                                                              #typeConstructor
    | number                                                                         #numericLiteral
    | booleanValue                                                                   #booleanLiteral
    | STRING                                                                         #stringLiteral
    | LPAREN expression (COMMA expression)+ RPAREN                                           #rowConstructor
    | ROW LPAREN expression (COMMA expression)* RPAREN                                       #rowConstructor
    | qualifiedName                                                                  #columnReference
    | qualifiedName LPAREN ASTERISK RPAREN over?                                           #functionCall
    | qualifiedName LPAREN (setQuantifier? expression (COMMA expression)*)? RPAREN over?     #functionCall
    | LPAREN query RPAREN                                                                  #subqueryExpression
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END         #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                         #searchedCase
    | CAST LPAREN expression AS type RPAREN                                                #cast
    | PASS                                                         #passExpression
    | TRY_CAST LPAREN expression AS type RPAREN                                            #cast
    | ARRAY LBRACKET (expression (COMMA expression)*)? RBRACKET                                  #arrayConstructor
    | value=primaryExpression LBRACKET index=valueExpression RBRACKET                          #subscript
    | name=CURRENT_DATE                                                              #specialDateTimeFunction
    | name=CURRENT_TIME (LPAREN precision=INTEGER_VALUE RPAREN)?                           #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP (LPAREN precision=INTEGER_VALUE RPAREN)?                      #specialDateTimeFunction
    | name=LOCALTIME (LPAREN precision=INTEGER_VALUE RPAREN)?                              #specialDateTimeFunction
    | name=LOCALTIMESTAMP (LPAREN precision=INTEGER_VALUE RPAREN)?                         #specialDateTimeFunction
    | SUBSTRING LPAREN valueExpression FROM valueExpression (FOR valueExpression)? RPAREN  #substring
    | EXTRACT LPAREN identifier FROM valueExpression RPAREN                                #extract
    | LPAREN expression RPAREN                                                             #parenthesizedExpression
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE STRING    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? STRING from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type
    : type ARRAY
    | ARRAY LT type GT
    | MAP LT type COMMA type GT
    | simpleType
    ;

simpleType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER LPAREN
        (PARTITION BY partition+=expression (COMMA partition+=expression)*)?
        (ORDER BY sortItem (COMMA sortItem)*)?
        windowFrame?
      RPAREN
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame // expression should be unsignedLiteral
    ;


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)  #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED)     #explainType
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    | DIMENSION_IDENTIFIER   #dimensionIdentifier
    ;

quotedIdentifier
    : QUOTED_IDENTIFIER
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS | SESSION
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | MAP
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | JSON | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | SET | RESET
    | VIEW | REPLACE
    | IF | NULLIF | COALESCE
    ;
