%{
#include <FlexLexer.h>
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/type/parser/ParserUtil.h"
%}
%require "3.0.4"
%language "C++"

%define parser_class_name {Parser}
%define api.namespace {facebook::velox::type}
%define api.value.type variant
%parse-param {Scanner* scanner}
%define parse.error verbose

%code requires
{
    namespace facebook::velox::type {
        class Scanner;
    } // namespace facebook::velox::type
    namespace facebook::velox {
        class Type;
    } // namespace facebook::velox
    struct RowArguments {
       std::vector<std::string> names;
       std::vector<std::shared_ptr<const facebook::velox::Type>> types;
    };
} // %code requires

%code
{
    #include "velox/type/parser/Scanner.h"

    #define yylex(x) scanner->lex(x)
}

%token               LPAREN RPAREN COMMA PERIOD COLON ARRAY MAP ROW FUNCTION DECIMAL LBRACE RBRACE
%token <std::string> WORD VARIABLE QUOTED_ID WORD_WITH_PERIODS
%token <long long>   NUMBER SIGNED_INT
%token YYEOF         0

%nterm <std::shared_ptr<const Type>> type type_single_word
%nterm <std::shared_ptr<const Type>> special_type function_type decimal_type row_type array_type map_type variable_type custom_type_with_children enum_type
%nterm <RowArguments> type_list_opt_names
%nterm <std::vector<std::shared_ptr<const Type>>> type_list
%nterm <std::pair<std::string, std::shared_ptr<const Type>>> named_type
%nterm <std::vector<std::string>> type_with_spaces
%nterm <std::string> field_name enum_name enum_kind enum_map_entry enum_map_entries enum_map_entries_json

%start type_spec

%%

/* The grammar entry point. */
type_spec : type                 { scanner->setType($1); }
          | error                { yyerrok; }
          ;

type : type_single_word  { $$ = $1; }
     | type_with_spaces  { $$ = inferTypeWithSpaces($1, true).second; }
     ;

type_single_word : WORD         { $$ = typeFromString($1); } // Handles most primitive types (e.g. bigint, etc).
                 | special_type { $$ = $1; }

special_type : array_type     { $$ = $1; }
             | map_type       { $$ = $1; }
             | row_type       { $$ = $1; }
             | function_type  { $$ = $1; }
             | variable_type  { $$ = $1; }
             | decimal_type   { $$ = $1; }
             | custom_type_with_children { $$ = $1; }
             | enum_type { $$ = $1; }

/*
 * Types with spaces have at least two words. They are joined in an
 * std::vector here, and resolved by `inferTypeWithSpaces()`. The first
 * word is special to allow for tokens such as "map", "array", etc, to
 * be used as field names.
 */
type_with_spaces : type_with_spaces WORD  { $1.push_back($2); $$ = std::move($1); }
                 | field_name WORD        { $$.push_back($1); $$.push_back($2); }
                 ;

/* List of allowed field names. */
field_name : WORD     { $$ = $1; }
           | ARRAY    { $$ = "array"; }
           | MAP      { $$ = "map"; }
           | FUNCTION { $$ = "function"; }
           | DECIMAL  { $$ = "decimal"; }
           | ROW      { $$ = "row"; }
           | VARIABLE { $$ = $1; }
           ;

/*
 * Varchar and varbinary have an optional `(int)`
 * e.g. both `varchar` and `varchar(4)` are valid.
 */
variable_type : VARIABLE LPAREN NUMBER RPAREN  { $$ = typeFromString($1); }
              | VARIABLE                       { $$ = typeFromString($1); }
              ;

decimal_type : DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN { $$ = DECIMAL($3, $5); }
             ;

array_type : ARRAY LPAREN type RPAREN { $$ = ARRAY($3); }
           ;

map_type : MAP LPAREN type COMMA type RPAREN { $$ = MAP($3, $5); }
         ;

function_type : FUNCTION LPAREN type_list RPAREN { auto returnType = $3.back(); $3.pop_back();
                                                   $$ = FUNCTION(std::move($3), returnType); }

row_type : ROW LPAREN type_list_opt_names RPAREN  { $$ = ROW(std::move($3.names), std::move($3.types)); }
         ;

custom_type_with_children : WORD LPAREN type_list RPAREN { $$ = customTypeWithChildren($1, $3); }

/* Consecutive list of types, separated by a comma. */
type_list : type                   { $$.push_back($1); }
          | type_list COMMA type   { $1.push_back($3); $$ = std::move($1); }
          ;

/*
 * Consecutive list of types which can optionally have a "name".
 * Only allowed inside row definitions.
 */
type_list_opt_names : type_list_opt_names COMMA named_type   { $1.names.push_back($3.first);
                                                               $1.types.push_back($3.second);
                                                               $$.names = std::move($1.names);
                                                               $$.types = std::move($1.types); }
                    | named_type                             { $$.names.push_back($1.first); $$.types.push_back($1.second); }
                    ;

/*
 * Named type is a type definition with an optional name. The name can be
 * quoted. Since types with spaces are allowed, there is potential ambiguity
 * in definitions with multiple words, for example:
 *
 * > my type
 *
 * Is "my" the name and "type" the type, or "my type" is the type name? We first
 * check if there is a type matching all words ("my type"), and if not, check if
 * there is a type matching all but the first wor ("type") and assume the first
 * ("my") to be the field name. See `inferTypeWithSpaces()`.
 */
named_type : type_single_word        { $$ = std::make_pair("", $1); }
           | field_name special_type { $$ = std::make_pair($1, $2); }
           | type_with_spaces        { $$ = inferTypeWithSpaces($1, false); }
           | QUOTED_ID type          { $1.erase(0, 1); $1.pop_back(); $$ = std::make_pair($1, $2); }  // Remove the quotes.
           ;
/*
 * Enum types have a format of:
 * "test.enum.mood:BigintEnum(test.enum.mood{"CURIOUS":2, "HAPPY":0})"
 * where "test.enum.mood" is the enum name, "BigintEnum" is the enum kind,
 * and "CURIOUS":2, "HAPPY":0 are the enum values.
 * These values are passed as parameters to BIGINT_ENUM type.
 */
enum_map_entries : enum_map_entry { $$ = $1; }
            | enum_map_entries COMMA enum_map_entry { $$ = $1 + ", " + $3; }
            ;

/*
 * Formats the values map like "[["CURIOUS",-2], ["HAPPY",0]]"
 * so that it can be parsed as an array of pairs using folly::parseJson in getEnumType.
 */
enum_map_entries_json : LBRACE enum_map_entries RBRACE {$$ = "[" + $2 + "]"; }
                    ;

enum_map_entry : QUOTED_ID COLON SIGNED_INT {  $$ = "[" + $1 + "," + std::to_string($3) + "]"; }
               | QUOTED_ID COLON NUMBER     {  $$ = "[" + $1 + "," + std::to_string($3) + "]"; }
               | QUOTED_ID COLON QUOTED_ID  {  $$ = "[" + $1 + "," + $3 + "]"; }
               ;

enum_kind : WORD { if ($1 != "BigintEnum" && $1 != "VarcharEnum" )
                    {
                        std::string msg = "Invalid type " + $1 + ", expected BigintEnum or VarcharEnum";
                        error(msg.c_str());
                    }
                $$ = $1; }
                ;

enum_name : WORD_WITH_PERIODS { $$ = $1; }
          | WORD { $$ = $1; }
          ;

enum_type : enum_name COLON enum_kind LPAREN enum_name enum_map_entries_json RPAREN
          { $$ = getEnumType($3, $1, $6); }
          ;

%%

void facebook::velox::type::Parser::error(const std::string& msg) {
  VELOX_UNSUPPORTED("Failed to parse type [{}]. {}", scanner->input(), msg);
}
