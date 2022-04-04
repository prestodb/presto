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

#include <boost/algorithm/string.hpp>
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"

// Generated from TypeSignature.g4 by ANTLR 4.9.3

#include "TypeSignatureVisitor.h"

#include "TypeSignatureParser.h"

using namespace antlrcpp;
using namespace facebook::presto::type;
using namespace antlr4;

TypeSignatureParser::TypeSignatureParser(TokenStream* input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(
      this, _atn, _decisionToDFA, _sharedContextCache);
}

TypeSignatureParser::~TypeSignatureParser() {
  delete _interpreter;
}

std::string TypeSignatureParser::getGrammarFileName() const {
  return "TypeSignature.g4";
}

const std::vector<std::string>& TypeSignatureParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& TypeSignatureParser::getVocabulary() const {
  return _vocabulary;
}

//----------------- StartContext
//------------------------------------------------------------------

TypeSignatureParser::StartContext::StartContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Type_specContext*
TypeSignatureParser::StartContext::type_spec() {
  return getRuleContext<TypeSignatureParser::Type_specContext>(0);
}

tree::TerminalNode* TypeSignatureParser::StartContext::EOF() {
  return getToken(TypeSignatureParser::EOF, 0);
}

size_t TypeSignatureParser::StartContext::getRuleIndex() const {
  return TypeSignatureParser::RuleStart;
}

antlrcpp::Any TypeSignatureParser::StartContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitStart(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::StartContext* TypeSignatureParser::start() {
  StartContext* _localctx =
      _tracker.createInstance<StartContext>(_ctx, getState());
  enterRule(_localctx, 0, TypeSignatureParser::RuleStart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(22);
    type_spec();
    setState(23);
    match(TypeSignatureParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_specContext
//------------------------------------------------------------------

TypeSignatureParser::Type_specContext::Type_specContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Named_typeContext*
TypeSignatureParser::Type_specContext::named_type() {
  return getRuleContext<TypeSignatureParser::Named_typeContext>(0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Type_specContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Type_specContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType_spec;
}

antlrcpp::Any TypeSignatureParser::Type_specContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType_spec(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Type_specContext* TypeSignatureParser::type_spec() {
  Type_specContext* _localctx =
      _tracker.createInstance<Type_specContext>(_ctx, getState());
  enterRule(_localctx, 2, TypeSignatureParser::RuleType_spec);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(27);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 0, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(25);
        named_type();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(26);
        type();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Named_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Named_typeContext::Named_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::IdentifierContext*
TypeSignatureParser::Named_typeContext::identifier() {
  return getRuleContext<TypeSignatureParser::IdentifierContext>(0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Named_typeContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Named_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleNamed_type;
}

antlrcpp::Any TypeSignatureParser::Named_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitNamed_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Named_typeContext* TypeSignatureParser::named_type() {
  Named_typeContext* _localctx =
      _tracker.createInstance<Named_typeContext>(_ctx, getState());
  enterRule(_localctx, 4, TypeSignatureParser::RuleNamed_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(29);
    identifier();
    setState(30);
    type();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypeContext
//------------------------------------------------------------------

TypeSignatureParser::TypeContext::TypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Simple_typeContext*
TypeSignatureParser::TypeContext::simple_type() {
  return getRuleContext<TypeSignatureParser::Simple_typeContext>(0);
}

TypeSignatureParser::Variable_typeContext*
TypeSignatureParser::TypeContext::variable_type() {
  return getRuleContext<TypeSignatureParser::Variable_typeContext>(0);
}

TypeSignatureParser::Array_typeContext*
TypeSignatureParser::TypeContext::array_type() {
  return getRuleContext<TypeSignatureParser::Array_typeContext>(0);
}

TypeSignatureParser::Map_typeContext*
TypeSignatureParser::TypeContext::map_type() {
  return getRuleContext<TypeSignatureParser::Map_typeContext>(0);
}

TypeSignatureParser::Row_typeContext*
TypeSignatureParser::TypeContext::row_type() {
  return getRuleContext<TypeSignatureParser::Row_typeContext>(0);
}

size_t TypeSignatureParser::TypeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType;
}

antlrcpp::Any TypeSignatureParser::TypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::TypeContext* TypeSignatureParser::type() {
  TypeContext* _localctx =
      _tracker.createInstance<TypeContext>(_ctx, getState());
  enterRule(_localctx, 6, TypeSignatureParser::RuleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(37);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 1, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(32);
        simple_type();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(33);
        variable_type();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(34);
        array_type();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(35);
        map_type();
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(36);
        row_type();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Simple_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Simple_typeContext::Simple_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Simple_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

tree::TerminalNode*
TypeSignatureParser::Simple_typeContext::TYPE_WITH_SPACES() {
  return getToken(TypeSignatureParser::TYPE_WITH_SPACES, 0);
}

size_t TypeSignatureParser::Simple_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleSimple_type;
}

antlrcpp::Any TypeSignatureParser::Simple_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitSimple_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Simple_typeContext* TypeSignatureParser::simple_type() {
  Simple_typeContext* _localctx =
      _tracker.createInstance<Simple_typeContext>(_ctx, getState());
  enterRule(_localctx, 8, TypeSignatureParser::RuleSimple_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(39);
    _la = _input->LA(1);
    if (!(_la == TypeSignatureParser::TYPE_WITH_SPACES

          || _la == TypeSignatureParser::WORD)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Variable_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Variable_typeContext::Variable_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Variable_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

std::vector<tree::TerminalNode*>
TypeSignatureParser::Variable_typeContext::NUMBER() {
  return getTokens(TypeSignatureParser::NUMBER);
}

tree::TerminalNode* TypeSignatureParser::Variable_typeContext::NUMBER(
    size_t i) {
  return getToken(TypeSignatureParser::NUMBER, i);
}

size_t TypeSignatureParser::Variable_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleVariable_type;
}

antlrcpp::Any TypeSignatureParser::Variable_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitVariable_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Variable_typeContext*
TypeSignatureParser::variable_type() {
  Variable_typeContext* _localctx =
      _tracker.createInstance<Variable_typeContext>(_ctx, getState());
  enterRule(_localctx, 10, TypeSignatureParser::RuleVariable_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(53);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 3, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(41);

        if (!(isVarToken())) {
          throw FailedPredicateException(this, " isVarToken() ");
        }
        setState(42);
        match(TypeSignatureParser::WORD);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(43);

        if (!(isVarToken())) {
          throw FailedPredicateException(this, " isVarToken() ");
        }
        setState(44);
        match(TypeSignatureParser::WORD);
        setState(45);
        match(TypeSignatureParser::T__0);
        setState(49);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == TypeSignatureParser::NUMBER) {
          setState(46);
          match(TypeSignatureParser::NUMBER);
          setState(51);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(52);
        match(TypeSignatureParser::T__1);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_listContext
//------------------------------------------------------------------

TypeSignatureParser::Type_listContext::Type_listContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<TypeSignatureParser::Type_specContext*>
TypeSignatureParser::Type_listContext::type_spec() {
  return getRuleContexts<TypeSignatureParser::Type_specContext>();
}

TypeSignatureParser::Type_specContext*
TypeSignatureParser::Type_listContext::type_spec(size_t i) {
  return getRuleContext<TypeSignatureParser::Type_specContext>(i);
}

size_t TypeSignatureParser::Type_listContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType_list;
}

antlrcpp::Any TypeSignatureParser::Type_listContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType_list(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Type_listContext* TypeSignatureParser::type_list() {
  Type_listContext* _localctx =
      _tracker.createInstance<Type_listContext>(_ctx, getState());
  enterRule(_localctx, 12, TypeSignatureParser::RuleType_list);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(55);
    type_spec();
    setState(60);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TypeSignatureParser::T__2) {
      setState(56);
      match(TypeSignatureParser::T__2);
      setState(57);
      type_spec();
      setState(62);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Row_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Row_typeContext::Row_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Row_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

TypeSignatureParser::Type_listContext*
TypeSignatureParser::Row_typeContext::type_list() {
  return getRuleContext<TypeSignatureParser::Type_listContext>(0);
}

size_t TypeSignatureParser::Row_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleRow_type;
}

antlrcpp::Any TypeSignatureParser::Row_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitRow_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Row_typeContext* TypeSignatureParser::row_type() {
  Row_typeContext* _localctx =
      _tracker.createInstance<Row_typeContext>(_ctx, getState());
  enterRule(_localctx, 14, TypeSignatureParser::RuleRow_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(63);

    if (!(isRowToken())) {
      throw FailedPredicateException(this, " isRowToken() ");
    }
    setState(64);
    match(TypeSignatureParser::WORD);
    setState(65);
    match(TypeSignatureParser::T__0);
    setState(66);
    type_list();
    setState(67);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Map_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Map_typeContext::Map_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Map_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

std::vector<TypeSignatureParser::TypeContext*>
TypeSignatureParser::Map_typeContext::type() {
  return getRuleContexts<TypeSignatureParser::TypeContext>();
}

TypeSignatureParser::TypeContext* TypeSignatureParser::Map_typeContext::type(
    size_t i) {
  return getRuleContext<TypeSignatureParser::TypeContext>(i);
}

size_t TypeSignatureParser::Map_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleMap_type;
}

antlrcpp::Any TypeSignatureParser::Map_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitMap_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Map_typeContext* TypeSignatureParser::map_type() {
  Map_typeContext* _localctx =
      _tracker.createInstance<Map_typeContext>(_ctx, getState());
  enterRule(_localctx, 16, TypeSignatureParser::RuleMap_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(69);

    if (!(isMapToken())) {
      throw FailedPredicateException(this, " isMapToken() ");
    }
    setState(70);
    match(TypeSignatureParser::WORD);
    setState(71);
    match(TypeSignatureParser::T__0);
    setState(72);
    type();
    setState(73);
    match(TypeSignatureParser::T__2);
    setState(74);
    type();
    setState(75);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Array_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Array_typeContext::Array_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Array_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Array_typeContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Array_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleArray_type;
}

antlrcpp::Any TypeSignatureParser::Array_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitArray_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Array_typeContext* TypeSignatureParser::array_type() {
  Array_typeContext* _localctx =
      _tracker.createInstance<Array_typeContext>(_ctx, getState());
  enterRule(_localctx, 18, TypeSignatureParser::RuleArray_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(77);

    if (!(isArrayToken())) {
      throw FailedPredicateException(this, " isArrayToken() ");
    }
    setState(78);
    match(TypeSignatureParser::WORD);
    setState(79);
    match(TypeSignatureParser::T__0);
    setState(80);
    type();
    setState(81);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext
//------------------------------------------------------------------

TypeSignatureParser::IdentifierContext::IdentifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::IdentifierContext::QUOTED_ID() {
  return getToken(TypeSignatureParser::QUOTED_ID, 0);
}

tree::TerminalNode* TypeSignatureParser::IdentifierContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

size_t TypeSignatureParser::IdentifierContext::getRuleIndex() const {
  return TypeSignatureParser::RuleIdentifier;
}

antlrcpp::Any TypeSignatureParser::IdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitIdentifier(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::IdentifierContext* TypeSignatureParser::identifier() {
  IdentifierContext* _localctx =
      _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 20, TypeSignatureParser::RuleIdentifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(83);
    _la = _input->LA(1);
    if (!(_la == TypeSignatureParser::WORD

          || _la == TypeSignatureParser::QUOTED_ID)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool TypeSignatureParser::sempred(
    RuleContext* context,
    size_t ruleIndex,
    size_t predicateIndex) {
  switch (ruleIndex) {
    case 5:
      return variable_typeSempred(
          antlrcpp::downCast<Variable_typeContext*>(context), predicateIndex);
    case 7:
      return row_typeSempred(
          antlrcpp::downCast<Row_typeContext*>(context), predicateIndex);
    case 8:
      return map_typeSempred(
          antlrcpp::downCast<Map_typeContext*>(context), predicateIndex);
    case 9:
      return array_typeSempred(
          antlrcpp::downCast<Array_typeContext*>(context), predicateIndex);

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::variable_typeSempred(
    Variable_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 0:
      return isVarToken();
    case 1:
      return isVarToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::row_typeSempred(
    Row_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 2:
      return isRowToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::map_typeSempred(
    Map_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 3:
      return isMapToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::array_typeSempred(
    Array_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 4:
      return isArrayToken();

    default:
      break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> TypeSignatureParser::_decisionToDFA;
atn::PredictionContextCache TypeSignatureParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN TypeSignatureParser::_atn;
std::vector<uint16_t> TypeSignatureParser::_serializedATN;

std::vector<std::string> TypeSignatureParser::_ruleNames = {
    "start",
    "type_spec",
    "named_type",
    "type",
    "simple_type",
    "variable_type",
    "type_list",
    "row_type",
    "map_type",
    "array_type",
    "identifier"};

std::vector<std::string> TypeSignatureParser::_literalNames = {
    "",
    "'('",
    "')'",
    "','"};

std::vector<std::string> TypeSignatureParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "TYPE_WITH_SPACES",
    "WORD",
    "QUOTED_ID",
    "NUMBER",
    "WHITESPACE"};

dfa::Vocabulary TypeSignatureParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> TypeSignatureParser::_tokenNames;

TypeSignatureParser::Initializer::Initializer() {
  for (size_t i = 0; i < _symbolicNames.size(); ++i) {
    std::string name = _vocabulary.getLiteralName(i);
    if (name.empty()) {
      name = _vocabulary.getSymbolicName(i);
    }

    if (name.empty()) {
      _tokenNames.push_back("<INVALID>");
    } else {
      _tokenNames.push_back(name);
    }
  }

  static const uint16_t serializedATNSegment0[] = {
      0x3,  0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964,
      0x3,  0xa,    0x58,   0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,  0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,  0x4,    0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,  0x8,    0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,  0x9,    0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,  0xc,    0x3,    0x2,    0x3,    0x2,    0x3,    0x2,    0x3,
      0x3,  0x3,    0x3,    0x5,    0x3,    0x1e,   0xa,    0x3,    0x3,
      0x4,  0x3,    0x4,    0x3,    0x4,    0x3,    0x5,    0x3,    0x5,
      0x3,  0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x28,
      0xa,  0x5,    0x3,    0x6,    0x3,    0x6,    0x3,    0x7,    0x3,
      0x7,  0x3,    0x7,    0x3,    0x7,    0x3,    0x7,    0x3,    0x7,
      0x7,  0x7,    0x32,   0xa,    0x7,    0xc,    0x7,    0xe,    0x7,
      0x35, 0xb,    0x7,    0x3,    0x7,    0x5,    0x7,    0x38,   0xa,
      0x7,  0x3,    0x8,    0x3,    0x8,    0x3,    0x8,    0x7,    0x8,
      0x3d, 0xa,    0x8,    0xc,    0x8,    0xe,    0x8,    0x40,   0xb,
      0x8,  0x3,    0x9,    0x3,    0x9,    0x3,    0x9,    0x3,    0x9,
      0x3,  0x9,    0x3,    0x9,    0x3,    0xa,    0x3,    0xa,    0x3,
      0xa,  0x3,    0xa,    0x3,    0xa,    0x3,    0xa,    0x3,    0xa,
      0x3,  0xa,    0x3,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xb,  0x3,    0xb,    0x3,    0xb,    0x3,    0xc,    0x3,    0xc,
      0x3,  0xc,    0x2,    0x2,    0xd,    0x2,    0x4,    0x6,    0x8,
      0xa,  0xc,    0xe,    0x10,   0x12,   0x14,   0x16,   0x2,    0x4,
      0x3,  0x2,    0x6,    0x7,    0x3,    0x2,    0x7,    0x8,    0x2,
      0x54, 0x2,    0x18,   0x3,    0x2,    0x2,    0x2,    0x4,    0x1d,
      0x3,  0x2,    0x2,    0x2,    0x6,    0x1f,   0x3,    0x2,    0x2,
      0x2,  0x8,    0x27,   0x3,    0x2,    0x2,    0x2,    0xa,    0x29,
      0x3,  0x2,    0x2,    0x2,    0xc,    0x37,   0x3,    0x2,    0x2,
      0x2,  0xe,    0x39,   0x3,    0x2,    0x2,    0x2,    0x10,   0x41,
      0x3,  0x2,    0x2,    0x2,    0x12,   0x47,   0x3,    0x2,    0x2,
      0x2,  0x14,   0x4f,   0x3,    0x2,    0x2,    0x2,    0x16,   0x55,
      0x3,  0x2,    0x2,    0x2,    0x18,   0x19,   0x5,    0x4,    0x3,
      0x2,  0x19,   0x1a,   0x7,    0x2,    0x2,    0x3,    0x1a,   0x3,
      0x3,  0x2,    0x2,    0x2,    0x1b,   0x1e,   0x5,    0x6,    0x4,
      0x2,  0x1c,   0x1e,   0x5,    0x8,    0x5,    0x2,    0x1d,   0x1b,
      0x3,  0x2,    0x2,    0x2,    0x1d,   0x1c,   0x3,    0x2,    0x2,
      0x2,  0x1e,   0x5,    0x3,    0x2,    0x2,    0x2,    0x1f,   0x20,
      0x5,  0x16,   0xc,    0x2,    0x20,   0x21,   0x5,    0x8,    0x5,
      0x2,  0x21,   0x7,    0x3,    0x2,    0x2,    0x2,    0x22,   0x28,
      0x5,  0xa,    0x6,    0x2,    0x23,   0x28,   0x5,    0xc,    0x7,
      0x2,  0x24,   0x28,   0x5,    0x14,   0xb,    0x2,    0x25,   0x28,
      0x5,  0x12,   0xa,    0x2,    0x26,   0x28,   0x5,    0x10,   0x9,
      0x2,  0x27,   0x22,   0x3,    0x2,    0x2,    0x2,    0x27,   0x23,
      0x3,  0x2,    0x2,    0x2,    0x27,   0x24,   0x3,    0x2,    0x2,
      0x2,  0x27,   0x25,   0x3,    0x2,    0x2,    0x2,    0x27,   0x26,
      0x3,  0x2,    0x2,    0x2,    0x28,   0x9,    0x3,    0x2,    0x2,
      0x2,  0x29,   0x2a,   0x9,    0x2,    0x2,    0x2,    0x2a,   0xb,
      0x3,  0x2,    0x2,    0x2,    0x2b,   0x2c,   0x6,    0x7,    0x2,
      0x2,  0x2c,   0x38,   0x7,    0x7,    0x2,    0x2,    0x2d,   0x2e,
      0x6,  0x7,    0x3,    0x2,    0x2e,   0x2f,   0x7,    0x7,    0x2,
      0x2,  0x2f,   0x33,   0x7,    0x3,    0x2,    0x2,    0x30,   0x32,
      0x7,  0x9,    0x2,    0x2,    0x31,   0x30,   0x3,    0x2,    0x2,
      0x2,  0x32,   0x35,   0x3,    0x2,    0x2,    0x2,    0x33,   0x31,
      0x3,  0x2,    0x2,    0x2,    0x33,   0x34,   0x3,    0x2,    0x2,
      0x2,  0x34,   0x36,   0x3,    0x2,    0x2,    0x2,    0x35,   0x33,
      0x3,  0x2,    0x2,    0x2,    0x36,   0x38,   0x7,    0x4,    0x2,
      0x2,  0x37,   0x2b,   0x3,    0x2,    0x2,    0x2,    0x37,   0x2d,
      0x3,  0x2,    0x2,    0x2,    0x38,   0xd,    0x3,    0x2,    0x2,
      0x2,  0x39,   0x3e,   0x5,    0x4,    0x3,    0x2,    0x3a,   0x3b,
      0x7,  0x5,    0x2,    0x2,    0x3b,   0x3d,   0x5,    0x4,    0x3,
      0x2,  0x3c,   0x3a,   0x3,    0x2,    0x2,    0x2,    0x3d,   0x40,
      0x3,  0x2,    0x2,    0x2,    0x3e,   0x3c,   0x3,    0x2,    0x2,
      0x2,  0x3e,   0x3f,   0x3,    0x2,    0x2,    0x2,    0x3f,   0xf,
      0x3,  0x2,    0x2,    0x2,    0x40,   0x3e,   0x3,    0x2,    0x2,
      0x2,  0x41,   0x42,   0x6,    0x9,    0x4,    0x2,    0x42,   0x43,
      0x7,  0x7,    0x2,    0x2,    0x43,   0x44,   0x7,    0x3,    0x2,
      0x2,  0x44,   0x45,   0x5,    0xe,    0x8,    0x2,    0x45,   0x46,
      0x7,  0x4,    0x2,    0x2,    0x46,   0x11,   0x3,    0x2,    0x2,
      0x2,  0x47,   0x48,   0x6,    0xa,    0x5,    0x2,    0x48,   0x49,
      0x7,  0x7,    0x2,    0x2,    0x49,   0x4a,   0x7,    0x3,    0x2,
      0x2,  0x4a,   0x4b,   0x5,    0x8,    0x5,    0x2,    0x4b,   0x4c,
      0x7,  0x5,    0x2,    0x2,    0x4c,   0x4d,   0x5,    0x8,    0x5,
      0x2,  0x4d,   0x4e,   0x7,    0x4,    0x2,    0x2,    0x4e,   0x13,
      0x3,  0x2,    0x2,    0x2,    0x4f,   0x50,   0x6,    0xb,    0x6,
      0x2,  0x50,   0x51,   0x7,    0x7,    0x2,    0x2,    0x51,   0x52,
      0x7,  0x3,    0x2,    0x2,    0x52,   0x53,   0x5,    0x8,    0x5,
      0x2,  0x53,   0x54,   0x7,    0x4,    0x2,    0x2,    0x54,   0x15,
      0x3,  0x2,    0x2,    0x2,    0x55,   0x56,   0x9,    0x3,    0x2,
      0x2,  0x56,   0x17,   0x3,    0x2,    0x2,    0x2,    0x7,    0x1d,
      0x27, 0x33,   0x37,   0x3e,
  };

  _serializedATN.insert(
      _serializedATN.end(),
      serializedATNSegment0,
      serializedATNSegment0 +
          sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

TypeSignatureParser::Initializer TypeSignatureParser::_init;
