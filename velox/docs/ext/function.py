# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The Function common functions."""

from __future__ import annotations

import ast
import re
from inspect import Parameter
from typing import NamedTuple

from docutils import nodes
from docutils.nodes import Element, Node
from sphinx import addnodes
from sphinx.addnodes import desc_signature, pending_xref

from sphinx.environment import BuildEnvironment
from sphinx.locale import _
from sphinx.util.inspect import signature_from_str

# REs for Function signatures
function_sig_re = re.compile(
    r"""^ ([\w.]*\.)?            # class name(s)
          (\w+)  \s*             # thing name
          (?: \(\s*(.*)\s*\)     # optional: arguments
           (?:\s* -> \s* (.*))?  #           return annotation
          )? $                   # and nothing more
          """,
    re.VERBOSE,
)

pairindextypes = {
    "module": _("module"),
    "keyword": _("keyword"),
    "operator": _("operator"),
    "object": _("object"),
    "exception": _("exception"),
    "statement": _("statement"),
    "builtin": _("built-in function"),
}


class ObjectEntry(NamedTuple):
    docname: str
    node_id: str
    objtype: str
    aliased: bool


class ModuleEntry(NamedTuple):
    docname: str
    node_id: str
    synopsis: str
    platform: str
    deprecated: bool


def parse_reftarget(
    reftarget: str, suppress_prefix: bool = False
) -> tuple[str, str, str, bool]:
    """Parse a type string and return (reftype, reftarget, title, refspecific flag)"""
    refspecific = False
    if reftarget.startswith("."):
        reftarget = reftarget[1:]
        title = reftarget
        refspecific = True
    elif reftarget.startswith("~"):
        reftarget = reftarget[1:]
        title = reftarget.split(".")[-1]
    elif suppress_prefix:
        title = reftarget.split(".")[-1]
    elif reftarget.startswith("typing."):
        title = reftarget[7:]
    else:
        title = reftarget

    if reftarget == "None" or reftarget.startswith("typing."):
        # typing module provides non-class types.  Obj reference is good to refer them.
        reftype = "obj"
    else:
        reftype = "class"

    return reftype, reftarget, title, refspecific


def type_to_xref(
    function_module: str,
    target: str,
    env: BuildEnvironment | None = None,
    suppress_prefix: bool = False,
) -> addnodes.pending_xref:
    """Convert a type string to a cross reference node."""
    if env:
        kwargs = {
            function_module + ":module": env.ref_context.get(
                function_module + ":module"
            ),
            function_module + ":class": env.ref_context.get(function_module + ":class"),
        }
    else:
        kwargs = {}

    reftype, target, title, refspecific = parse_reftarget(target, suppress_prefix)
    contnodes = [nodes.Text(title)]

    return pending_xref(
        "",
        *contnodes,
        refdomain=function_module,
        reftype=reftype,
        reftarget=target,
        refspecific=refspecific,
        **kwargs,
    )


def parse_annotation(
    function_module: str, annotation: str, env: BuildEnvironment | None
) -> list[Node]:
    """Parse type annotation."""

    def unparse(node: ast.AST) -> list[Node]:
        if isinstance(node, ast.Attribute):
            return [nodes.Text(f"{unparse(node.value)[0]}.{node.attr}")]
        elif isinstance(node, ast.BinOp):
            result: list[Node] = unparse(node.left)
            result.extend(unparse(node.op))
            result.extend(unparse(node.right))
            return result
        elif isinstance(node, ast.BitOr):
            return [
                addnodes.desc_sig_space(),
                addnodes.desc_sig_punctuation("", "|"),
                addnodes.desc_sig_space(),
            ]
        elif isinstance(node, ast.Constant):
            if node.value is Ellipsis:
                return [addnodes.desc_sig_punctuation("", "...")]
            elif isinstance(node.value, bool):
                return [addnodes.desc_sig_keyword("", repr(node.value))]
            elif isinstance(node.value, int):
                return [addnodes.desc_sig_literal_number("", repr(node.value))]
            elif isinstance(node.value, str):
                return [addnodes.desc_sig_literal_string("", repr(node.value))]
            else:
                # handles None, which is further handled by type_to_xref later
                # and fallback for other types that should be converted
                return [nodes.Text(repr(node.value))]
        elif isinstance(node, ast.Expr):
            return unparse(node.value)
        elif isinstance(node, ast.Index):
            return unparse(node.value)
        elif isinstance(node, ast.Invert):
            return [addnodes.desc_sig_punctuation("", "~")]
        elif isinstance(node, ast.List):
            result = [addnodes.desc_sig_punctuation("", "[")]
            if node.elts:
                # check if there are elements in node.elts to only pop the
                # last element of result if the for-loop was run at least
                # once
                for elem in node.elts:
                    result.extend(unparse(elem))
                    result.append(addnodes.desc_sig_punctuation("", ","))
                    result.append(addnodes.desc_sig_space())
                result.pop()
                result.pop()
            result.append(addnodes.desc_sig_punctuation("", "]"))
            return result
        elif isinstance(node, ast.Module):
            return sum((unparse(e) for e in node.body), [])
        elif isinstance(node, ast.Name):
            return [nodes.Text(node.id)]
        elif isinstance(node, ast.Subscript):
            if getattr(node.value, "id", "") in {"Optional", "Union"}:
                return _unparse_pep_604_annotation(node)
            result = unparse(node.value)
            result.append(addnodes.desc_sig_punctuation("", "]"))
            result.extend(unparse(node.slice))
            result.append(addnodes.desc_sig_punctuation("", "]"))

            # Wrap the Text nodes inside brackets by literal node if the subscript is a Literal
            if result[0] in ("Literal", "typing.Literal"):
                for i, subnode in enumerate(result[1:], start=1):
                    if isinstance(subnode, nodes.Text):
                        result[i] = nodes.literal("", "", subnode)
            return result
        elif isinstance(node, ast.UnaryOp):
            return unparse(node.op) + unparse(node.operand)
        elif isinstance(node, ast.Tuple):
            if node.elts:
                result = []
                for elem in node.elts:
                    result.extend(unparse(elem))
                    result.append(addnodes.desc_sig_punctuation("", ","))
                    result.append(addnodes.desc_sig_space())
                result.pop()
                result.pop()
            else:
                result = [
                    addnodes.desc_sig_punctuation("", "("),
                    addnodes.desc_sig_punctuation("", ")"),
                ]

            return result
        else:
            raise SyntaxError  # unsupported syntax

    def _unparse_pep_604_annotation(node: ast.Subscript) -> list[Node]:
        subscript = node.slice
        if isinstance(subscript, ast.Index):
            subscript = subscript.value  # type: ignore[assignment]

        flattened: list[Node] = []
        if isinstance(subscript, ast.Tuple):
            flattened.extend(unparse(subscript.elts[0]))
            for elt in subscript.elts[1:]:
                flattened.extend(unparse(ast.BitOr()))
                flattened.extend(unparse(elt))
        else:
            # e.g. a Union[] inside an Optional[]
            flattened.extend(unparse(subscript))

        if getattr(node.value, "id", "") == "Optional":
            flattened.extend(unparse(ast.BitOr()))
            flattened.append(nodes.Text("None"))

        return flattened

    try:
        tree = ast.parse(annotation)
        result: list[Node] = []
        for node in unparse(tree):
            if isinstance(node, nodes.literal):
                result.append(node[0])
            elif isinstance(node, nodes.Text) and node.strip():
                if (
                    result
                    and isinstance(result[-1], addnodes.desc_sig_punctuation)
                    and result[-1].astext() == "~"
                ):
                    result.pop()
                    result.append(
                        type_to_xref(
                            function_module, str(node), env, suppress_prefix=True
                        )
                    )
                else:
                    result.append(type_to_xref(function_module, str(node), env))
            else:
                result.append(node)
        return result
    except SyntaxError:
        return [type_to_xref(function_module, annotation, env)]


def parse_arglist(
    function_module: str, arglist: str, env: BuildEnvironment | None = None
) -> addnodes.desc_parameterlist:
    """Parse a list of arguments using AST parser"""
    params = addnodes.desc_parameterlist(arglist)
    sig = signature_from_str("(%s)" % arglist)
    last_kind = None
    for param in sig.parameters.values():
        if param.kind != param.POSITIONAL_ONLY and last_kind == param.POSITIONAL_ONLY:
            # PEP-570: Separator for Positional Only Parameter: /
            params += addnodes.desc_parameter(
                "", "", addnodes.desc_sig_operator("", "/")
            )
        if param.kind == param.KEYWORD_ONLY and last_kind in (
            param.POSITIONAL_OR_KEYWORD,
            param.POSITIONAL_ONLY,
            None,
        ):
            # PEP-3102: Separator for Keyword Only Parameter: *
            params += addnodes.desc_parameter(
                "", "", addnodes.desc_sig_operator("", "*")
            )

        node = addnodes.desc_parameter()
        if param.kind == param.VAR_POSITIONAL:
            node += addnodes.desc_sig_operator("", "*")
            node += addnodes.desc_sig_name("", param.name)
        elif param.kind == param.VAR_KEYWORD:
            node += addnodes.desc_sig_operator("", "**")
            node += addnodes.desc_sig_name("", param.name)
        else:
            node += addnodes.desc_sig_name("", param.name)

        if param.annotation is not param.empty:
            children = parse_annotation(function_module, param.annotation, env)
            node += addnodes.desc_sig_punctuation("", ":")
            node += addnodes.desc_sig_space()
            node += addnodes.desc_sig_name("", "", *children)  # type: ignore
        if param.default is not param.empty:
            if param.annotation is not param.empty:
                node += addnodes.desc_sig_space()
                node += addnodes.desc_sig_operator("", "=")
                node += addnodes.desc_sig_space()
            else:
                node += addnodes.desc_sig_operator("", "=")
            node += nodes.inline(
                "", param.default, classes=["default_value"], support_smartquotes=False
            )

        params += node
        last_kind = param.kind

    if last_kind == Parameter.POSITIONAL_ONLY:
        # PEP-570: Separator for Positional Only Parameter: /
        params += addnodes.desc_parameter("", "", addnodes.desc_sig_operator("", "/"))

    return params


def pseudo_parse_arglist(signode: desc_signature, arglist: str) -> None:
    """ "Parse" a list of arguments separated by commas.

    Arguments can have "optional" annotations given by enclosing them in
    brackets.  Currently, this will split at any comma, even if it's inside a
    string literal (e.g. default argument value).
    """
    paramlist = addnodes.desc_parameterlist()
    stack: list[Element] = [paramlist]
    try:
        for argument in arglist.split(","):
            argument = argument.strip()
            ends_open = ends_close = 0
            while argument.startswith("["):
                stack.append(addnodes.desc_optional())
                stack[-2] += stack[-1]
                argument = argument[1:].strip()
            while argument.startswith("]"):
                stack.pop()
                argument = argument[1:].strip()
            while argument.endswith("]") and not argument.endswith("[]"):
                ends_close += 1
                argument = argument[:-1].strip()
            while argument.endswith("["):
                ends_open += 1
                argument = argument[:-1].strip()
            if argument:
                stack[-1] += addnodes.desc_parameter(
                    "", "", addnodes.desc_sig_name(argument, argument)
                )
            while ends_open:
                stack.append(addnodes.desc_optional())
                stack[-2] += stack[-1]
                ends_open -= 1
            while ends_close:
                stack.pop()
                ends_close -= 1
        if len(stack) != 1:
            raise IndexError
    except IndexError:
        # if there are too few or too many elements on the stack, just give up
        # and treat the whole argument list as one argument, discarding the
        # already partially populated paramlist node
        paramlist = addnodes.desc_parameterlist()
        paramlist += addnodes.desc_parameter(arglist, arglist)
        signode += paramlist
    else:
        signode += paramlist
