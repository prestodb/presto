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

"""Generating iceberg function link :iceberg:func:`<name>`"""

from __future__ import annotations

from function import (
    function_sig_re,
    pairindextypes,
    parse_arglist,
    pseudo_parse_arglist,
    parse_annotation,
    ObjectEntry,
    ModuleEntry,
)
from typing import Any, Iterable, Iterator, Tuple, cast

from docutils import nodes
from docutils.nodes import Element, Node
from docutils.parsers.rst import directives
from sphinx import addnodes
from sphinx.addnodes import desc_signature, pending_xref
from sphinx.application import Sphinx
from sphinx.builders import Builder
from sphinx.directives import ObjectDescription
from sphinx.domains import Domain, Index, IndexEntry, ObjType
from sphinx.environment import BuildEnvironment
from sphinx.locale import _, __
from sphinx.roles import XRefRole
from sphinx.util import logging
from sphinx.util.docfields import Field
from sphinx.util.nodes import (
    find_pending_xref_condition,
    make_id,
    make_refnode,
)
from sphinx.util.typing import OptionSpec

logger = logging.getLogger(__name__)

function_module = "iceberg"


class IcebergObject(ObjectDescription[Tuple[str, str]]):
    """
    Description of a general Iceberg object.

    :cvar allow_nesting: Class is an object that allows for nested namespaces
    :vartype allow_nesting: bool
    """

    option_spec: OptionSpec = {
        "noindex": directives.flag,
        "noindexentry": directives.flag,
        "nocontentsentry": directives.flag,
        "module": directives.unchanged,
        "canonical": directives.unchanged,
        "annotation": directives.unchanged,
    }

    doc_field_types = [
        Field(
            "returnvalue",
            label=_("Returns"),
            has_arg=False,
            names=("returns", "return"),
        ),
    ]

    allow_nesting = False

    def get_signature_prefix(self, sig: str) -> list[nodes.Node]:
        """May return a prefix to put before the object name in the
        signature.
        """
        return []

    def needs_arglist(self) -> bool:
        """May return true if an empty argument list is to be generated even if
        the document contains none.
        """
        return False

    def handle_signature(self, sig: str, signode: desc_signature) -> tuple[str, str]:
        """Transform a Iceberg signature into RST nodes.
        Return (fully qualified name of the thing, classname if any).
        If inside a class, the current class name is handled intelligently:
        * it is stripped from the displayed name if present
        * it is added to the full name (return value) if not present
        """
        m = function_sig_re.match(sig)
        if m is None:
            raise ValueError
        prefix, name, arglist, retann = m.groups()

        # determine module and class name (if applicable), as well as full name
        modname = self.options.get("module", self.env.ref_context.get("iceberg:module"))
        classname = self.env.ref_context.get("iceberg:class")
        if classname:
            add_module = False
            if prefix and (prefix == classname or prefix.startswith(classname + ".")):
                fullname = prefix + name
                # class name is given again in the signature
                prefix = prefix[len(classname) :].lstrip(".")
            elif prefix:
                # class name is given in the signature, but different
                # (shouldn't happen)
                fullname = classname + "." + prefix + name
            else:
                # class name is not given in the signature
                fullname = classname + "." + name
        else:
            add_module = True
            if prefix:
                classname = prefix.rstrip(".")
                fullname = prefix + name
            else:
                classname = ""
                fullname = name

        signode["module"] = modname
        signode["class"] = classname
        signode["fullname"] = fullname

        sig_prefix = self.get_signature_prefix(sig)
        if sig_prefix:
            if type(sig_prefix) is str:
                raise TypeError(
                    "Python directive method get_signature_prefix()"
                    " must return a list of nodes."
                    f" Return value was '{sig_prefix}'."
                )
            else:
                signode += addnodes.desc_annotation(str(sig_prefix), "", *sig_prefix)

        if prefix:
            signode += addnodes.desc_addname(prefix, prefix)
        elif modname and add_module and self.env.config.add_module_names:
            nodetext = modname + "."
            signode += addnodes.desc_addname(nodetext, nodetext)

        signode += addnodes.desc_name(name, name)
        if arglist:
            try:
                signode += parse_arglist(function_module, arglist, self.env)
            except SyntaxError:
                # fallback to parse arglist original parser.
                # it supports to represent optional arguments (ex. "func(foo [, bar])")
                pseudo_parse_arglist(signode, arglist)
            except NotImplementedError as exc:
                logger.warning(
                    "could not parse arglist (%r): %s", arglist, exc, location=signode
                )
                pseudo_parse_arglist(signode, arglist)
        else:
            if self.needs_arglist():
                # for callables, add an empty parameter list
                signode += addnodes.desc_parameterlist()

        if retann:
            children = parse_annotation(function_module, retann, self.env)
            signode += addnodes.desc_returns(retann, "", *children)

        anno = self.options.get("annotation")
        if anno:
            signode += addnodes.desc_annotation(
                " " + anno, "", addnodes.desc_sig_space(), nodes.Text(anno)
            )

        return fullname, prefix

    def _object_hierarchy_parts(self, sig_node: desc_signature) -> tuple[str, ...]:
        if "fullname" not in sig_node:
            return ()
        modname = sig_node.get("module")
        fullname = sig_node["fullname"]

        if modname:
            return (modname, *fullname.split("."))
        else:
            return tuple(fullname.split("."))

    def get_index_text(self, modname: str, name: tuple[str, str]) -> str:
        """Return the text for the index entry of the object."""
        raise NotImplementedError("must be implemented in subclasses")

    def add_target_and_index(
        self, name_cls: tuple[str, str], sig: str, signode: desc_signature
    ) -> None:
        modname = self.options.get("module", self.env.ref_context.get("iceberg:module"))
        fullname = (modname + "." if modname else "") + name_cls[0]
        node_id = make_id(self.env, self.state.document, "", fullname)
        signode["ids"].append(node_id)
        self.state.document.note_explicit_target(signode)

        domain = cast(IcebergDomain, self.env.get_domain("iceberg"))
        domain.note_object(fullname, self.objtype, node_id, location=signode)

        canonical_name = self.options.get("canonical")
        if canonical_name:
            domain.note_object(
                canonical_name, self.objtype, node_id, aliased=True, location=signode
            )

        if "noindexentry" not in self.options:
            indextext = self.get_index_text(modname, name_cls)
            if indextext:
                self.indexnode["entries"].append(
                    ("single", indextext, node_id, "", None)
                )

    def before_content(self) -> None:
        """Handle object nesting before content

        For constructs that aren't nestable, the stack is bypassed, and instead
        only the most recent object is tracked. This object prefix name will be
        removed with :iceberg:meth:`after_content`.
        """
        prefix = None
        if self.names:
            # fullname and name_prefix come from the `handle_signature` method.
            # fullname represents the full object name that is constructed using
            # object nesting and explicit prefixes. `name_prefix` is the
            # explicit prefix given in a signature
            (fullname, name_prefix) = self.names[-1]
            if self.allow_nesting:
                prefix = fullname
            elif name_prefix:
                prefix = name_prefix.strip(".")
        if prefix:
            self.env.ref_context["iceberg:class"] = prefix
            if self.allow_nesting:
                classes = self.env.ref_context.setdefault("iceberg:classes", [])
                classes.append(prefix)
        if "module" in self.options:
            modules = self.env.ref_context.setdefault("iceberg:modules", [])
            modules.append(self.env.ref_context.get("iceberg:module"))
            self.env.ref_context["iceberg:module"] = self.options["module"]

    def after_content(self) -> None:
        """Handle object de-nesting after content

        If this class is a nestable object, removing the last nested class prefix
        ends further nesting in the object.

        If this class is not a nestable object, the list of classes should not
        be altered as we didn't affect the nesting levels in
        :iceberg:meth:`before_content`.
        """
        classes = self.env.ref_context.setdefault("iceberg:classes", [])
        if self.allow_nesting:
            try:
                classes.pop()
            except IndexError:
                pass
        self.env.ref_context["iceberg:class"] = (
            classes[-1] if len(classes) > 0 else None
        )
        if "module" in self.options:
            modules = self.env.ref_context.setdefault("iceberg:modules", [])
            if modules:
                self.env.ref_context["iceberg:module"] = modules.pop()
            else:
                self.env.ref_context.pop("iceberg:module")

    def _toc_entry_name(self, sig_node: desc_signature) -> str:
        if not sig_node.get("_toc_parts"):
            return ""

        config = self.env.app.config
        objtype = sig_node.parent.get("objtype")
        if config.add_function_parentheses and objtype in {"function", "method"}:
            parens = "()"
        else:
            parens = ""
        *parents, name = sig_node["_toc_parts"]
        if config.toc_object_entries_show_parents == "domain":
            return sig_node.get("fullname", name) + parens
        if config.toc_object_entries_show_parents == "hide":
            return name + parens
        if config.toc_object_entries_show_parents == "all":
            return ".".join(parents + [name + parens])
        return ""


class IcebergFunction(IcebergObject):
    """Description of a function."""

    option_spec: OptionSpec = IcebergObject.option_spec.copy()
    option_spec.update(
        {
            "async": directives.flag,
        }
    )

    def get_signature_prefix(self, sig: str) -> list[nodes.Node]:
        if "async" in self.options:
            return [addnodes.desc_sig_keyword("", "async"), addnodes.desc_sig_space()]
        else:
            return []

    def needs_arglist(self) -> bool:
        return True

    def add_target_and_index(
        self, name_cls: tuple[str, str], sig: str, signode: desc_signature
    ) -> None:
        super().add_target_and_index(name_cls, sig, signode)
        if "noindexentry" not in self.options:
            modname = self.options.get(
                "module", self.env.ref_context.get("iceberg:module")
            )
            node_id = signode["ids"][0]

            name, cls = name_cls
            if modname:
                text = _("%s() (in module %s)") % (name, modname)
                self.indexnode["entries"].append(("single", text, node_id, "", None))
            else:
                text = f"{pairindextypes['builtin']}; {name}()"
                self.indexnode["entries"].append(("pair", text, node_id, "", None))

    def get_index_text(self, modname: str, name_cls: tuple[str, str]) -> str | None:
        # add index in own add_target_and_index() instead.
        return None


class IcebergXRefRole(XRefRole):
    def process_link(
        self,
        env: BuildEnvironment,
        refnode: Element,
        has_explicit_title: bool,
        title: str,
        target: str,
    ) -> tuple[str, str]:
        refnode["iceberg:module"] = env.ref_context.get("iceberg:module")
        refnode["iceberg:class"] = env.ref_context.get("iceberg:class")
        if not has_explicit_title:
            title = title.lstrip(".")  # only has a meaning for the target
            target = target.lstrip("~")  # only has a meaning for the title
            # if the first character is a tilde, don't display the module/class
            # parts of the contents
            if title[0:1] == "~":
                title = title[1:]
                dot = title.rfind(".")
                if dot != -1:
                    title = title[dot + 1 :]
        # if the first character is a dot, search more specific namespaces first
        # else search builtins first
        if target[0:1] == ".":
            target = target[1:]
            refnode["refspecific"] = True
        return title, target


class IcebergModuleIndex(Index):
    """
    Index subclass to provide the Iceberg module index.
    """

    name = "modindex"
    localname = _("Iceberg Module Index")
    shortname = _("modules")

    def generate(
        self, docnames: Iterable[str] | None = None
    ) -> tuple[list[tuple[str, list[IndexEntry]]], bool]:
        content: dict[str, list[IndexEntry]] = {}
        # list of prefixes to ignore
        ignores: list[str] = self.domain.env.config["modindex_common_prefix"]
        ignores = sorted(ignores, key=len, reverse=True)
        # list of all modules, sorted by module name
        modules = sorted(
            self.domain.data["modules"].items(), key=lambda x: x[0].lower()
        )
        # sort out collapsible modules
        prev_modname = ""
        num_toplevels = 0
        for modname, (docname, node_id, synopsis, platforms, deprecated) in modules:
            if docnames and docname not in docnames:
                continue

            for ignore in ignores:
                if modname.startswith(ignore):
                    modname = modname[len(ignore) :]
                    stripped = ignore
                    break
            else:
                stripped = ""

            # we stripped the whole module name?
            if not modname:
                modname, stripped = stripped, ""

            entries = content.setdefault(modname[0].lower(), [])

            package = modname.split(".")[0]
            if package != modname:
                # it's a submodule
                if prev_modname == package:
                    # first submodule - make parent a group head
                    if entries:
                        last = entries[-1]
                        entries[-1] = IndexEntry(
                            last[0], 1, last[2], last[3], last[4], last[5], last[6]
                        )
                elif not prev_modname.startswith(package):
                    # submodule without parent in list, add dummy entry
                    entries.append(
                        IndexEntry(stripped + package, 1, "", "", "", "", "")
                    )
                subtype = 2
            else:
                num_toplevels += 1
                subtype = 0

            qualifier = _("Deprecated") if deprecated else ""
            entries.append(
                IndexEntry(
                    stripped + modname,
                    subtype,
                    docname,
                    node_id,
                    platforms,
                    qualifier,
                    synopsis,
                )
            )
            prev_modname = modname

        # apply heuristics when to collapse modindex at page load:
        # only collapse if number of toplevel modules is larger than
        # number of submodules
        collapse = len(modules) - num_toplevels < num_toplevels

        # sort by first letter
        sorted_content = sorted(content.items())

        return sorted_content, collapse


class IcebergDomain(Domain):
    """Iceberg domain."""

    name = "iceberg"
    label = "Iceberg"
    object_types: dict[str, ObjType] = {
        "function": ObjType(_("function"), "func", "obj"),
    }

    directives = {
        "function": IcebergFunction,
    }
    roles = {
        "func": IcebergXRefRole(fix_parens=True),
    }
    initial_data: dict[str, dict[str, tuple[Any]]] = {
        "objects": {},  # fullname -> docname, objtype
        "modules": {},  # modname -> docname, synopsis, platform, deprecated
    }
    indices = [
        IcebergModuleIndex,
    ]

    @property
    def objects(self) -> dict[str, ObjectEntry]:
        return self.data.setdefault("objects", {})  # fullname -> ObjectEntry

    def note_object(
        self,
        name: str,
        objtype: str,
        node_id: str,
        aliased: bool = False,
        location: Any = None,
    ) -> None:
        """Note a iceberg object for cross reference.

        .. versionadded:: 2.1
        """
        if name in self.objects:
            other = self.objects[name]
            if other.aliased and aliased is False:
                # The original definition found. Override it!
                pass
            elif other.aliased is False and aliased:
                # The original definition is already registered.
                return
            else:
                # duplicated
                logger.warning(
                    __(
                        "duplicate object description of %s, "
                        "other instance in %s, use :noindex: for one of them"
                    ),
                    name,
                    other.docname,
                    location=location,
                )
        self.objects[name] = ObjectEntry(self.env.docname, node_id, objtype, aliased)

    @property
    def modules(self) -> dict[str, ModuleEntry]:
        return self.data.setdefault("modules", {})  # modname -> ModuleEntry

    def note_module(
        self, name: str, node_id: str, synopsis: str, platform: str, deprecated: bool
    ) -> None:
        """Note a iceberg module for cross reference.

        .. versionadded:: 2.1
        """
        self.modules[name] = ModuleEntry(
            self.env.docname, node_id, synopsis, platform, deprecated
        )

    def clear_doc(self, docname: str) -> None:
        for fullname, obj in list(self.objects.items()):
            if obj.docname == docname:
                del self.objects[fullname]
        for modname, mod in list(self.modules.items()):
            if mod.docname == docname:
                del self.modules[modname]

    def merge_domaindata(self, docnames: list[str], otherdata: dict[str, Any]) -> None:
        # XXX check duplicates?
        for fullname, obj in otherdata["objects"].items():
            if obj.docname in docnames:
                self.objects[fullname] = obj
        for modname, mod in otherdata["modules"].items():
            if mod.docname in docnames:
                self.modules[modname] = mod

    def find_obj(
        self,
        env: BuildEnvironment,
        modname: str,
        classname: str,
        name: str,
        type: str | None,
        searchmode: int = 0,
    ) -> list[tuple[str, ObjectEntry]]:
        """Find a Iceberg object for "name", perhaps using the given module
        and/or classname.  Returns a list of (name, object entry) tuples.
        """
        # skip parens
        if name[-2:] == "()":
            name = name[:-2]

        if not name:
            return []

        matches: list[tuple[str, ObjectEntry]] = []

        newname = None
        if searchmode == 1:
            if type is None:
                objtypes = list(self.object_types)
            else:
                objtypes = self.objtypes_for_role(type)
            if objtypes is not None:
                if modname and classname:
                    fullname = modname + "." + classname + "." + name
                    if (
                        fullname in self.objects
                        and self.objects[fullname].objtype in objtypes
                    ):
                        newname = fullname
                if not newname:
                    if (
                        modname
                        and modname + "." + name in self.objects
                        and self.objects[modname + "." + name].objtype in objtypes
                    ):
                        newname = modname + "." + name
                    elif (
                        name in self.objects and self.objects[name].objtype in objtypes
                    ):
                        newname = name
                    else:
                        # "fuzzy" searching mode
                        searchname = "." + name
                        matches = [
                            (oname, self.objects[oname])
                            for oname in self.objects
                            if oname.endswith(searchname)
                            and self.objects[oname].objtype in objtypes
                        ]
        else:
            # NOTE: searching for exact match, object type is not considered
            if name in self.objects:
                newname = name
            elif type == "mod":
                # only exact matches allowed for modules
                return []
            elif classname and classname + "." + name in self.objects:
                newname = classname + "." + name
            elif modname and modname + "." + name in self.objects:
                newname = modname + "." + name
            elif (
                modname
                and classname
                and modname + "." + classname + "." + name in self.objects
            ):
                newname = modname + "." + classname + "." + name
        if newname is not None:
            matches.append((newname, self.objects[newname]))
        return matches

    def resolve_xref(
        self,
        env: BuildEnvironment,
        fromdocname: str,
        builder: Builder,
        type: str,
        target: str,
        node: pending_xref,
        contnode: Element,
    ) -> Element | None:
        modname = node.get("iceberg:module")
        clsname = node.get("iceberg:class")
        searchmode = 1 if node.hasattr("refspecific") else 0
        matches = self.find_obj(env, modname, clsname, target, type, searchmode)

        if not matches and type == "attr":
            # fallback to meth (for property; Sphinx-2.4.x)
            # this ensures that `:attr:` role continues to refer to the old property entry
            # that defined by ``method`` directive in old reST files.
            matches = self.find_obj(env, modname, clsname, target, "meth", searchmode)
        if not matches and type == "meth":
            # fallback to attr (for property)
            # this ensures that `:meth:` in the old reST files can refer to the property
            # entry that defined by ``property`` directive.
            #
            # Note: _prop is a secret role only for internal look-up.
            matches = self.find_obj(env, modname, clsname, target, "_prop", searchmode)

        if not matches:
            return None
        elif len(matches) > 1:
            canonicals = [m for m in matches if not m[1].aliased]
            if len(canonicals) == 1:
                matches = canonicals
            else:
                logger.warning(
                    __("more than one target found for cross-reference %r: %s"),
                    target,
                    ", ".join(match[0] for match in matches),
                    type="ref",
                    subtype="python",
                    location=node,
                )
        name, obj = matches[0]

        if obj[2] == "module":
            return self._make_module_refnode(builder, fromdocname, name, contnode)
        else:
            # determine the content of the reference by conditions
            content = find_pending_xref_condition(node, "resolved")
            if content:
                children = content.children
            else:
                # if not found, use contnode
                children = [contnode]

            return make_refnode(builder, fromdocname, obj[0], obj[1], children, name)

    def resolve_any_xref(
        self,
        env: BuildEnvironment,
        fromdocname: str,
        builder: Builder,
        target: str,
        node: pending_xref,
        contnode: Element,
    ) -> list[tuple[str, Element]]:
        modname = node.get("iceberg:module")
        clsname = node.get("iceberg:class")
        results: list[tuple[str, Element]] = []

        # always search in "refspecific" mode with the :any: role
        matches = self.find_obj(env, modname, clsname, target, None, 1)
        multiple_matches = len(matches) > 1

        for name, obj in matches:
            if multiple_matches and obj.aliased:
                # Skip duplicated matches
                continue

            if obj[2] == "module":
                results.append(
                    (
                        "iceberg:mod",
                        self._make_module_refnode(builder, fromdocname, name, contnode),
                    )
                )
            else:
                # determine the content of the reference by conditions
                content = find_pending_xref_condition(node, "resolved")
                if content:
                    children = content.children
                else:
                    # if not found, use contnode
                    children = [contnode]

                results.append(
                    (
                        "iceberg:" + self.role_for_objtype(obj[2]),
                        make_refnode(
                            builder, fromdocname, obj[0], obj[1], children, name
                        ),
                    )
                )
        return results

    def _make_module_refnode(
        self, builder: Builder, fromdocname: str, name: str, contnode: Node
    ) -> Element:
        # get additional info for modules
        module = self.modules[name]
        title = name
        if module.synopsis:
            title += ": " + module.synopsis
        if module.deprecated:
            title += _(" (deprecated)")
        if module.platform:
            title += " (" + module.platform + ")"
        return make_refnode(
            builder, fromdocname, module.docname, module.node_id, contnode, title
        )

    def get_objects(self) -> Iterator[tuple[str, str, str, str, str, int]]:
        for modname, mod in self.modules.items():
            yield (modname, modname, "module", mod.docname, mod.node_id, 0)
        for refname, obj in self.objects.items():
            if obj.objtype != "module":  # modules are already handled
                if obj.aliased:
                    # aliased names are not full-text searchable.
                    yield (refname, refname, obj.objtype, obj.docname, obj.node_id, -1)
                else:
                    yield (refname, refname, obj.objtype, obj.docname, obj.node_id, 1)

    def get_full_qualified_name(self, node: Element) -> str | None:
        modname = node.get("iceberg:module")
        clsname = node.get("iceberg:class")
        target = node.get("reftarget")
        if target is None:
            return None
        else:
            return ".".join(filter(None, [modname, clsname, target]))


def setup(app: Sphinx) -> dict[str, Any]:
    app.setup_extension("sphinx.directives")
    app.add_domain(IcebergDomain)

    return {
        "version": "builtin",
        "env_version": 3,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
