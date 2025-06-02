# -*- coding: utf-8 -*-
"""
Parse cmake listfiles and emit an html file containing semantic and
syntactic annotations.
"""

from __future__ import unicode_literals

import io
import logging
import os

from cmakelang import lex
from cmakelang.parse.common import TreeNode


def dump_html(node, outfile):
  """
  Write to `outfile` an html annoted version of the listfile which has been
  parsed into the parse tree rooted at `node`
  """

  if isinstance(node, TreeNode):
    outfile.write('<span class="cmf-{}">'.format(node.node_type.name))
    for child in node.children:
      dump_html(child, outfile)
    outfile.write('</span>')
  elif isinstance(node, lex.Token):
    outfile.write('<span class="cmf-{}">'.format(node.type.name))
    outfile.write(node.spelling)
    outfile.write('</span>')


def get_html(node, fullpage=False):
  """
  Return a string containing html markup of the annoted listfile which has
  been parsed into the parse tree rooted at `node`.
  """

  outfile = io.StringIO()
  dump_html(node, outfile)
  content = outfile.getvalue()
  if not fullpage:
    return content

  tpl_kwargs = {"content": content}

  try:
    import jinja2
  except ImportError:
    logging.error(
        "Cannot import jinja. Please install jinja in your python environment"
        " to use the fullpage html renderer")
    return None

  thisdir = os.path.realpath(os.path.dirname(__file__))
  tpldir = os.path.join(thisdir, "templates")
  env = jinja2.Environment(
      loader=jinja2.FileSystemLoader(tpldir),
      autoescape=jinja2.select_autoescape(["html"])
  )

  stylesheet_path = os.path.join(tpldir, "style.css")

  with io.open(stylesheet_path, "r", encoding="utf-8") as infile:
    tpl_kwargs["stylesheet"] = infile.read()

  template = env.get_template("layout.html.tpl")
  return template.render(**tpl_kwargs)
