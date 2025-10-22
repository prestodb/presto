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
#

#
# Presto documentation build configuration file
#
# This file is execfile()d with the current directory set to its containing dir.
#

import os
import sys
import xml.dom.minidom

try:
    sys.dont_write_bytecode = True
except:
    pass

sys.path.insert(0, os.path.abspath('ext'))


def child_node(node, name):
    for i in node.childNodes:
        if (i.nodeType == i.ELEMENT_NODE) and (i.tagName == name):
            return i
    return None


def node_text(node):
    return node.childNodes[0].data


def maven_version(pom):
    dom = xml.dom.minidom.parse(pom)
    project = dom.childNodes[0]

    version = child_node(project, 'version')
    if version:
        return node_text(version)

    parent = child_node(project, 'parent')
    version = child_node(parent, 'version')
    return node_text(version)


def get_version():
    version = os.environ.get('PRESTO_VERSION', '').strip()
    return version or maven_version('../../../pom.xml')

# -- General configuration -----------------------------------------------------


needs_sphinx = '8.2.1'

extensions = [
    'sphinx_immaterial', 'sphinx_copybutton', 'download', 'issue', 'pr', 'sphinx.ext.autosectionlabel'
]

copyright = 'The Presto Foundation. All rights reserved. Presto is a registered trademark of LF Projects, LLC'

templates_path = ['_templates']

source_suffix = '.rst'

master_doc = 'index'

project = u'Presto'

# Set Author blank to avoid default value of 'unknown'
author = ''

version = get_version()
release = version

exclude_patterns = ['_build']

highlight_language = 'sql'

rst_epilog = """
.. |presto_server_release| replace:: ``presto-server-{release}``
.. |presto_router_release| replace:: ``presto-router-{release}``
""".replace('{release}', release)

# 'xelatex' natively supports Unicode
latex_engine = 'xelatex'

autosectionlabel_prefix_document = True

# -- Options for HTML output ---------------------------------------------------

html_theme = 'sphinx_immaterial'

# Set link name generated in the top bar.
html_title = '%s %s Documentation' % (project, release)
html_logo = 'images/logo.png'
html_favicon = 'images/favicon.ico'

# doesn't seem to do anything
# html_baseurl = 'overview.html'

html_static_path = ['.']

templates_path = ['_templates']

# Set the primary domain to js because if left as the default python
# the theme errors when functions aren't available in a python module
primary_domain = 'js'

html_add_permalinks = '#'
html_show_copyright = True
html_show_sphinx = False

object_description_options = [
    ("js:.*", dict(toc_icon_class=None, include_fields_in_toc=False)),
]

html_theme_options = {

    # Set your GA account ID to enable tracking
    "analytics": {
        "provider": "google",
        "property": "G-K7GB6F0LBZ"
    },
    'features': [
        'toc.follow',
        'toc.sticky',
        'content.code.copy',
    ],
    'palette': [
        {
            "media": "(prefers-color-scheme: light)",
            "scheme": "default",
            "primary": "black",
            "accent": "cyan",
            "toggle": {
                "icon": "material/toggle-switch-off-outline",
                "name": "Switch to dark mode",
            }
        },
        {
            "media": "(prefers-color-scheme: dark)",
            "scheme": "slate",
            "primary": "light-blue",
            "accent": "deep-orange",
            "toggle": {
                "icon": "material/toggle-switch",
                "name": "Switch to light mode",
            }
        },
    ],
    'edit_uri': 'blob/master/presto-docs/src/main/sphinx',

    # 'base_url': '/',
    'repo_url': 'https://github.com/prestodb/presto',
    'repo_name': 'Presto',

    # If true, TOC entries that are not ancestors of the current page are collapsed
    'globaltoc_collapse': True,
    "social": [
        {
            "icon": "fontawesome/brands/github",
            "link": "https://github.com/prestodb/presto",
        },
    ],
}
