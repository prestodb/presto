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

needs_sphinx = '3.3.0'

extensions = [
    'sphinx_copybutton', 'download', 'issue', 'pr'
]

copyright = 'The Presto Foundation. All rights reserved. Presto is a registered trademark of LF Projects, LLC'

templates_path = ['templates']

source_suffix = '.rst'

master_doc = 'index'

project = u'Presto'

version = get_version()
release = version

exclude_patterns = ['_build']

highlight_language = 'sql'

rst_epilog = """
.. |presto_server_release| replace:: ``presto-server-{release}``
""".replace('{release}', release)

# -- Options for HTML output ---------------------------------------------------

html_theme = 'sphinx_material'

# Set link name generated in the top bar.
html_title = '%s %s Documentation' % (project, release)
html_logo = 'images/logo.png'
html_favicon = 'images/favicon.ico'

# doesn't seem to do anything
# html_baseurl = 'overview.html'

html_static_path = ['static']
html_css_files = [
    'presto.css',
]

templates_path = ['_templates']

html_add_permalinks = '#'
html_show_copyright = True
html_show_sphinx = False

html_sidebars = {
    "**": ['logo-text.html', 'globaltoc.html', 'localtoc.html', 'searchbox.html']
}

html_show_sourcelink = False

# Material theme options (see theme.conf for more information)
html_theme_options = {

# Set the name to appear in the left sidebar/header. If not provided, uses
# html_short_title if defined, or html_title
  #  'nav_title': 'Project Name',

    # Set your GA account ID to enable tracking
   'google_analytics_account': 'UA-82811140-44',

    # Specify a base_url used to generate sitemap.xml. If not
    # specified, then no sitemap will be built.
   'base_url': '/',

# Colors
# The theme color for mobile browsers. Hex color.
    'theme_color': '374665',
 # Primary colors:
# red, pink, purple, deep-purple, indigo, blue, light-blue, cyan,
# teal, green, light-green, lime, yellow, amber, orange, deep-orange,
# brown, grey, blue-grey, white. default is blue.
    'color_primary': 'grey',
    # Accent colors:
# red, pink, purple, deep-purple, indigo, blue, light-blue, cyan,
# teal, green, light-green, lime, yellow, amber, orange, deep-orange
    'color_accent': 'blue',

# Repository integration
# Set the repo url for the link to appear
    'repo_url': 'https://github.com/prestodb/presto',
    'repo_name': 'Presto',
    'repo_type': 'github',


# TOC Tree generation
# The maximum depth of the global TOC; set it to -1 to allow unlimited depth
'globaltoc_depth': 2,
# If true, TOC entries that are not ancestors of the current page are collapsed
'globaltoc_collapse': True,
# If true, the global TOC tree will also contain hidden entries
'globaltoc_includehidden': False,


# Include the master document at the top of the page in the breadcrumb bar.
# You must also set this to true if you want to override the rootrellink block, in which
# case the content of the overridden block will appear
# master_doc = True

# A list of dictionaries where each has three keys:
#   href: The URL or pagename (str)
#   title: The title to appear (str)
#   internal: Flag indicating to use pathto (bool)
# nav_links =

# Text to appear at the top of the home page in a "hero" div. Must be a
# dict[str, str] of the form pagename: hero text, e.g., {'index': 'text on index'}
# heroes =

# Enable the version dropdown feature. See the demo site for the structure
# of the json file required.
# 'version_dropdown': 'True',

# Text to use in the dropdown menu
# version_dropdown_text = Versions

# Optional dictionary that to use when populating the version dropdown.
# The key is the text that appears, and the value is the absolute path
# of the alternative versions
# version_info =

# Relative path to json file. The default is "versions.json" which assumes the
# file hosted in the root of the site. You can use other file names or locations, e.g.,
# "_static/old_versions.json"
# 'version_json': 'static/versions.json',

# Table classes to _not_ strip.  Must be a list. Classes on this list are *not*
# removed from tables. All other classes are removed, and only tables with outclasses
# are styled by default.
# table_classes =


}
