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

# noinspection PyUnresolvedReferences
from docutils import nodes, utils
# noinspection PyUnresolvedReferences
from sphinx.errors import SphinxError

GROUP_ID = 'com.facebook.presto'
ARTIFACTS = {
    'server': ('presto-server', 'tar.gz', None),
    'cli': ('presto-cli', 'jar', 'executable'),
    'jdbc': ('presto-jdbc', 'jar', None),
    'verifier': ('presto-verifier', 'jar', 'executable'),
    'benchmark-driver': ('presto-benchmark-driver', 'jar', 'executable'),
}


def maven_filename(artifact, version, packaging, classifier):
    classifier = '-' + classifier if classifier else ''
    return '%s-%s%s.%s' % (artifact, version, classifier, packaging)


def maven_download(group, artifact, version, packaging, classifier):
    base = 'https://repo1.maven.org/maven2/'
    group_path = group.replace('.', '/')
    filename = maven_filename(artifact, version, packaging, classifier)
    return base + '/'.join((group_path, artifact, version, filename))


def setup(app):
    # noinspection PyDefaultArgument,PyUnusedLocal
    def download_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):
        version = app.config.release

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text)
            return [], []

        artifact, packaging, classifier = ARTIFACTS[text]

        title = maven_filename(artifact, version, packaging, classifier)
        uri = maven_download(GROUP_ID, artifact, version, packaging, classifier)

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []
    app.add_role('maven_download', download_link_role)
