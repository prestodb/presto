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
.PHONY: TypeSignature parser format-fix header-fix

TypeSignature: parser format-fix header-fix

parser:
	antlr -Dlanguage=Cpp -package facebook::presto::type -visitor -no-listener -o antlr TypeSignature.g4
	rm -rf .antlr
	find antlr | ../../../velox/scripts/license-header.py --header ../../../license.header -i  -
	find antlr | ../../../velox/scripts/license-header.py --header ../../../license.header -vk -

format-fix:
	../../../velox/scripts/check.py format tree antlr --fix
	../../../velox/scripts/check.py format tree antlr 

header-fix:
	../../../velox/scripts/check.py format tree antlr --fix
	../../../velox/scripts/check.py format tree antlr 
