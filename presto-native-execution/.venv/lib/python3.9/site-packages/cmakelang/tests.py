import unittest

# pylint: disable=wildcard-import
# pylint: disable=unused-wildcard-import
# pylint: disable=unused-import

from cmakelang.format.invocation_tests import TestInvocations
from cmakelang.format.layout_tests import TestCanonicalLayout
from cmakelang.lex.tests import TestSpecificLexings
from cmakelang.markup_tests import *
from cmakelang.parse.tests import TestCanonicalParse

from cmakelang.command_tests import (
    TestAddCustomCommand,
    TestComment,
    TestConditional,
    TestCustomCommand,
    TestExport,
    TestExternalProject,
    TestForeach,
    TestFile,
    TestInstall,
    TestSetTargetProperties,
    TestSet)

from cmakelang.command_tests.add_executable_tests \
    import TestAddExecutableCommand
from cmakelang.command_tests.add_library_tests \
    import TestAddLibraryCommand
from cmakelang.command_tests.misc_tests \
    import TestMiscFormatting
from cmakelang.contrib.validate_database \
    import TestContributorAgreements
from cmakelang.contrib.validate_pullrequest \
    import TestContribution
from cmakelang.lint.test.expect_tests import (
    ConfigTestCase,
    LintTests)
from cmakelang.lint.test.execution_tests import (
    TestFormatFiles)
from cmakelang.test.version_number_test \
    import TestVersionNumber
from cmakelang.test.command_db_test \
    import TestCommandDatabase
from cmakelang.test.config_include_test \
    import TestConfigInclude

if __name__ == '__main__':
  unittest.main()
