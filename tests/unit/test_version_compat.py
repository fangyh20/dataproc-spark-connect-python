# Copyright 2024 Google LLC
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

import unittest
from unittest import mock

from google.cloud.dataproc_spark_connect.version_compat import (
    get_pyspark_version,
    IS_PYSPARK_4_PLUS,
    PYSPARK_VERSION,
)


class VersionCompatibilityTests(unittest.TestCase):

    def test_get_pyspark_version_with_valid_version(self):
        """Test version detection with a valid PySpark version."""
        with mock.patch('pyspark.__version__', '3.5.1'):
            version = get_pyspark_version()
            self.assertEqual(version, (3, 5))

        with mock.patch('pyspark.__version__', '4.0.0'):
            version = get_pyspark_version()
            self.assertEqual(version, (4, 0))

    def test_get_pyspark_version_with_import_error(self):
        """Test version detection when PySpark is not available."""
        with mock.patch('builtins.__import__', side_effect=ImportError("No module named 'pyspark'")):
            version = get_pyspark_version()
            self.assertEqual(version, (3, 5))  # Default fallback

    def test_get_pyspark_version_with_malformed_version(self):
        """Test version detection with malformed version string."""
        with mock.patch('pyspark.__version__', 'invalid.version'):
            version = get_pyspark_version()
            self.assertEqual(version, (3, 5))  # Default fallback

    def test_is_pyspark_4_plus_detection(self):
        """Test IS_PYSPARK_4_PLUS flag detection."""
        with mock.patch('pyspark.__version__', '3.5.1'):
            from google.cloud.dataproc_spark_connect import version_compat
            # Force reimport to get new version detection
            import importlib
            importlib.reload(version_compat)
            self.assertFalse(version_compat.IS_PYSPARK_4_PLUS)

        with mock.patch('pyspark.__version__', '4.0.0'):
            importlib.reload(version_compat)
            self.assertTrue(version_compat.IS_PYSPARK_4_PLUS)

    def test_current_environment_version_detection(self):
        """Test that version detection works in the current environment."""
        # This test verifies that the module can import and detect versions
        # without mocking, using the actual PySpark installation
        self.assertIsInstance(PYSPARK_VERSION, tuple)
        self.assertEqual(len(PYSPARK_VERSION), 2)
        self.assertIsInstance(PYSPARK_VERSION[0], int)
        self.assertIsInstance(PYSPARK_VERSION[1], int)
        self.assertIsInstance(IS_PYSPARK_4_PLUS, bool)


if __name__ == "__main__":
    unittest.main()