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

"""Version compatibility utilities for PySpark 3.5+ and 4.0+ support."""

import logging

logger = logging.getLogger(__name__)


def get_pyspark_version():
    """Get PySpark version as a tuple of major and minor version numbers.
    
    Returns:
        tuple: (major, minor) version numbers
        
    Example:
        >>> get_pyspark_version()
        (3, 5)
    """
    try:
        import pyspark
        version_parts = pyspark.__version__.split('.')
        return (int(version_parts[0]), int(version_parts[1]))
    except (ImportError, ValueError, IndexError) as e:
        logger.warning(f"Could not determine PySpark version: {e}")
        # Default to 3.5 for backward compatibility
        return (3, 5)


# Global version detection
PYSPARK_VERSION = get_pyspark_version()
IS_PYSPARK_4_PLUS = PYSPARK_VERSION >= (4, 0)

logger.debug(f"Detected PySpark version: {PYSPARK_VERSION}, IS_PYSPARK_4_PLUS: {IS_PYSPARK_4_PLUS}")