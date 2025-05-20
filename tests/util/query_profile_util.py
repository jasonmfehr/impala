# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

import re


def parse_db_user(profile_text):
  user = re.search(r'\n\s+User:\s+(.*?)\n', profile_text)
  assert user is not None, "User not found in query profile"
  return user.group(1)


def parse_session_id(profile_text):
  """Parses the session id from the query profile text."""
  match = re.search(r'\n\s+Session ID:\s+(.*)\n', profile_text)
  assert match is not None, "Session ID not found in query profile"
  return match.group(1)
