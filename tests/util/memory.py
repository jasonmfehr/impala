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
#
# Utility functions for calculating common mathematical measurements. Note that although
# some of these functions are available in external python packages (ex. numpy), these
# are simple enough that it is better to implement them ourselves to avoid extra
# dependencies.

def assert_byte_str(expected_str, actual_bytes, msg, factor=1024):
  """Asserts a pretty printed memory string matches a specifiec number of bytes."""
  expected_parts = expected_str.split(' ')
  expected_unit = expected_parts[1]
  calc = float(actual_bytes)

  if expected_unit == 'B':
    calc /= 1
  elif expected_unit == 'KB' or expected_unit == 'K':
    calc /= factor
  elif expected_unit == 'MB' or expected_unit == 'M':
    calc /= factor**2
  elif expected_unit == 'GB' or expected_unit == 'G':
    calc /= factor**3
  else:
    raise ValueError("Invalid unit for byte string: {}".format(expected_str))

  # The differences between round in Python 2 and Python 3 do not matter here.
  # pylint: disable=round-builtin
  rnd = round(calc, 2)

  # Allow a tolerance of +- .01 of the final unit (kb, mb, or gb) to allow for rounding
  # differences between Impala and the Python tests.
  expected_min = float(expected_parts[0]) - 0.01
  expected_max = expected_min + 0.02

  assert expected_min <= rnd <= expected_max, "{0} -- expected: {1}, actual: {2}, " \
      "calculated: {3}".format(msg, expected_parts[0], actual_bytes, rnd)


def convert_to_bytes(mem_str, unit_combined=False, factor=1024):
  """Converts a pretty printed memory string into bytes. Since pretty printing removes
     precision, the result may not equal the original number of bytes. Returns an int.

     By default, the format of mem_str is to have a space between the number and the
     units, but setting the unit_combined causes this function to use the last two
     characters of mem_str as the units and all other characters as the number."""
  unit = ""
  calc = 0

  if unit_combined:
    unit = mem_str[-2:]
    calc = float(mem_str[:-2])
  else:
    split_str = mem_str.split(' ')
    unit = split_str[1]
    calc = float(split_str[0])

  if unit == 'B':
    calc *= 1
  elif unit == 'KB' or unit == 'K':
    calc *= factor
  elif unit == 'MB' or unit == 'M':
    calc *= factor**2
  elif unit == 'GB' or unit == 'G':
    calc *= factor**3
  else:
    raise ValueError("Invalid unit for byte string: {}".format(mem_str))

  return int(calc)
