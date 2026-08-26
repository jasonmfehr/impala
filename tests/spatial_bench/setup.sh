#!/usr/bin/env bash
#
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

set -euo pipefail

# shellcheck disable=SC1091
. "${IMPALA_HOME}/bin/impala-config.sh"

SCALE_FACTOR="1"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale_factor)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --scale_factor"
        exit 1
      fi
      SCALE_FACTOR="$2"
      shift 2
      ;;
    --scale_factor=*)
      SCALE_FACTOR="${1#*=}"
      shift
      ;;
    -s)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for -s"
        exit 1
      fi
      SCALE_FACTOR="$2"
      shift 2
      ;;
    -s*)
      SCALE_FACTOR="${1#-s}"
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--scale_factor <0.1|1|10|100>]"
      exit 1
      ;;
  esac
done

case "${SCALE_FACTOR}" in
  0.1|1|10|100) ;;
  *)
    echo "Invalid value for --scale_factor: '${SCALE_FACTOR}'. " \
         "Allowed values are: 0.1, 1, 10, 100"
    exit 1
    ;;
esac

if [[ "${TARGET_FILESYSTEM:-}" != "hdfs" ]]; then
  echo "Target filesystem is not HDFS"
  exit 1
fi

pushd "${IMPALA_HOME}"
trap "popd || true" EXIT

TEST_DB="spatial_bench"
echo "[INFO] Dropping database '${TEST_DB}'."
./bin/impala-shell.sh -q "drop database if exists ${TEST_DB} cascade" 2>/dev/null || true
echo "[INFO] Creating database '${TEST_DB}'."
./bin/impala-shell.sh -q "create database ${TEST_DB}" 2>/dev/null

echo "[INFO] Determining warehouse root based on the location of the 'default' db."
WAREHOUSE_ROOT=$(./bin/impala-shell.sh \
  -q "describe database default" 2>/dev/null | \
  grep "hdfs://" | cut -d"|" -f3 | tr -d " ")
echo "[INFO] Warehouse root determined as '${WAREHOUSE_ROOT}'."

TESTDATA_REMOTE_DIR="${WAREHOUSE_ROOT}/${TEST_DB}-data"
echo "[INFO] Setting up test data directory at '${TESTDATA_REMOTE_DIR}'."
hdfs dfs -rm -r -f "${TESTDATA_REMOTE_DIR}"
hdfs dfs -mkdir -p "${TESTDATA_REMOTE_DIR}"

TEMPORARY_DIR="$(mktemp -d --tmpdir spatialbench.XXXXXXXXXX)"
echo "[INFO] Using temporary directory: ${TEMPORARY_DIR}"

echo "[INFO] Creating Python virtual environment in temporary directory."
impala-python3 -m venv "${TEMPORARY_DIR}/venv"

echo "[INFO] Activating Python virtual environment."
# shellcheck source=/dev/null
source "${TEMPORARY_DIR}/venv/bin/activate"
echo "[INFO] Using Python version: " "$(python --version)"

echo "[INFO] Installing Pip and Python dependencies."
python -m pip install --upgrade pip
python -m pip install -r "${IMPALA_HOME}/tests/spatial_bench/requirements.txt"

# echo "[INFO] Cloning Apache Sedona GitHub repository."
# git clone \
#   --branch master \
#   --single-branch \
#   --depth 1 \
#   https://github.com/apache/sedona \
#   "${TEMPORARY_DIR}/sedona"

HF_DATA_DIR="${TEMPORARY_DIR}/spatialbench-data"
DATASET_SUBDIR="v0.1.0/sf${SCALE_FACTOR}"
echo "[INFO] Downloading spatial bench data from '${DATASET_SUBDIR}' to" \
    "'${HF_DATA_DIR}' using Hugging Face CLI."
hf download apache-sedona/spatialbench \
  --repo-type dataset \
  --include "${DATASET_SUBDIR}/**" \
  --local-dir "${HF_DATA_DIR}"

echo "[INFO] Copying spatial bench data to the remote directory."
for file in $(find "${HF_DATA_DIR}/${DATASET_SUBDIR}/" -type d | grep -v "/$"); do
  TABLE_DIR=$(echo "[INFO] $file" | rev | cut -d"/" -f1 | rev)
  echo "[INFO] Creating HDFS directory for table '${TABLE_DIR}'."
  hdfs dfs -mkdir -p "${TESTDATA_REMOTE_DIR}/${TABLE_DIR}"

  echo "[INFO] Copying data for table '${TABLE_DIR}' to HDFS."
  hdfs dfs -put -f "$file"/* "${TESTDATA_REMOTE_DIR}/${TABLE_DIR}/"
done

#
# BUILDING Table
#
TBL="building"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      b_buildingkey BIGINT,
      b_name VARCHAR(65535),
      b_boundary STRING
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (b_buildingkey, b_name, b_boundary)" 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

#
# CUSTOMER Table
#
TBL="customer"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      c_custkey BIGINT,
      c_name VARCHAR(65535),
      c_address VARCHAR(65535),
      c_region VARCHAR(65535),
      c_nation VARCHAR(65535),
      c_phone VARCHAR(65535)
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (c_custkey, c_name, c_address, c_region, c_nation, c_phone)" 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

#
# DRIVER Table
#
TBL="driver"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      d_driverkey BIGINT,
      d_name VARCHAR(65535),
      d_address VARCHAR(65535),
      d_region VARCHAR(65535),
      d_nation VARCHAR(65535),
      d_phone VARCHAR(65535)
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (d_driverkey, d_name, d_address, d_region, d_nation, d_phone)" 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

#
# TRIP Table
#
TBL="trip"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "
    CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      t_tripkey BIGINT,
      t_custkey BIGINT,
      t_driverkey BIGINT,
      t_vehiclekey BIGINT,
      t_pickuptime TIMESTAMP,
      t_dropofftime TIMESTAMP,
      t_fare DECIMAL(15, 5),
      t_tip DECIMAL(15, 5),
      t_totalamount DECIMAL(15, 5),
      t_distance DECIMAL(15, 5),
      t_pickuploc STRING,
      t_dropoffloc STRING
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (t_tripkey, t_custkey, t_driverkey, t_vehiclekey, t_pickuploc, t_dropoffloc)
    " 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

#
# VEHICLE Table
#
TBL="vehicle"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      v_vehiclekey BIGINT,
      v_mfgr VARCHAR(65535),
      v_brand VARCHAR(65535),
      v_type VARCHAR(65535),
      v_comment VARCHAR(65535)
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (v_vehiclekey, v_mfgr, v_brand, v_type, v_comment)" 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

#
# ZONE Table
#
TBL="zone"
echo "[INFO] Creating '${TEST_DB}.${TBL}' table."
./bin/impala-shell.sh -q "CREATE EXTERNAL TABLE ${TEST_DB}.${TBL} (
      z_zonekey BIGINT,
      z_gersid VARCHAR(65535),
      z_country VARCHAR(65535),
      z_region VARCHAR(65535),
      z_name VARCHAR(65535),
      z_subtype VARCHAR(65535),
      z_boundary STRING
    )
    STORED AS PARQUET
    LOCATION '${TESTDATA_REMOTE_DIR}/${TBL}'" 2>/dev/null

echo "[INFO] Computing table/column stats for '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "COMPUTE STATS ${TEST_DB}.${TBL}
    (z_zonekey, z_country, z_region, z_name, z_subtype, z_boundary)" 2>/dev/null

echo "[INFO] Refreshing '${TEST_DB}.${TBL}'."
./bin/impala-shell.sh -q "REFRESH ${TEST_DB}.${TBL}" 2>/dev/null

echo "[INFO] Setup complete."
