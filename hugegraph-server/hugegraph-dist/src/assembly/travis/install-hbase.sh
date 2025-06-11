#!/bin/bash
# FIXME: This script installs HBase 2.0.2, which requires Java 8.
# To make the CI environment fully Java 11 compatible, this script
# needs to be updated to install a newer version of HBase (e.g., 2.3.x or later)
# that supports Java 11. This change should be coordinated with updates
# to the HBase client dependencies and thorough testing.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -ev

TRAVIS_DIR=$(dirname $0)
CI_HBASE_LOG_DIR="$HOME/hbase_ci_logs"
echo "HBase CI log directory set to: ${CI_HBASE_LOG_DIR}"
HBASE_DOWNLOAD_ADDRESS="https://archive.apache.org/dist/hbase"
HBASE_VERSION="2.5.8"
HBASE_PACKAGE="hbase-${HBASE_VERSION}"
HBASE_TAR="${HBASE_PACKAGE}-bin.tar.gz"

# download hbase
if [ ! -f $HOME/downloads/${HBASE_TAR} ]; then
  sudo wget -q -O $HOME/downloads/${HBASE_TAR} ${HBASE_DOWNLOAD_ADDRESS}/${HBASE_VERSION}/${HBASE_TAR}
fi

# decompress hbase
sudo cp $HOME/downloads/${HBASE_TAR} ${HBASE_TAR} && tar xzf ${HBASE_TAR}

# using tmpfs for the Hbase data directory reduces travis test runtime
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
sudo ln -s /mnt/ramdisk /tmp/hbase

# config hbase
sudo cp -f $TRAVIS_DIR/hbase-site.xml ${HBASE_PACKAGE}/conf

ZK_DATA_DIR=/tmp/zookeeper # As defined in hbase-site.xml
echo "Preparing ZooKeeper data directory: $ZK_DATA_DIR"
if [ -d "$ZK_DATA_DIR" ]; then
    sudo rm -rf "$ZK_DATA_DIR"
    echo "Removed existing ZooKeeper data directory."
fi
sudo mkdir -p "$ZK_DATA_DIR"
# Attempt to chown to the current user.
# If start-hbase.sh is run as root via sudo, HBase/ZK itself will manage permissions.
# If it's run as a non-root user (even after sudo), this helps ensure the non-root user can write.
# This might need adjustment based on the exact user running the HBase process in CI.
sudo chown -R "$(whoami)" "$ZK_DATA_DIR" || echo "Chown failed, proceeding with default permissions for $ZK_DATA_DIR"
echo "Created ZooKeeper data directory."

# start hbase service
mkdir -p "${CI_HBASE_LOG_DIR}"
echo "Ensured HBase CI log directory exists: ${CI_HBASE_LOG_DIR}"
echo "Current JAVA_HOME before starting HBase: $JAVA_HOME"
echo "Forcing JAVA_HOME for HBase startup. Assuming the CI step for Java 8 setup has correctly set JAVA_HOME."
# Ensure that if JAVA_HOME is not set, we don't just pass an empty string.
# However, the CI step 'Install Java8 for backend' should have set it.
if [ -z "$JAVA_HOME" ]; then
    echo "Error: JAVA_HOME is not set prior to starting HBase. Cannot force Java 8." >&2
    exit 1 # Or handle error as appropriate
fi
sudo "JAVA_HOME=${JAVA_HOME}" "HBASE_LOG_DIR=${CI_HBASE_LOG_DIR}" "${HBASE_PACKAGE}/bin/start-hbase.sh"

echo "Sleeping for 30 seconds to allow HBase to attempt startup and generate logs..."
sleep 30

echo "Listing HBase log directory: ${CI_HBASE_LOG_DIR}"
ls -lR "${CI_HBASE_LOG_DIR}" || echo "Could not list HBase log directory: ${CI_HBASE_LOG_DIR}"

echo "Printing .log and .out files from ${CI_HBASE_LOG_DIR}:"
# Use a loop that's robust to spaces in filenames, though not expected here.
find "${CI_HBASE_LOG_DIR}" -type f \( -name "*.log" -o -name "*.out" \) -print0 | while IFS= read -r -d $'\0' file; do
    echo ">>>> Contents of ${file} <<<<"
    cat "${file}" || echo "Failed to cat ${file}"
    echo ">>>> End of ${file} <<<<"
done
echo "Finished printing HBase logs."
