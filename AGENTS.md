<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Impala

## Project Overview

Apache Impala is the open source, native analytic database for open data and table formats. Impala provides low latency and high concurrency for BI/analytic queries on the Hadoop ecosystem, including Iceberg, open data formats, and most cloud storage options. Impala scales linearly, even in multitenant environments.

Impala is divided into several components:
- The Impala Daemon (impalad) serves as both query coordinator and executor. Typically, each daemon instance is either a coordinator or an executor, but not both (although it is allowed for a single daemon to be both). Clients connect to impalad coordinators. This daemon handles query planning and coordination between all executors and is written in C++ for query handling and Java for query planning. It also manages metadata caching and retrieval from the catalogd.
- The Impala Catalog Daemon (catalogd), written mostly in Java with some C++ for network communication, which manages metadata for all Impala daemons in a cluster. It is responsible for loading and distributing table and database metadata to all impalad. The catalogd serves as a caching layer over other catalogs (such as Hive Metastore and Iceberg REST catalogs).
- The Impala State Store (statestored), written in C++, which tracks the health and status of all Impala daemons in a cluster.
- The Impala Shell (impala-shell) which is a command-line interface, written in Python, for connecting to and interacting with Impala daemons.

## Folder Structure

- `be`: The backend codebase, written in C++, which includes the core query execution engine, storage engine, and other low-level components. CMake is used to manage the build process. CTest and GoogleTest are used for unit and integration tests.
- `bin`: Contains various utility scripts and executables for developing, building, and testing Impala.
- `common`: Serialization library definitions for Thrift, Flatbuffers, and Protobuf used across different components of Impala.
- `docker`: Dockerfiles and related resources for building container images for Impala runtime components. This directory is not used during the development or build process of Impala itself.
- `fe`: The frontend codebase, written in Java, which is the query planner. Maven is used to manage the build process and all Java dependencies. The parent pom is located at `java/pom.xml`. JUnit is used for unit and integration tests. Java 17 is the required JDK version for building and running the frontend.
- `java`: Other Java projects used during development, testing, and runtime. Each subfolder is a separate Maven sub-project under the parent pom `java/pom.xml`. Java 17 is the required JDK version for building and running these projects.
- `shell`: The Impala Shell codebase, written in Python. This directory contains the code for the command-line interface used to interact with Impala.
- `testdata`: Data used during testing, including sample tables, datasets, and other resources needed for running tests. Also contains python files for loading existing datasets into an Impala instance.
- `tests`: Integration and end-to-end tests for Impala written in Python. These tests cover various functionalities and components of Impala to ensure correctness and performance.
- `toolchain`: Compiled libraries used by Impala during the build process.
- `www`: The web UI codebase which provides a graphical interface for each Impala daemon for monitoring and managing Impala clusters.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` for the project's threat model, in-scope / out-of-scope declarations, and known non-findings before reporting issues.

## Coding Standards

- Most files have a hard line length limit of 90 characters. The files excluded from this rule can be found in `bin/jenkins/critique-gerrit-review.py` in the `EXCLUDE_FILE_PATTERNS` and `EXCLUDE_THRIFT_FILES` lists.
- When breaking lines longer than 90 characters, prefer breaking at a higher-level syntactic construct (e.g. after a comma, before an operator, after an opening parenthesis) rather than in the middle of a token while also keeping the total number of lines to a minimum. Also, indent the continued line 4 spaces from the column where the original line starts to visually distinguish it from a new line.
- Use 2 spaces instead of tabs for indentation.
- Remove all spaces at the end of each line.
- Blank lines should not contain any spaces or tabs.
- Use Google C++ Style Guide located at https://google.github.io/styleguide/cppguide.html for C++ code in the `/be` directory (files with extensions ending in *.h or *.cc) with these exceptions:
  - Self-container Headers: We allow putting inline functions in separate files, using the suffix `.inline.h`. This can speed up compile times by reducing the volume of code to be compiled and reducing dependencies between headers.
  - Header include guards: Use `#pragma once` for include guards instead of traditional C macro include guards.
  - Constant Naming: We use UPPER_CASE for constants names instead of kConstantName.
  - Namespace Scoping:  The `using namespace` directive is allowed in .cc files where it greatly reduces code volume.
  - Formatting: Use the rules defined in the `/.clang-format` for for automatic code formatting.
  - If Conditions Format: Only use a single line for if conditions when the entire if statement (including body) fits within the 90 character limit. Otherwise, use curly braces and put the body on a new line even if the body is a single statement.
- Most new files must include the Apache 2.0 License header. Exceptions are made where comments are not allowed (such as markdown and json files). Copy the license header from an existing file without modifications when adding a new file.

## Git Commit Message Standards

- The first line of all Git commit messages must start with the string `<jira_id>: ` where `<jira_id>` is the identified of a Jira from https://issues.apache.org/jira/projects/IMPALA/issues/.  `<jira_id>` will match the regular expression `IMPALA-\d{4,}`. Do not guess at the Jira id to use. Ask the user which Jira id to use.
- The footer of all commit messages must contain a line beginning with `Assisted-by:`. If this line is not present, ask the user if it should be added. If the user confirms it should be added, add a line to the end of the most recent commit message in the format `Assisted-by: <model> (<agent-name>)` where `<model>` is the name of the current model and `<agent-name>` is the name of the agent using this skill. If the user declines to add this line, proceed without adding it. If the user input is invalid, immediately abort and do not proceed. Gerrit requires `Change-Id:` to be in the final footer paragraph of the commit message. Keep all trailer lines contiguous in that last paragraph (for example, `Change-Id:` and `Assisted-by:` should be adjacent, with no blank line between them).

## Environment Configuration

Before running any commands using a shell, the environment must first be configured. Run the following command to properly configure the environment for running shell commands, building code, or running tests. `<git_repo_root>` must be the fully qualified path to the directory on disk that contains the root of this git repo:
`export IMPALA_HOME=<git_repo_root> && cd "${IMPALA_HOME}" && source "${IMPALA_HOME}/bin/impala-config.sh" && source "${IMPALA_HOME}/bin/set-classpath.sh"`

## Building

- To build the C++ code in the `be` folder and the Java code in the `fe` at the same time, use the `buildall.sh` script. Run `buildall.sh --help` to learn about its command line flags.
- To build only the C++ code under the `be` folder structure, run `make -j ${IMPALA_BUILD_THREADS} impalad`. Use this command when only code in the `be` folder structure has been modified and needs to be built.
- To build only the Java code under the `fe` folder structure, run `make -j ${IMPALA_BUILD_THREADS} java`. Use this command when only code in the `fe` folder structure has been modified and needs to be built.
- The C++ code has CTest tests nested under the `be/src` folder. These tests are in files with named `*-test.cc`. To build one of these test files, run `make <testname>` where `<testname>` is the name of the file without the `.cc` extension. For example, to build `mem-pool-test.cc`, run `make mem-pool-test`.
- To build individual projects under the `java` folder, `cd` to the project's root folder where its `pom.xml` file is located and run `mvn install`. Use this step only for projects under the `java` folder.

## Running Tests

- To run C++ CTest tests, locate the compiled binary associated with the test. These binaries will be under the `be/build/` folder structure and will have a name matching the `*-test.cc` file. The specific folder structure will depend on the build type of debug or release. For example, when built in DEBUG mode, the `be/src/runtime/mem-pool-test.cc` file will have a binary located at `be/build/debug/runtime/mem-pool-test`. Run this binary to execute the CTest tests in `mem-pool-test.cc`.
- To run Java JUnit tests, run the command `mvn test -Dtest="<target_test>"` where `<target_test>` is either a Java JUnit class name or a Java JUnit class name and test function separated by the `#` symbol. For example, to run all tests in the `LocalCatalogTest` class, run `mvn test -Dtest="LocalCatalogTest"`. To run only the `testDbs` test function from the `LocalCatalogTest` class, run `mvn test -Dtest="LocalCatalogTest#testDbs"`.
- To debug Java JUnit tests, run `mvn -Dmaven.surefire.debug="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 -Xnoagent -Djava.compiler=NONE" -fae -Dtest="<target_test>" test` where `<target_test>` follows the same pattern as running Java JUnit tests.
- To run Python based tests under the `tests` folder, run `impala-py.test <path_to_test>` where `<path_to_test>` is the relative path to a Python test file. For example, `impala-py.test tests/custom_cluster/test_otel_trace.py`. To run a single test within a Python test file, add `-k <test_name>` onto the `impala-py.test` command and provide the name of a function whose name begins with `test_`. For tests under the `tests/custom_cluster` folder, examine the test file contents to see if the annotation `@SkipIfExploration.is_not_exhaustive()` is present. Add `--exploration_strategy=exhaustive` to the `impala-py.test` command to run tests with this annotation.

## Using Skills

Skills are defined in the `.agents/skills` directory rooted at the repo root directory. When using scripts that have a relative path, search for these scripts relative to this skill's root directory.
For example, to use the `scripts/omake.sh` script for the `build-one-cc-file` skill, search for this script relative to the `.agents/skills/build-one-cc-file` directory.
