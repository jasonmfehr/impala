# Project Overview

Apache Impala is the open source, native analytic database for open data and table formats Impala provides low latency and high concurrency for BI/analytic queries on the Hadoop ecosystem, including Iceberg, open data formats, and most cloud storage options. Impala also scales linearly, even in multitenant environments.

Impala is divided into several components:
- The Impala Daemon (impalad) servers as both query coordinator and executor. Typically, each deamon instance is either a coordinator or an executor, but not both (although it is allowed for a single daemon to be both). Clients connect to impalad coordinators. This daemon handles query planning and coordination between all executors and is written in C++ for query handling and Java for query planning. It also manages metadata caching and retrieval from the catalogd.
- The Impala Catalog Daemon (catalogd), written mostly in Java with some C++ for network communuication, which manages metadata for all Impala daemons in a cluster. It is responsible for loading and distributing table and database metadata to all impalad. The catalogd serves as a caching layer over other catalogs (such as Hive Metastore and Iceberg REST catalogs).
- The Impala State Store (statestored), written in C++, which tracks the health and status of all Impala daemons in a cluster.
- The Impala Shell (impala-shell) which is a command-line interface, written in Python, for connecting to and interacting with Impala daemons.

## Folder Structure

- `/be`: The backend codebase, written in C++, which includes the core query execution engine, storage engine, and other low-level components. CMake is used to manage the build process. CTest and GoogleTest are used for unit and integration tests.
- `/bin`: Contains various utility scripts and executables for developing, building, and testing Impala.
- `/common`: Serialization library definitions for Thrift, Flatbuffers, and Protobuf used across different components of Impala.
- `/docker`: Dockerfiles and related resources for building container images for Impala runtime components. This directory is not used during the development or build process of Impala itself.
- `/fe`: The frontend codebase, written in Java, which is the query planner. Maven is used to manage the build process and all Java dependencies. The parent pom is located at `java/pom.xml`. JUnit is used for unit and integration tests. Java 17 is the required JDK version for building and running the frontend.
- `/java`: Other Java projects used during development, testing, and runtime. Each subfolder is a separate Maven sub-project under the parent pom `java/pom.xml`. Java 17 is the required JDK version for building and running these projects.
- `/shell": The Impala Shell codebase, written in Python. This directory contains the code for the command-line interface used to interact with Impala.
- `/testdata`: Data used during testing, including sample tables, datasets, and other resources needed for running tests. Also contains python files for loading existing datasets into an Impala instance.
- `/tests`: Integration and end-to-end tests for Impala written in Python. These tests cover various functionalities and components of Impala to ensure correctness and performance.
- `/toolchain`: Compiled libraries used by Impala during the build process.
- `/www`: The web UI codebase which provides a graphical interface for each Impala daemon for monitoring and managing Impala clusters.

## Coding Standards

- Most files have a hard line length limit of 90 characters. The files excluded from this rule can be found in `bin/jenkins/critique-gerrit-review.py` in the `EXCLUDE_FILE_PATTERNS` and `EXCLUDE_THRIFT_FILES` lists.
- When breaking lines longer than 90 characters, prefer breaking at a higher-level syntactic construct (e.g. after a comma, before an operator, after an opening parenthesis) rather than in the middle of a token while also keeping the total number of lines to a minimum. Also, indent the continued line 4 spaces from the column where the original line starts to visually distinguish it from a new line.
- Use 2 spaces instead of tabs for indentation.
- Remove all spaces at the end of each line.
- Blank lines should not contain any spaces or tabs.
- Use Google C++ Style Guide for C++ code in the `/be` directory with these exceptions:
  - Self-container Headers: We allow putting inline functions in separate files, using the suffix `.inline.h`. This can speed up compile times by reducing the volume of code to be compiled and reducing dependencies between headers.
  - Header include guards: Use `#pragma once` for include guards instead of traditional C macro include guards.
  - Constant Nameing: We use UPPER_CASE for constants names instead of kConstantName.
  - Namespace Scoping:  The `using namespace` directive is allowed in .cc files where it greatly reduces code volume.
  - Formatting: Use the rules defined in the `/.clang-format` for for automatic code formatting.
  - If Conditions Format: Only use a single line for if conditions when the entire if statement (including body) fits within the 90 character limit. Otherwise, use curly braces and put the body on a new line even if the body is a single statement.
- Most new files must include the Apache 2.0 License header. Exceptions are made where comments are not allowed (such as markdown and json files). Copy the license header from an existing file without modifications when adding a new file.
