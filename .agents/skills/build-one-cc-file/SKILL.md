---
name: build-one-cc-file
description: Build a single C++ file. Use when a single .cc file needs to be compiled to check for errors.
license: Apache-2.0
metadata:
  version: "1.0"
---

## Build One C++ File

Use build-one-cc-file when needing to compile a single `*.cc` C++ file to check for
errors. This is useful when making changes to a single file and wanting to quickly check
for compilation errors without building the entire project.

When using this skill, do not use the compile tool or run Ninja.

## Available Scripts
- `scripts/omake.sh` - Script to build a single C++ file. Run with the `--help` argument
  to see usage instructions. Run with the `--dry-run` argument to see the exact command
  that would be executed without actually running it.

  Pass exactly one argument: the source basename only (no directory path and no `.cc`
  extension).

  The only parameter is the C++ file basename, not a path. Do not include folders or
  file extension. The script will search for the corresponding build.make file and invoke
  make on that file.

  Correct: `scripts/omake.sh rpc-trace`
  Incorrect: `scripts/omake.sh be/src/rpc/rpc-trace.cc`
  Incorrect: `scripts/omake.sh rpc-trace.cc`

  Before running, if you have a path like `be/src/rpc/rpc-trace.cc`, convert it to the
  basename `rpc-trace`.

  If `omake.sh` cannot find a matching build.make target, check that you passed basename
  only (for example `rpc-trace`), not a path or `.cc` filename.
