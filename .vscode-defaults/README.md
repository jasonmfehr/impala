# VSCode Defaults

## Description
This folder contain default settings that provide a good set of defaults for using VSCode
to develop Impala.

* `cmake-kits.json`: Configures CMake to use the toolchain's C/C++ compiler.
* `launch.json`: Sets up debug launch configurations for Coordinator, Catalog, and JUnit
                 tests.
* `settings.json`: Main settings file that excludes unnecessary files from searching, the
                   explorer view, and Maven, includes for C++, and port forwarding during
                   remote development.

## How to Use

1. Copy the `.vscode-defaults` folder to `.vscode`.
2. Open the `${IMPALA_HOME}` folder in VSCode (do not use "Add Folder to Workspace").

## Port Forwarding

During remote development (where a local VSCode GUI is connected to a VSCode server
running on a remote host), ports will automatically be forwarded from the local machine to
the remote machine as processes are started. Run the VSCode command
"Ports: Focus on Ports View" to see which ports are being forwarded. VSCode will attempt
to use the same port number (e.g. `localhost:28000` will forward to `remotehost:28000`)
unless the local port is already in use at which point VSCode will pick a random local
port.
