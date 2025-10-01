# alma-scripts
Scripts to support Alma integrations and other projects

# Developer Information

## Build (first time) / rebuild (as needed)

`docker compose build`

This builds a Docker image, `alma-scripts-dev:latest`, which can be used for developing, testing, and running code.

## Dev container

This project comes with a basic dev container definition, in `.devcontainer/devcontainer.json`. It's known to work with VS Code,
and may work with other IDEs like PyCharm.  For VS Code, it also installs the Python, Black (formatter), and Flake8 (linter)
extensions.

The project's directory is available within the container at `/home/app_user/project`.

### Rebuilding the dev container

VS Code builds its own container from the base image. This container may not always get rebuilt when the base image is rebuilt
(e.g., if packages are changed via `requirements.txt`).

If needed, rebuild the dev container by:
1. Close VS Code and wait several seconds for the dev container to shut down (check via `docker ps`).
2. Delete the dev container.
   1. `docker images | grep vsc-alma-scripts` # vsc-alma-scripts-LONG_HEX_STRING-uid
   2. `docker image rm -f vsc-alma-scripts-LONG_HEX_STRING-uid`
3. Start VS Code as usual.

## Running code

Running code from a VS Code terminal within the dev container should just work, e.g.: `python some_script.py` (whatever the specific program is).

Otherwise, run a program via docker compose.  From the project directory:

```
# Start the system
$ docker compose up -d

# Open a shell in the container
$ docker compose exec dev bash

# Open a Python shell in the container
$ docker compose exec dev python
```

### Running tests

To run tests (if any...):
```
$ docker compose exec dev python -m unittest
```

### Obsolete folder
The `obsolete` sub-directory contains scripts that were written as one-offs or were otherwise made obsolete, but are useful to keep around as examples.
