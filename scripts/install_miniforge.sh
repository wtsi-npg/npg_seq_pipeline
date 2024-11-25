#!/bin/bash
#
# Once installed, the Conda environment can be activated by running:
#
# source $CONDA_HOME/etc/profile.d/conda.sh
#
# conda activate

set -ex

MINIFORGE_VERSION="24.9.0-0"
MINIFORGE_SHA256="77fb505f6266ffa1b5d59604cf6ba25948627e908928cbff148813957b1c28af"

CONDA_HOME=${CONDA_HOME:="$HOME/conda"}
export CONDA_HOME

CONDARC="$HOME/.condarc"
export CONDARC

cat <<EOF > "$CONDARC"
auto_update_conda: false
always_yes: true
ssl_verify: true
show_channel_urls: true

channels:
  - conda-forge
EOF

curl -sSL "https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Mambaforge-${MINIFORGE_VERSION}-Linux-x86_64.sh" -o ./miniforge.sh
sha256sum ./miniforge.sh | grep "$MINIFORGE_SHA256"
/bin/bash ./miniforge.sh -b -p "$CONDA_HOME"
rm ./miniforge.sh
