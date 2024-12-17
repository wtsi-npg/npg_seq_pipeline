#!/bin/bash
#
# Once installed, the Conda environment can be activated by running:
#
# source $CONDA_HOME/etc/profile.d/conda.sh
#
# conda activate

set -ex

MINIFORGE_VERSION="24.9.2-0"
MINIFORGE_SHA256="ca8c544254c40ae5192eb7db4e133ff4eb9f942a1fec737dba8205ac3f626322"

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

curl -sSL "https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-${MINIFORGE_VERSION}-Linux-x86_64.sh" -o ./miniforge.sh
sha256sum ./miniforge.sh | grep "$MINIFORGE_SHA256"
/bin/bash ./miniforge.sh -b -p "$CONDA_HOME"
rm ./miniforge.sh
