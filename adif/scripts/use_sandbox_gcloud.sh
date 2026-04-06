#!/bin/zsh

set -euo pipefail

SCRIPT_PATH=$0
SCRIPT_DIR=${SCRIPT_PATH:A:h}
PROJECT_ROOT=${SCRIPT_DIR:h}
SOURCE_CONFIG_DIR="${HOME}/.config/gcloud"
LOCAL_ROOT="${PROJECT_ROOT}/.codex-local"
LOCAL_CONFIG_DIR="${LOCAL_ROOT}/gcloud"
DEFAULT_PROJECT="${BQ_PROJECT:-looker-studio-pro-452620}"

copy_config() {
  mkdir -p "${LOCAL_CONFIG_DIR}"

  if [[ ! -d "${SOURCE_CONFIG_DIR}" ]]; then
    echo "Source gcloud config not found at ${SOURCE_CONFIG_DIR}" >&2
    return 1
  fi

  rsync -a "${SOURCE_CONFIG_DIR}/" "${LOCAL_CONFIG_DIR}/"
}

ensure_local_config() {
  if [[ ! -f "${LOCAL_CONFIG_DIR}/credentials.db" ]]; then
    copy_config
    return
  fi

  if [[ "${1:-}" == "--refresh-copy" ]]; then
    copy_config
  fi
}

print_refresh_help() {
  cat <<EOF
Local sandbox-safe Google Cloud config is ready at:
  ${LOCAL_CONFIG_DIR}

If Google asks you to sign in again, run either of these commands through this helper:
  zsh ${SCRIPT_PATH} gcloud auth login
  zsh ${SCRIPT_PATH} gcloud auth application-default login
EOF
}

main() {
  local refresh_requested="false"

  if [[ "${1:-}" == "--refresh-copy" ]]; then
    refresh_requested="true"
    shift
  fi

  if [[ "${refresh_requested}" == "true" ]]; then
    ensure_local_config --refresh-copy
  else
    ensure_local_config
  fi

  export CLOUDSDK_CONFIG="${LOCAL_CONFIG_DIR}"
  export GOOGLE_APPLICATION_CREDENTIALS="${LOCAL_CONFIG_DIR}/application_default_credentials.json"
  export BQ_PROJECT="${DEFAULT_PROJECT}"

  if [[ $# -eq 0 ]]; then
    cat <<EOF
Prepared sandbox-safe Google Cloud environment for this project.

Exports:
  CLOUDSDK_CONFIG=${CLOUDSDK_CONFIG}
  GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
  BQ_PROJECT=${BQ_PROJECT}

Run commands through this helper:
  zsh ${SCRIPT_PATH} gcloud auth list
  zsh ${SCRIPT_PATH} bq ls --project_id=${BQ_PROJECT}

To refresh the local copy from ~/.config/gcloud:
  zsh ${SCRIPT_PATH} --refresh-copy
EOF
    print_refresh_help
    return 0
  fi

  exec "$@"
}

main "$@"
