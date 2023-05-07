#!/bin/bash

# Ensure required environment variables are set
if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" || -z "${S3_SERVER_PATH}" ]]; then
  echo "Error: Required environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_SERVER_PATH) not set."
  exit 1
fi

export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

sudo echo "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" > ~/.s3fs.passwd

sudo s3fs flat-file-loader-dev:/Archive /opt/python_scripts/flat_file_loader/data/Archive -o passwd_file=~/.s3fs.passwd -o url=$S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other
sudo s3fs flat-file-loader-dev:/Logs /opt/python_scripts/flat_file_loader/data/Logs -o passwd_file=~/.s3fs.passwd -o url=$S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other
sudo s3fs flat-file-loader-dev:/Upload /opt/python_scripts/flat_file_loader/data/Upload -o passwd_file=~/.s3fs.passwd -o url=$S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other
sudo s3fs flat-file-loader-dev:/Config /opt/python_scripts/flat_file_loader/src/config -o passwd_file=~/.s3fs.passwd -o url=$S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty

# Run the specified command
# exec "$@"

tail -f /dev/null
