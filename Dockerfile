# FROM ubuntu:latest
# FROM ubuntu:20.04
FROM tiangolo/uwsgi-nginx

RUN apt update && \
    apt install -y software-properties-common build-essential checkinstall sudo nano

# RUN add-apt-repository -y ppa:deadsnakes/ppa && \
#     apt update && \
#     apt install -y s3fs python3-pip git

RUN apt update && \
    apt install -y s3fs python3-pip git fuse dos2unix

RUN pip3 install --upgrade pip

# RUN apt install -y python3.8

RUN alias python3=python3.8

# RUN echo "alias python=python3.8" >> ~/.bashrc

# RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1

# RUN git clone https://github.com/james-larsen/universal-flat-file-loader.git /opt/python_scripts/flat_file_loader
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Archive
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Logs
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Upload
# RUN mkdir -p /opt/python_scripts/flat_file_loader/src/config

RUN pip3 install \
    'pandas>=1.5.3' \
    'sqlalchemy>=2.0.4' \
    'psycopg2-binary>=2.9.5' \
    'configparser>=5.3.0' \
    'openpyxl>=3.1.0' \
    'boto3>=1.26.123' \
    'keyring>=23.13.1' \
    'nexus-utilities'

RUN mkdir /opt/python_scripts/

COPY . /opt/python_scripts/flat_file_loader

RUN dos2unix /opt/python_scripts/flat_file_loader/entrypoint.sh

RUN touch ~/.s3fs.passwd

RUN chmod 600 ~/.s3fs.passwd

# COPY entrypoint.sh /opt/python_scripts/flat_file_loader/entrypoint.sh

RUN chmod +x /opt/python_scripts/flat_file_loader/entrypoint.sh

ENTRYPOINT ["/opt/python_scripts/flat_file_loader/entrypoint.sh"]

# ENTRYPOINT ["/bin/bash"]