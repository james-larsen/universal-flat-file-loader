# FROM ubuntu:latest
# FROM ubuntu:20.04
FROM tiangolo/uwsgi-nginx

RUN apt update && \
    apt install -y software-properties-common build-essential checkinstall

# RUN add-apt-repository -y ppa:deadsnakes/ppa && \
#     apt update && \
#     apt install -y s3fs python3-pip git

RUN apt update && \
    apt install -y s3fs python3-pip git

RUN pip3 install --upgrade pip

# RUN apt install -y python3.8

RUN alias python3=python3.8

# RUN echo "alias python=python3.8" >> ~/.bashrc

# RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1

RUN mkdir /opt/python_scripts/

# RUN git clone https://github.com/james-larsen/universal-flat-file-loader.git /opt/python_scripts/flat_file_loader
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Archive
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Logs
# RUN mkdir -p /opt/python_scripts/flat_file_loader/data/Upload
# RUN mkdir -p /opt/python_scripts/flat_file_loader/src/config

COPY . /opt/python_scripts/flat_file_loader

RUN touch ~/.s3fs.passwd

RUN chmod 600 ~/.s3fs.passwd

RUN pip3 install 'pandas>=1.5.3'

RUN pip3 install 'sqlalchemy>=2.0.4'

RUN pip3 install 'psycopg2-binary>=2.9.5'

RUN pip3 install 'configparser>=5.3.0'

RUN pip3 install 'openpyxl>=3.1.0'

RUN pip3 install 'boto3>=1.26.123'

RUN pip3 install 'keyring>=23.13.1'

RUN pip3 install 'nexus-utilities'

# COPY entrypoint.sh /opt/python_scripts/flat_file_loader/entrypoint.sh

RUN chmod +x /opt/python_scripts/flat_file_loader/entrypoint.sh

ENTRYPOINT ["/opt/python_scripts/flat_file_loader/entrypoint.sh"]
