FROM python:3.10
RUN apt-get update && apt-get install zstd
RUN useradd --create-home --shell /bin/bash username
#apt-get update && apt-get install -y build-essential && 
#libpq-dev
WORKDIR /home/user
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
USER username
COPY --chown=username . .
CMD ["bash"]
