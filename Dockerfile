FROM ubuntu:24.04 as builder

ENV TZ=America/US
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONFAULTHANDLER=1

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
        build-essential \
        ninja-build \
        python3-pip \
        python3-venv \
        python3-dev curl gnupg apt-transport-https \
        zlib1g libgflags2.2 libgflags-dev && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages conan==2.15.0 'cmake<4.0' && \
     conan profile detect --force && \
     conan remote add otterbrix http://conan.otterbrix.com

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

WORKDIR /app/build
COPY conanfile.py ./conanfile.py
RUN conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=gnu20

WORKDIR /app
COPY ./include ./include
COPY ./common ./common
COPY ./logical_replication ./logical_replication
COPY ./otterbrix ./otterbrix
COPY ./postgres ./postgres
COPY ./main.cpp ./main.cpp
COPY ./CMakeLists.txt ./CMakeLists.txt

WORKDIR /app/build

RUN cmake .. -G Ninja -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . --target all -- -j $(nproc)

CMD [ "./diplom" ]