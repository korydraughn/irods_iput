#! /bin/bash

clang++ -std=c++17 -stdlib=libc++ -nostdinc++ -Wall -Wextra \
        -I/usr/include/irods \
        -I/opt/irods-externals/clang6.0-0/include/c++/v1 \
        -I/opt/irods-externals/boost1.67.0-0/include \
        -L/opt/irods-externals/clang-runtime6.0-0/lib \
        -L/opt/irods-externals/boost1.67.0-0/lib \
        -o my_iput main.cpp \
        -lirods_common \
        -lirods_client \
        -lboost_filesystem \
        -lboost_system
