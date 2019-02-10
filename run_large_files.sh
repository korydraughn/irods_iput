#! /bin/bash

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime6.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/irods-externals/boost1.67.0-0/lib

#time ./my_iput /home/kory/files_2GB/large_files /tempZone/home/rods
time ./my_iput /home/kory/files_2GB/large_files
