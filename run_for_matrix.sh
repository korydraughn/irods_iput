#! /bin/bash

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime6.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/irods-externals/boost1.67.0-0/lib

#time ./my_iput -c1 /home/kory/files_2GB/10G_file.txt

truncate -s0 matrix.txt

for n in 1000 2000 4000 8000 16000 32000
do
    echo "Putting ${n} files ..." >> matrix.txt
    { time ./my_iput -c20 /home/kory/files_2GB/${n}.d ; } 2>> matrix.txt
    echo >> matrix.txt
done
