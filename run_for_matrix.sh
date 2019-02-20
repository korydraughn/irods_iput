#! /bin/bash

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime6.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/irods-externals/boost1.67.0-0/lib

truncate -s0 matrix.txt

echo 'Official iput' >> matrix.txt
echo '~~~~~~~~~~~~~' >> matrix.txt
echo >> matrix.txt

echo 'Putting 1 10GB file' >> matrix.txt
{ time iput /home/kory/files_2GB/10G_file.txt ; } 2>> matrix.txt
irm -f 10G_file.txt

for n in 1000 2000 4000 8000 16000
do
    echo "Putting ${n} files ..." >> matrix.txt
    { time iput /home/kory/files_2GB/${n}.d ; } 2>> matrix.txt
    echo >> matrix.txt
done

irm -rf 1000.d &
irm -rf 2000.d &
irm -rf 4000.d &
irm -rf 8000.d &
irm -rf 16000.d

echo >> matrix.txt
echo >> matrix.txt
echo 'iput reimplementation' >> matrix.txt
echo '~~~~~~~~~~~~~~~~~~~~~' >> matrix.txt
echo >> matrix.txt

echo 'Putting 1 10GB file' >> matrix.txt
{ time ./my_iput -c1 /home/kory/files_2GB/10G_file.txt ; } 2>> matrix.txt
irm -f 10G_file.txt >> matrix.txt

for n in 1000 2000 4000 8000 16000
do
    echo "Putting ${n} files ..." >> matrix.txt
    { time ./my_iput -c20 /home/kory/files_2GB/${n}.d ; } 2>> matrix.txt
    echo >> matrix.txt
done

irm -rf 1000.d &
irm -rf 2000.d &
irm -rf 4000.d &
irm -rf 8000.d &
irm -rf 16000.d
