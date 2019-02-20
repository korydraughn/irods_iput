#! /bin/bash

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime6.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/irods-externals/boost1.67.0-0/lib

filename=iput_reimpl_matrix.txt

truncate -s0 $filename

echo 'iput reimplementation' >> $filename
echo '~~~~~~~~~~~~~~~~~~~~~' >> $filename
echo >> $filename

echo 'Putting 1 10GB file' >> $filename
{ time ./my_iput /home/kory/files_2GB/10G_file.txt ; } 2>> $filename
echo >> $filename

for n in 1000 2000 4000 8000 16000
do
    echo "Putting ${n} files ..." >> $filename
    { time ./my_iput -c24 /home/kory/files_2GB/${n}.d ; } 2>> $filename
    echo >> $filename
done

