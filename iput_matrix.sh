#! /bin/bash

filename=iput_matrix.txt

truncate -s0 $filename

echo 'Official iput' >> $filename
echo '~~~~~~~~~~~~~' >> $filename
echo >> $filename

echo 'Putting 1 10GB file' >> $filename
{ time iput /home/kory/files_2GB/10G_file.txt ; } 2>> $filename
echo >> $filename

for n in 1000 2000 4000 8000 16000
do
    echo "Putting ${n} files ..." >> $filename
    { time iput -rf /home/kory/files_2GB/${n}.d ; } 2>> $filename
    echo >> $filename
done

