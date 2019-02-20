#! /bin/bash

irm -f 10G_file.txt &
irm -rf 1000.d &
irm -rf 2000.d &
irm -rf 4000.d &
irm -rf 8000.d &
irm -rf 16000.d &
irm -rf 32000.d
