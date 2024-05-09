#!/bin/bash

WORK_PATH=/Users/chengjied/Documents/code/dantin/sub-pipeline
BASE_PATH=/Users/chengjied/Documents/code/dantin/data-pipeline/server/misc/data

cd ${WORK_PATH}
# dir_names=( "2023q1" "2023q2" "2023q3" "2023q4" "2024q1" )
dir_names=( "2023q2" "2023q3" "2023q4" "2024q1" )
file_names=( "demo" "drug" "indi" "outc" "reac" "rpsr" "ther" )
for dir_name in "${dir_names[@]}"
do
    suffix=$(echo "${dir_name}" | sed 's/^.\{2\}//')
    for file_name in "${file_names[@]}"
    do
        file=$(echo "${file_name}${suffix}" | tr '[:lower:]' '[:upper:]')
        echo "processing ${file}.txt in ${dir_name}"
        ${WORK_PATH}/bin/importer -config ${WORK_PATH}/misc/config.toml -table ${file_name} -data ${BASE_PATH}/${dir_name}/ASCII/${file}.txt
    done
done
