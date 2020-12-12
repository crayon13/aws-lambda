#!/bin/sh
SRC_PATH='../src'
TEMP_PATH='../temp'
ZIP_PATH='../dist/function.zip'


rm -rf ${ZIP_PATH}
rm -rf ${TEMP_PATH}

mkdir ${TEMP_PATH}
cp lambda_function.py ${TEMP_PATH}

cd ${TEMP_PATH}
pip3 install requests -t .
pip3 install requests_aws4auth -t .
zip -r ${ZIP_PATH} .
cd ${SRC_PATH}