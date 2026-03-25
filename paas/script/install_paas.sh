#!/usr/bin/env bash

if [ ! -d "/home/homework/bitalos-paas" ]; then
    mkdir -p "/home/homework/bitalos-paas"
fi

if [ ! -d "/home/homework/bitalos-paas/bitalos-paas" ]; then
    mkdir -p "/home/homework/bitalos-paas/bitalos-paas"
fi

if [ ! -d "/home/homework/bitalos-paas/bitalos-data" ]; then
    mkdir -p "/home/homework/bitalos-paas/bitalos-data"
fi

if [ ! -d "/home/homework/builder" ]; then
    mkdir -p "/home/homework/builder"
fi

WORK_SPACE=/home/homework/bitalos-paas/bitalos-paas

cd ${WORK_SPACE} || (echo "no this dir "${WORK_SPACE} && exit)

if [ ! -d ${WORK_SPACE} ]; then
    echo "no this dir "${WORK_SPACE}
    mkdir -p ${WORK_SPACE}
fi

if [ ! -f bitalospaas.tar.gz ]; then
  echo "no this file bitalospaas.tar.gz"
  exit
fi

cd ${WORK_SPACE} || (echo "no this dir "${WORK_SPACE} && exit)
mkdir conf
mkdir bin

tar -zxvf bitalospaas.tar.gz
mv src/conf/bitalospaas.toml conf/
mv src/bin/bitalospaas bin/
mv src/control.sh .
sh control.sh restart bitalospaas.toml
rm -r src
rm bitalospaas.tar.gz