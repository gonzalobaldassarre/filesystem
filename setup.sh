#!/bin/bash

echo "Se instala el repo de Configs"
cd ..
git clone https://github.com/chaloxc/FINAL.git
echo "Se instala Commons"
git clone https://github.com/sisoputnfrba/1C2019-Scripts-lql-entrega.git
git clone https://github.com/sisoputnfrba/so-commons-library
cd so-commons-library
sudo make install
cd ..
echo "Compilacion de proyecto"
cd tp-2019-1c-Los-Dinosaurios-Del-Libro
cd global/Debug/
make clean
make all
sleep 2
cd ../../Kernel/Debug/
make clean
make all
sleep 2
cd ../../Memoria/Debug/
make clean
make all
sleep 2
cd ../../LFS/Debug/
make clean
make all
sleep 2
cd ../../creacion_LFS/Debug/
make clean
make all
sleep 2
export LD_LIBRARY_PATH=/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/global/Debug
