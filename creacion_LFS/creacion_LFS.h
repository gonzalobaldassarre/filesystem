/*
 * creacion_LFS.h
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#ifndef CREACION_LFS_H_
#define CREACION_LFS_H_

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/bitarray.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>


struct stat mystat;
t_bitarray* bitarray;
t_config* archivoconfig;

void crear_bitmap(char* path_metadata, int nro_bloques, int size_bloques);



#endif /* CREACION_LFS_H_ */
