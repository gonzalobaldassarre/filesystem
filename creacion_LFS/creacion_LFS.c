/*
 * creacion_LFS.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */


#include "creacion_LFS.h"

int main(int argc, char* argv[]) {
	char *montaje;
	char* magic_number;
	int block_size, blocks;
	leer_config(); // abrimos config
	montaje = config_get_string_value(archivoconfig, "PUNTO_MONTAJE");
	if(argc != 4){
		printf("Debe informar cantidad de bloques, size bloque y magic number");
		return -1;
	}

	blocks = atoi(argv[1]);
	block_size=atoi(argv[2]);
	magic_number = argv[3];

	char **directorios = string_split(montaje, "/");
	char* directorio_base = string_new();
	string_append(&directorio_base, "/home/utnso/");

	//las posicion 0 es home, posicion 1 utnso
	if(directorios[2]!=NULL){
		int i=2;
		while(directorios[i]!=NULL){
			string_append(&directorio_base, directorios[i]);
			mkdir(directorio_base, 0777);
			string_append(&directorio_base, "/");
			i=i+1;
		}
	}

//	int i=0;
//	while(directorio_base[i]!=NULL){
//		free(directorio_base[i]);
//		i = i +1;
//	}
	free(directorio_base);
	int i = 0;
	while(directorios[i]!=NULL){
		free(directorios[i]);
		i = i +1;
	}
	free(directorios);
	char* dir_bloques = string_from_format("%s/Bloques", montaje);
	mkdir(dir_bloques, 0777);
	char* dir_metadata = string_from_format("%s/Metadata", montaje);
	mkdir(dir_metadata, 0777);
	char* dir_tablas = string_from_format("%s/Tables", montaje);
	mkdir(dir_tablas, 0777);

	crear_bitmap(dir_metadata, blocks, block_size);
	crear_metadata(dir_metadata, blocks, block_size, magic_number);
	crear_bloques(dir_bloques, blocks, block_size);
	free(dir_bloques);
	free(dir_metadata);
	free(dir_tablas);
	printf("Creado LFS correctamente");
	terminar_programa();
}

void crear_metadata(char* dir_metadata, int blocks, int block_size, char*magic_number){
	char* dir_metadata_tabla = string_from_format("%s/Metadata", dir_metadata);
	FILE* file = fopen(dir_metadata_tabla, "wb+");
	char* magic = string_new();
	string_append(&magic, magic_number);
	fclose(file);
	t_config* metadata_tabla = config_create(dir_metadata_tabla);
	dictionary_put(metadata_tabla->properties,"BLOCK_SIZE", string_itoa(block_size) );
	dictionary_put(metadata_tabla->properties, "BLOCKS", string_itoa(blocks));
	dictionary_put(metadata_tabla->properties, "MAGIC_NUMBER", magic);

	config_save(metadata_tabla);
	config_destroy(metadata_tabla);
	free(dir_metadata_tabla);

}

void crear_bloques(char* dir_bloques, int nro_bloques, int size_bloque){

	//char* nombre_bitmap = string_format("%s/Bitmap.bin", path_metadata);
    for(int i=0; i<nro_bloques; i++){
        char *nombre_bloque = string_from_format("%s/%d.bin", dir_bloques, i);
        FILE* fpFile = fopen(nombre_bloque,"wb+");
        fclose(fpFile);
        truncate(nombre_bloque, size_bloque);
        free(nombre_bloque);
    }

    //truncate(nombre_bitmap, nro_bloques/8);
}

void crear_bitmap(char* path_metadata, int nro_bloques, int size_bloques){

	char* nombre_bitmap = string_from_format("%s/Bitmap.bin", path_metadata);
	FILE* fpFile = fopen(nombre_bitmap,"wb");
	fclose(fpFile);
	truncate(nombre_bitmap, nro_bloques/8);

	int fd = open(nombre_bitmap, O_RDWR);
	if (fstat(fd, &mystat) < 0) {
		printf("Error al establecer fstat\n");
		close(fd);
	}

	char *bmap = mmap(NULL, mystat.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,	fd, 0);
	int tamanioBitarray = nro_bloques/8;

	bitarray = bitarray_create_with_mode(bmap, nro_bloques/8, MSB_FIRST);


	for(int cont=0; cont < tamanioBitarray*8; cont++){
		bitarray_clean_bit(bitarray, cont);
	}
	msync(bmap, sizeof(bitarray), MS_SYNC);


	munmap(bmap, mystat.st_size);
	free(nombre_bitmap);
}

void leer_config() {				// APERTURA DE CONFIG
	archivoconfig = config_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/lfs.config");
}

void terminar_programa()
{
	config_destroy(archivoconfig);
	bitarray_destroy(bitarray);
}


