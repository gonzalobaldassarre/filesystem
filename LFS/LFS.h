/*
 * LFS.h
 *
 *  Created on: 7 abr. 2019
 *      Author: utnso
 */
#ifndef LFS_H_
#define LFS_H_

#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/queue.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <global/protocolos.h>
#include <global/utils.h>
#include <pthread.h>
#include <readline/readline.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/inotify.h>
#include <semaphore.h>
#include <sys/inotify.h>

#include "LFS_Compactacion.h"
#include "LFS_Consola.h"
#include "LFS_Dump.h"
t_list* memtable;
t_list* tablas_en_lfs;
t_list* hilos_memorias;
char* bmap;
struct stat size_bitmap;
t_bitarray* bitarray;

typedef struct{
	char* nombre;
	char* path_tabla;
}t_parametros_compactacion;


typedef struct{
	char* nombre;
	pthread_t id_hilo_compactacion;
	pthread_mutex_t mutex_compactacion;
	pthread_mutex_t mutex_compac_select;
	pthread_mutex_t mutex_select_drop;
	bool esta_bloqueado;
	t_parametros_compactacion* parametros;
}t_tabla_logica;



typedef struct{
	t_instruccion_lql* instruccion;
	int socket_memoria;
}t_instruccion_bloqueada;

t_dictionary* instrucciones_bloqueadas_por_tabla;

// ******* DEFINICION DE FUNCIONES A UTILIZAR ******* //
void chequearSocket(int socketin);
void iniciar_loggers();
void leer_config();
void leer_tiempo_dump_y_retardo_del_config();
void terminar_programa();
					// ******* TIPOS NECESARIOS ******* //
t_log* logger;
t_log* logger_dump;
t_log* logger_compactacion;
t_log* logger_consola;
t_config* archivoconfig;
char* path_montaje;
int  max_size_value, block_size, blocks, bloques_disponibles;
char* ip_lfs;
char* puerto_lfs;
int tiempo_dump;
long retardo;
pthread_mutex_t mutexMemtable, mutexDump, mutexTablasLFS, mutex_temp, mutexHilosMemoria, mutexInstBloqueadas, mutex_compac_select, mutexBitmap;
bool fin_de_programa;
pthread_t hilo_consola;
pthread_t hilo_dump;
pthread_t hilo_server;
pthread_t hilo_inotify;

bool tabla_esta_bloqueada(char* nombre_tabla);
t_instruccion_bloqueada* crear_instruccion_select_bloqueada(t_paquete_select* select, int socket_memoria);
t_instruccion_bloqueada* crear_instruccion_insert_bloqueada(t_paquete_insert* insert, int socket_memoria);
t_list* filtrar_registros_con_key(t_list* registros, uint16_t key);
char* leer_bloques_de_archivo(char* path_archivo);
t_list* obtener_registros_de_archivo(char* path_archivo_temporal);
t_list* obtener_registros_de_buffer(char* buffer);
int crear_directorio_tabla (char* dir_tabla);
void levantar_lfs(char* montaje);
void obtener_bitmap();
void obtener_info_metadata();
typedef char* t_valor;	//valor que devuelve el select
t_registro* buscar_registro_actual(t_list* registros_encontrados);
t_list* buscar_registros_en_particion(char* nombre_tabla,uint16_t key);
t_list* buscar_registros_temporales(char* nombre_tabla, uint16_t key);
t_status_solicitud* resolver_insert(t_log* log_a_usar, char* tabla, uint16_t key, char* value, long timestamp);
t_status_solicitud* resolver_select (char* nombre_tabla, uint16_t key);
t_status_solicitud* resolver_create (t_log* log_a_usar, char* nombre_tabla, t_consistencia consistencia, int num_particiones, long compactacion);
int drop(char* tabla);
void enviar_tabla_para_describe(int socket_memoria, char* nombre_tabla);
int obtener_cantidad_tablas_LFS();
void liberar_bloque(int num_bloque);
void enviar_metadata_todas_tablas (int socket_memoria);
int create(char* tabla, t_consistencia consistencia, int maximo_particiones, long tiempo_compactacion);
t_status_solicitud* resolver_drop(t_log* log_a_usar,char* nombre_tabla);
void resolver_describe(char* nombre_tabla, int socket_memoria);
t_registro* crear_registro(char* value, uint16_t key, long timestamp);
t_tabla_logica* crear_tabla_logica(char* nombre_tabla);
void agregar_registro_memtable(t_registro* registro_a_insertar, char * nombre_tabla);
bool validar_datos_describe(char* nombre_tabla, int socket_memoria);
t_cache_tabla* obtener_tabla_memtable(char* nombre_tabla);
t_cache_tabla* crear_tabla_cache(char* nombre_tabla);
t_cache_tabla* buscar_tabla_memtable(char* nombre_tabla);
t_list* buscar_registros_memtable(char* nombre_tabla, uint16_t key);
bool existe_tabla_fisica(char* nombre_tabla);
void crear_hilo_memoria(int socket_memoria);
void crear_hilo_inotify();
int resolver_operacion(int socket_memoria, t_operacion cod_op);
char* string_block();
char* array_int_to_array_char(t_list* array_int);
int obtener_bloque_disponible();
t_metadata* obtener_info_metadata_tabla(char* dir_tabla, char* nombre_tabla);
void crear_particiones(char* dir_tabla,int  num_particiones);
void crear_archivo(char* dir_archivo, int size, t_list* array_bloques);
void guardar_datos_particion_o_temp(char* dir_archivo , int size, t_list* array_bloques);
void crear_archivo_metadata_tabla(char* dir_tabla, int num_particiones,long compactacion,t_consistencia consistencia);
void iniciarMutexMemtable();
void crear_hilo_inotify();
void liberar_tabla_logica(t_tabla_logica* tabla);
t_tabla_logica* buscar_tabla_logica_con_nombre(char* nombre_tabla);

//t_metadata describe(char* tabla);

#endif /* LFS_H_ */
