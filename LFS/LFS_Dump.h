/*
 * LFS_Dump.h
 *
 *  Created on: 23 jun. 2019
 *      Author: utnso
 */

#ifndef LFS_DUMP_H_
#define LFS_DUMP_H_

#include "LFS.h"

void crear_hilo_dump();
void dump_proceso();
t_list* copiar_y_limpiar_memtable();
void dump_por_tabla(t_cache_tabla* tabla);
void escribir_registros_y_crear_archivo(t_log* log_utilizado, t_list* registros, char* path_archivo_nuevo);
t_list* bajo_registros_a_blocks(t_log* logger_utilizado, int size_registros, char* registros);
void escribir_bloque(int bloque, char* datos);
int tamanio_bloque(int bloque_por_escribir, int bloques_totales, int size_datos);
int proximo_archivo_temporal_para(char* tabla);
int cantidad_archivos_actuales(char* path_dir, char* extension_archivo);
bool archivo_es_del_tipo(char* archivo, char* extension_archivo);
void eliminar_registro(t_registro* registro);
void eliminar_tabla(t_cache_tabla* tabla_cache);
int div_redondeada_a_mayor(int a, int b);

#endif /* LFS_DUMP_H_ */
