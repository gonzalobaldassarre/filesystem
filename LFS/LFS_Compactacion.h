/*
 * LFS_Compactacion.h
 *
 *  Created on: 27 jun. 2019
 *      Author: utnso
 */

#ifndef LFS_COMPACTACION_H_
#define LFS_COMPACTACION_H_

#include "LFS_Dump.h"
#include "LFS.h"
void *compactar(void* nombre_tabla);
long obtener_tiempo_compactacion(char* path_tabla);
bool hay_temporales(char* path_tabla);
t_list* renombrar_archivos_para_compactar(char* path_tabla);
t_list* leer_registros_temporales(t_list* temporales_a_compactar);
t_list* leer_registros_particiones(char* path_tabla);
//t_list* leer_registros_bloques(char* bloques, int size_total);
//char* leer_registros_char_de_bloque(int bloque);
//t_list* transformar_registros(char* registros);
t_list* filtrar_registros_duplicados_segun_particiones(char* path_tabla, t_list* registros_nuevos);
void vaciar_datos_de_listas_no_tocadas(t_list* registros_particiones, t_dictionary* particiones_tocadas);
void actualizar_registro(t_list* registros, t_registro* un_registro);
void liberar_bloques_compactacion(t_list* bloques_a_liberar);
void liberar_bloques_archivo(char* path_archivo);
bool pertenece_a_lista_particiones(t_list* particiones_a_liberar,char* nombre_archivo);
void realizar_compactacion(char* path_tabla, t_list* registros_filtrados);
t_list* encontrar_particiones_tocadas(t_list* registros_filtrados);
t_list* eliminar_temp_y_bin_tabla(char* path_tabla, t_list* registros_por_particion_a_modificar);
void liberar_bloques_compactacion(t_list* bloques_a_liberar);


#endif /* LFS_COMPACTACION_H_ */
