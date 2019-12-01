/*
 * LFS_Consola.h
 *
 *  Created on: 24 may. 2019
 *      Author: utnso
 */

#ifndef LFS_CONSOLA_H_
#define LFS_CONSOLA_H_

#include <readline/readline.h>
#include<global/parser.h>
#include "LFS.h"

bool finalizo;
int resolver_operacion_por_consola(t_instruccion_lql instruccion);
void crear_hilo_consola();
void *levantar_consola();
void resolver_select_consola (char* nombre_tabla, uint16_t key);
void informar_todas_tablas();
void mostrar_metadata_tabla(char* nombre_tabla);
void resolver_describe_consola(char* nombre_tabla);
#endif /* LFS_CONSOLA_H_ */
