/*
 * conexiones.h
 *
 *  Created on: 2 mar. 2019
 *      Author: utnso
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <string.h>
#include<stdio.h>
#include<stdlib.h>
#include<signal.h>
#include<unistd.h>
#include<sys/socket.h>
#include<netdb.h>
#include<string.h>
#include<commons/log.h>
#include "protocolos.h"

typedef struct // REVISAR <-
{
	t_operacion codigo_operacion;
	t_buffer* buffer;
} t_paquete;

typedef char* t_valor;	//valor que devuelve el select


int insert(char* tabla, uint16_t key, long timestamp);

t_valor select_(char* tabla, uint16_t key);

int drop(char* tabla);

int create(char* tabla, t_consistencia consistencia, int maximo_particiones, long tiempo_compactacion);

t_metadata describe(char* tabla);

char* consistencia_to_string(t_consistencia consistencia);
int crear_conexion(char* ip, char* puerto);
int enviar_mensaje(char* mensaje, int socket_cliente, int cod_operacion);
t_paquete* crear_paquete(void);
t_paquete* crear_super_paquete(void);
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
void enviar_paquete(t_paquete* paquete, int socket_cliente);
void enviar_paquete_select(int socket_envio, t_paquete_select* consulta_select);
void enviar_paquete_select_consola(int socket_envio, t_instruccion_lql consulta_select);
void enviar_paquete_metadata(int socket_envio, t_metadata* metadata);
void liberar_conexion(int socket_cliente);
void eliminar_paquete(t_paquete* paquete);
int recibir_operacion(int socket_cliente);
int iniciar_servidor(char * ip, char *puerto);
int esperar_cliente(int socket_servidor);
char* recibir_buffer(int* size, int socket_cliente);
void recibir_mensaje(t_log* logger, int socket_cliente);
int confirmar_conexion_exitosa(int socket_kernel_fd);
void recibir_handshake(t_log* logger,int socket_fd);
int enviar_handshake(int socket_fd, char* mensaje);
void enviar_status_resultado(t_status_solicitud* paquete_a_enviar, int socket_envio);
void eliminar_paquete_select(t_paquete_select* paquete_select);
void enviar_paquete_insert(int socket_envio, t_paquete_insert* consulta_insert);
void enviar_paquete_drop_describe(int socket_envio, t_paquete_drop_describe* consulta_drop_describe);
void enviar_paquete_create(int socket_envio, t_paquete_create* consulta_create);
void enviar_paquete_metadata(int socket_envio, t_metadata* metadata);
void eliminar_paquete_drop_describe(t_paquete_drop_describe* paquete_drop_describe);
void eliminar_paquete_create(t_paquete_create* paquete_create);
void eliminar_paquete_insert(t_paquete_insert* paquete_insert);
void eliminar_paquete_status(t_status_solicitud * paquete);

#endif /* UTILS_H_ */
