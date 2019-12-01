/*
 * conexiones.c
 *
 *  Created on: 2 mar. 2019
 *      Author: utnso
 */

#include "utils.h"

char* consistencia_to_string(t_consistencia consistencia){
	char* cons;
	switch (consistencia){
		case STRONG :
			 cons = malloc(string_size(STRONG_TEXT));
			 memcpy(cons, STRONG_TEXT,string_size(STRONG_TEXT) );
			 break;
		case STRONG_HASH:
			cons = malloc(string_size(STRONG_HASH_TEXT));
			memcpy(cons, STRONG_HASH_TEXT,string_size(STRONG_HASH_TEXT) );
			break;
		case EVENTUAL:
			cons = malloc(string_size(EVENTUAL_TEXT));
			memcpy(cons, EVENTUAL_TEXT,string_size(EVENTUAL_TEXT) );
			break;
	}
	return cons;
}

void enviar_status_resultado(t_status_solicitud* paquete_a_enviar, int socket_envio){

	int bytes = get_tamanio_paquete_status(paquete_a_enviar);
	char* a_enviar = serializar_status_solicitud(paquete_a_enviar, bytes);
	send(socket_envio, a_enviar, bytes, MSG_WAITALL);
	free(a_enviar);
	eliminar_paquete_status(paquete_a_enviar);

}


void enviar_paquete_select(int socket_envio, t_paquete_select* consulta_select){

	int bytes = get_tamanio_paquete_select(consulta_select);
	char* a_enviar = serializar_paquete_select(consulta_select, bytes);

	send(socket_envio, a_enviar, bytes, MSG_WAITALL);

	free(a_enviar);
}
void enviar_paquete_select_consola(int socket_envio, t_instruccion_lql consulta_select){
	t_paquete_select* paquete = crear_paquete_select(consulta_select);
	int bytes = get_tamanio_paquete_select(paquete);
	char* a_enviar = serializar_paquete_select(paquete, bytes);
	send(socket_envio, a_enviar, bytes, MSG_WAITALL);
	eliminar_paquete_select(paquete);
	free(a_enviar);
}
void enviar_paquete_insert(int socket_envio, t_paquete_insert* consulta_insert){

	int bytes = get_tamanio_paquete_insert(consulta_insert);
	char* a_enviar = serializar_paquete_insert(consulta_insert, bytes);

	send(socket_envio, a_enviar, bytes, MSG_WAITALL);

	free(a_enviar);
}

void enviar_paquete_drop_describe(int socket_envio, t_paquete_drop_describe* consulta_drop_describe){
	int bytes = get_tamanio_paquete_describe_drop(consulta_drop_describe);

	char* a_enviar = serialiazar_paquete_drop_describe(consulta_drop_describe, bytes);

	send(socket_envio, a_enviar, bytes, MSG_WAITALL);

	free(a_enviar);

}

void enviar_paquete_create(int socket_envio, t_paquete_create* consulta_create){
	int bytes = get_tamanio_paquete_create(consulta_create);

	char* a_enviar = serializar_paquete_create(consulta_create, bytes);

	send(socket_envio, a_enviar, bytes, MSG_WAITALL);

	free(a_enviar);
}

void enviar_paquete_metadata(int socket_envio, t_metadata* metadata){

	int bytes = get_tamanio_paquete_metadata(metadata);

	char* a_enviar = serializar_metadata(metadata, bytes);

	send(socket_envio, a_enviar, bytes, MSG_WAITALL);

	free(a_enviar);
}

void recibir_handshake(t_log* logger,int socket_fd){
	int cod_op = recibir_operacion(socket_fd);
	if (cod_op == HANDSHAKE){
		recibir_mensaje(logger, socket_fd);
	}else{
		log_info(logger, "ERROR. No se realizó correctamente el HANDSHAKE");
	}
}

int enviar_handshake(int socket_fd, char* mensaje){

	return enviar_mensaje(mensaje, socket_fd, HANDSHAKE);
}

void* serializar_paquete(t_paquete* paquete, int bytes)
{
	void * magic = malloc(bytes);
	int desplazamiento = 0;

	memcpy(magic + desplazamiento, &(paquete->codigo_operacion), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(magic + desplazamiento, &(paquete->buffer->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(magic + desplazamiento, paquete->buffer->palabra, paquete->buffer->size);
	desplazamiento+= paquete->buffer->size;

	return magic;
}

int crear_conexion(char *ip, char* puerto)
{
	struct addrinfo hints;
	struct addrinfo *server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &server_info);

	int socket_cliente = socket(server_info->ai_family, server_info->ai_socktype, server_info->ai_protocol);

	if(connect(socket_cliente, server_info->ai_addr, server_info->ai_addrlen) == -1){
		freeaddrinfo(server_info);
		return -1;
	}
	freeaddrinfo(server_info);

	return socket_cliente;
}

int recibir_operacion(int socket_cliente)
{
	int cod_op;
	if(recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL) != 0)
		return cod_op;
	else
	{
		close(socket_cliente);
		return -1;
	}
}


int enviar_mensaje(char* mensaje, int socket_cliente, int cod_operacion)
{
	t_paquete* paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = cod_operacion;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(mensaje) + 1;
	paquete->buffer->palabra = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->palabra, mensaje, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2*sizeof(int);

	void* a_enviar = serializar_paquete(paquete, bytes);

	int result = send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
	return result;
}


void crear_buffer(t_paquete* paquete)
{
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->palabra = NULL;
}

//t_paquete* crear_super_paquete(void)
//{
	//me falta un malloc!
	t_paquete* paquete;

	//descomentar despues de arreglar
	//paquete->codigo_operacion = PAQUETE;
	//crear_buffer(paquete);
//	return paquete;
//}

int esperar_cliente(int socket_servidor)
{
	struct sockaddr_in dir_cliente;
	socklen_t tam_direccion = sizeof(struct sockaddr_in);

	int socket_cliente = accept(socket_servidor, (void*) &dir_cliente, &tam_direccion);

	return socket_cliente;
}

void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio)
{
	paquete->buffer->palabra = realloc(paquete->buffer->palabra, paquete->buffer->size + tamanio + sizeof(int));

	memcpy(paquete->buffer->palabra + paquete->buffer->size, &tamanio, sizeof(int));
	memcpy(paquete->buffer->palabra + paquete->buffer->size + sizeof(int), valor, tamanio);

	paquete->buffer->size += tamanio + sizeof(int);
}

void enviar_paquete(t_paquete* paquete, int socket_cliente)
{
	int bytes = paquete->buffer->size + 2*sizeof(int);
	void* a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
}

void eliminar_metadata(t_metadata* metadata){
	free(metadata->nombre_tabla->palabra);
	free(metadata->nombre_tabla);
	free(metadata);
}

void eliminar_paquete_drop_describe(t_paquete_drop_describe* paquete_drop_describe){
	free(paquete_drop_describe->nombre_tabla->palabra);
	free(paquete_drop_describe->nombre_tabla);
	free(paquete_drop_describe);
}

void eliminar_paquete_create(t_paquete_create* paquete_create){
	free(paquete_create->nombre_tabla->palabra);
	free(paquete_create->nombre_tabla);
	free(paquete_create);

}

void eliminar_paquete_insert(t_paquete_insert* paquete_insert){
	free(paquete_insert->nombre_tabla->palabra);
	free(paquete_insert->nombre_tabla);
	free(paquete_insert->valor->palabra);
	free(paquete_insert->valor);
	free(paquete_insert);
}

void eliminar_paquete_select(t_paquete_select* paquete_select){
	free(paquete_select->nombre_tabla->palabra);
	free(paquete_select->nombre_tabla);
	free(paquete_select);
}

void eliminar_paquete_status(t_status_solicitud * paquete) {
	free(paquete->mensaje->palabra);
	free(paquete->mensaje);
	free(paquete);
}

void eliminar_paquete(t_paquete* paquete)
{
	free(paquete->buffer->palabra);
	free(paquete->buffer);
	free(paquete);
}

void liberar_conexion(int socket_cliente)
{
	close(socket_cliente);
}

char* recibir_buffer(int* size, int socket_cliente)
{
	void * buffer;

	recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
	buffer = malloc(*size);
	recv(socket_cliente, buffer, *size, MSG_WAITALL);

	return buffer;
}

void recibir_mensaje(t_log* logger, int socket_cliente)
{
	int size;
	char* buffer = recibir_buffer(&size, socket_cliente);
	log_info(logger, "Me llego el mensaje %s", buffer);
	free(buffer);
}

int iniciar_servidor(char *ip, char* puerto)
{
	int socket_servidor;

    struct addrinfo hints, *servinfo, *p;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    getaddrinfo(ip, puerto, &hints, &servinfo);

    for (p=servinfo; p != NULL; p = p->ai_next)
    {
        if ((socket_servidor = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (bind(socket_servidor, p->ai_addr, p->ai_addrlen) == -1) {
            close(socket_servidor);
            continue;
        }
        break;
    }

	listen(socket_servidor, SOMAXCONN);

    freeaddrinfo(servinfo);


    return socket_servidor;
}

