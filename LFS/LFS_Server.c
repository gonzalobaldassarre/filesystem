/*
 * LFS_Server.c
 *
 *  Created on: 12 jul. 2019
 *      Author: utnso
 */

#include "LFS_Server.h"

void crear_hilo_server() {
	if (pthread_create(&hilo_server, 0, levantar_server, NULL) !=0){
		log_error(logger_consola, "Error al crear el hilo");
	}
	if (pthread_detach(hilo_server) != 0){
		log_error(logger, "Error al frenar hilo");
	}
}

void* levantar_server(){
	int socket_memoria;
	log_info(logger, "Iniciando conexion");
	int server_LFS = iniciar_servidor(ip_lfs, puerto_lfs);
	log_info(logger, "Server iniciado. A espera de clientes");
	while (1){
		if ((socket_memoria = esperar_cliente(server_LFS)) == -1) {
			log_error(logger, "No pudo aceptarse la conexion del cliente");
		} else {
			crear_hilo_memoria(socket_memoria);
		}
	}
}
