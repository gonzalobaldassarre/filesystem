/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: utnso
 */
#include "memoria.h"
int main(int argc, char **argv)

{
	/** creacion path **/
	path="/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/Memoria/";
	char* pathc = malloc(strlen(argv[1])+strlen(path)+1);
	memcpy(pathc,path,strlen(path));
	memcpy(pathc+strlen(path),argv[1],strlen(argv[1])+1);

	char* pathl = malloc(strlen(argv[2])+strlen(path)+1);
	memcpy(pathl,path,strlen(path));
	memcpy(pathl+strlen(path),argv[2],strlen(argv[2])+1);

	char* pathg = malloc(strlen(argv[3])+strlen(path)+1);
	memcpy(pathg,path,strlen(path));
	memcpy(pathg+strlen(path),argv[3],strlen(argv[3])+1);

	FD_ZERO(&master);    // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	/*
	 * INICIO LOGGER, CONFIG, LEVANTO DATOS E INICIO SERVER
	 */


	iniciar_logger(pathl);
	iniciar_logger_gossip(pathg);

	leer_config(pathc);
	iniciar_hilo_inotify_memoria(argv);
	if(pthread_mutex_init(&mutexMemoria,NULL)==0){
		//log_info(logger, "MutexMemoria inicializado correctamente");
	} else {
		log_error(logger_mostrado, "Fallo inicializacion de MutexMemoria");
	};
	/*
	 * HAY QUE CAMBIAR RUTA A UNA VARIABLE PARA PODER LEVANTAR MEMORIAS CON DIFERENTES CONFIGS
	 */
	levantar_datos_memoria();
	char* memoria_principal = (char*) malloc(tamanio_memoria*sizeof(char));
	log_info(logger,"Se reservaron %i bytes para la memoria", (tamanio_memoria*sizeof(char)));
	tablas = list_create();
	log_info(logger,"Tabla de segmentos creada, cantidad actual: %i ", tablas->elements_count); // @suppress("Field cannot be resolved")
	levantar_datos_lfs();
	/*
	 * obtener socket a la escucha
	 */
	server_memoria = iniciar_servidor(ip_memoria, puerto_memoria);
	seeds = levantarSeeds();
	puertosSeeds = levantarPuertosSeeds();
	seedsCargadas();
	iniciarTablaDeGossiping(tablaGossiping);

	/*
	 * ME CONECTO CON LFS E INTENTO UN HANDSHAKE -----------------------INTENTA CONECTARSE, SI NO PUEDE CORTA LA EJECUCION
	 */
	socket_conexion_lfs = crear_conexion(ip__lfs,puerto__lfs); //
	//log_info(logger,"que paso %i",socketMemoriaSeed);
	if(socket_conexion_lfs != -1)
		{
		log_info(logger,"Creada la conexion para LFS %i", socket_conexion_lfs);
		intentar_handshake_a_lfs(socket_conexion_lfs); 										// diferente al handshake normal SE SETEA TAMANIO_PAGINA y CANTIDAD_PAGINAS
		log_info(logger, "Conexion exitosa con con %i", socket_memoria);
		log_info(logger,"El tamaño de cada pagina sera: %i", tamanio_pagina);
		log_info(logger,"La cantidad de paginas en memoria principal será: %i", cantidad_paginas);
		} else
			{
			log_info(logger_mostrado,"No se pudo realizar la conexion con LFS. Abortando.");
			log_info(logger_mostrado, "Se liberaran %i bytes de la memoria",(tamanio_memoria*sizeof(char)));
			free(memoria_principal);
			return -1;
			}
	iniciarHiloConsola(memoria_principal, tablas);
	iniciarHiloJournaling(memoria_principal, tablas);
	iniciarHiloGossiping(tablaGossiping);
	log_info(logger,"GOSSIPING hay %i memorias OK",tablaGossiping->elements_count);
	select_esperar_conexiones_o_peticiones(memoria_principal,tablas);

	return EXIT_SUCCESS;
}

/*
			######## #### ##    ##    ########  ######## ##          ##     ##    ###    #### ##    ##
			##        ##  ###   ##    ##     ## ##       ##          ###   ###   ## ##    ##  ###   ##
			##        ##  ####  ##    ##     ## ##       ##          #### ####  ##   ##   ##  ####  ##
			######    ##  ## ## ##    ##     ## ######   ##          ## ### ## ##     ##  ##  ## ## ##
			##        ##  ##  ####    ##     ## ##       ##          ##     ## #########  ##  ##  ####
			##        ##  ##   ###    ##     ## ##       ##          ##     ## ##     ##  ##  ##   ###
			##       #### ##    ##    ########  ######## ########    ##     ## ##     ## #### ##    ##
*/
void *iniciar_select(void* dato){ // @suppress("No return")
	datosSelect* datos = (datosSelect *)dato;
	select_esperar_conexiones_o_peticiones(datos->memoria,datos->tabla);
}
void iniciarHiloKernel(datosSelect* dato){
	pthread_t hiloSelect;
	if (pthread_create(&hiloSelect, 0, iniciar_select, dato) !=0){
		log_error(logger, "Error al crear el hilo");
	}
	if (pthread_detach(hiloSelect) != 0){
		log_error(logger, "Error al crear el hilo");
	}

}
void *iniciar_gossiping(void* tablaGossiping){
	t_list* tablag = (t_list *) tablaGossiping;
	while(1){
		sleep(retardo_gossiping);
		log_info(logger_g, "[GOSSIPING AUTOMATICO] Inicio GOSSIPING");
		gossiping_consola();
		log_info(logger_g, "[GOSSIPING AUTOMATICO] Fin GOSSIPING");
	}
}

void resolver_gossiping(int socket){
	recibir_tabla_de_gossiping(socket);
	enviar_mi_tabla_de_gossiping(socket);
}
void recibir_tabla_de_gossiping(int socket){
	int numero_memorias = 0;
	recv(socket,&numero_memorias,sizeof(int),MSG_WAITALL);
	//log_info(logger_g, "Memorias de tabla: %i",numero_memorias);

	for(int i=0;i<numero_memorias;i++){
		t_gossip* memoria=malloc(sizeof(t_gossip));
		int tamanio_ip;
		recv(socket,&tamanio_ip,sizeof(int),MSG_WAITALL);
		memoria->ip_memoria=malloc(tamanio_ip);

		//log_info(logger, "Tamanio ip %i",tamanio_ip);
		recv(socket,memoria->ip_memoria,tamanio_ip,MSG_WAITALL);
		//log_info(logger, "IP: %s",memoria->ip_memoria);
		int tamanio_nombre;
		recv(socket,&tamanio_nombre,sizeof(int),MSG_WAITALL);

		//log_info(logger, "Tamanio nombre %i",tamanio_nombre);
		memoria->nombre_memoria=malloc(tamanio_nombre);
		recv(socket,memoria->nombre_memoria,tamanio_nombre,MSG_WAITALL);
		int tamanio_puerto;
		recv(socket,&tamanio_puerto,sizeof(int),MSG_WAITALL);
		memoria->puerto_memoria=malloc(tamanio_puerto);

		//log_info(logger, "Tamanio puerto %i",tamanio_puerto);
		recv(socket,memoria->puerto_memoria,tamanio_puerto,MSG_WAITALL);
		log_warning(logger_g, "[MEMORIA RECIBIDA]IP: %s , PUERTO: %s , NOMBRE: %s",memoria->ip_memoria,memoria->puerto_memoria,memoria->nombre_memoria);

		if(encontrarMemoria(memoria->nombre_memoria, tablaGossiping)!=NULL){
			eliminar_memoria_gossip(memoria);
		}else{
			list_add(tablaGossiping,memoria);
		}
	}
}
void eliminar_memoria_gossip(t_gossip* memoria){
	free(memoria->ip_memoria);
	free(memoria->nombre_memoria);
	free(memoria->puerto_memoria);
	free(memoria);
}

void enviar_mi_tabla_de_gossiping(int socket){
	send(socket,&tablaGossiping->elements_count,sizeof(int),MSG_WAITALL);
	for(int i=0;i<tablaGossiping->elements_count;i++){

		t_gossip* memoria = list_get(tablaGossiping,i);
		int memo = crear_conexion(memoria->ip_memoria,memoria->puerto_memoria);
		if(memo!=-1){
			close(memo);
			int tamanio_ip=strlen(memoria->ip_memoria)+1;
			send(socket,&tamanio_ip,sizeof(int),MSG_WAITALL);
			send(socket,memoria->ip_memoria,tamanio_ip,MSG_WAITALL);

			int tamanio_nombre=strlen(memoria->nombre_memoria)+1;
			send(socket,&tamanio_nombre,sizeof(int),MSG_WAITALL);
			send(socket,memoria->nombre_memoria,tamanio_nombre,MSG_WAITALL);

			int tamanio_puerto=strlen(memoria->puerto_memoria)+1;
			send(socket,&tamanio_puerto,sizeof(int),MSG_WAITALL);
			send(socket,memoria->puerto_memoria,tamanio_puerto,MSG_WAITALL);
		} else {
			int tamanio_ip=strlen(memoria->ip_memoria)+1;
			send(socket,&tamanio_ip,sizeof(int),MSG_WAITALL);
			send(socket,memoria->ip_memoria,tamanio_ip,MSG_WAITALL);

			int tamanio_nombre=strlen(memoria->nombre_memoria)+1;
			send(socket,&tamanio_nombre,sizeof(int),MSG_WAITALL);
			send(socket,memoria->nombre_memoria,tamanio_nombre,MSG_WAITALL);

			int tamanio_puerto=strlen(memoria->puerto_memoria)+1;
			send(socket,&tamanio_puerto,sizeof(int),MSG_WAITALL);
			send(socket,memoria->puerto_memoria,tamanio_puerto,MSG_WAITALL);
			list_remove(tablaGossiping,i);
			free(memoria->ip_memoria);
			free(memoria->nombre_memoria);
			free(memoria->puerto_memoria);
			free(memoria);
			i-=1;
		}

	}
}

void iniciarHiloGossiping(t_list* tablaGossiping){ // @suppress("Type cannot be resolved")
	pthread_t hiloGossiping;
	if (pthread_create(&hiloGossiping, 0, iniciar_gossiping, tablaGossiping) !=0){
			log_error(logger_mostrado, "Error al crear el hilo GOSSIPING");
		}
	if (pthread_detach(hiloGossiping) != 0){
			log_error(logger_mostrado, "Error al crear el hilo GOSSIPING");
		}
}
void iniciarHiloJournaling(char* memo, t_list* tablas){
	pthread_t hiloJournaling;
	datosSelect* datos=malloc(sizeof(datosSelect));
	datos->memoria=memo;
	datos->tabla=tablas;
	if (pthread_create(&hiloJournaling, 0, journaling_automatico, datos) !=0){
				log_error(logger, "Error al crear el hilo JOURNALING");
			}
			if (pthread_detach(hiloJournaling) != 0){
				log_error(logger, "Error al crear el hilo JOURNALING");
			}
}
void iniciarHiloConsola(char* memo, t_list* tablas){
	pthread_t hiloSelect;
	datosSelect* datos=malloc(sizeof(datosSelect));
	datos->memoria=memo;
	datos->tabla=tablas;
		if (pthread_create(&hiloSelect, 0, iniciar_consola, datos) !=0){
			log_error(logger_mostrado, "Error al crear el hilo de consola");
		}
		if (pthread_detach(hiloSelect) != 0){
			log_error(logger_mostrado, "Error al crear el hilo de consola");
		}
}
void *journaling_automatico(void* dato){
	datosSelect* datos = (datosSelect*) dato;
	while(1){
		sleep(retardo_journaling);
		pthread_mutex_lock(&mutexMemoria);
		journaling(datos->memoria,datos->tabla);
		pthread_mutex_unlock(&mutexMemoria);
	}
}
void *iniciar_consola(void* dato){
	datosSelect* datos = (datosSelect*) dato;
	while(1){
			char* consol = "CONSOLA ";
			char* consol2 = " >>";
			char* consol3 = malloc(strlen(consol)+strlen(consol2)+strlen(nombre_memoria)+1);
			memcpy(consol3,consol,strlen(consol));
			memcpy(consol3+strlen(consol),nombre_memoria,strlen(nombre_memoria));
			memcpy(consol3+strlen(consol)+strlen(nombre_memoria),consol2,strlen(consol2)+1);
			char* linea = readline(consol3);
			free(consol3);

			parsear_y_ejecutar(linea, 1,datos->memoria,datos->tabla);

			free(linea);
		}
}
void parsear_y_ejecutar(char* linea, int flag_de_consola, char* memoria, t_list* tablas){
	t_instruccion_lql instruccion = parsear_linea(linea);
	if (instruccion.valido) {
		ejecutar_API_desde_consola(instruccion,memoria,tablas);
	}else{
		if (flag_de_consola){
			log_error(logger, "Reingrese correctamente la instruccion");
		}
	}
}
void ejecutar_API_desde_consola(t_instruccion_lql instruccion,char* memoria,t_list* tablas){
	t_operacion operacion = instruccion.operacion;
	switch(operacion) {
		case SELECT:
			log_info(logger, "Se solicita SELECT desde consola");
			pthread_mutex_lock(&mutexMemoria);
			if(resolver_select_para_consola(instruccion,memoria,tablas)==-1){resolver_select_para_consola(instruccion,memoria,tablas);}
			pthread_mutex_unlock(&mutexMemoria);
			break;
		case INSERT:
			pthread_mutex_lock(&mutexMemoria);
			log_info(logger, "Se solicitó INSERT desde consola");
			if(resolver_insert_para_consola(instruccion,memoria,tablas)==-1){resolver_insert_para_consola(instruccion,memoria,tablas);}
			pthread_mutex_unlock(&mutexMemoria);
			break;
		case CREATE:
			log_info(logger, "Se solicitó CREATE desde consola");
			resolver_create_consola(socket_conexion_lfs,instruccion);
			//aca debería enviarse el mensaje a LFS con CREATE
			break;
		case DESCRIBE:
			log_info(logger, "Se solicitó DESCRIBE desde consola");
			resolver_describe_consola(instruccion);
			//aca debería enviarse el mensaje a LFS con DESCRIBE
			break;
		case DROP:
			log_info(logger, "Se solicitó DROP desde consola");
			resolver_drop_consola(instruccion);
			//aca debería enviarse el mensaje a LFS con DROP
			break;
		case RUN:
			log_info(logger, "Se solicitó RUN");
			break;
		case GOSSPING:
			log_info(logger, "Se solicitó GOSSIPING");
			gossiping_consola();
			break;
		case JOURNAL:
			log_info(logger, "Se solicitó Journaling");
			pthread_mutex_lock(&mutexMemoria);
			journaling(memoria, tablas);
			pthread_mutex_unlock(&mutexMemoria);
			break;
		default:
			log_warning(logger, "Operacion desconocida.");
			break;
		}
}
void gossiping_consola(){
	for(int j=0;seeds[j]!=NULL;j++){ // CHEQUEO SEEDS
		int ssocketMemoriaSeed = crear_conexion(seeds[j],puertosSeeds[j]);
		if(ssocketMemoriaSeed!=-1) { // intento con la seed, siempre debo intentar con la seed
		enviar_gossiping(ssocketMemoriaSeed);
		close(ssocketMemoriaSeed);
		} else {
			//log_error(logger_mostrado,"[SEED DESCONECTADA]Seed no responde");
			log_error(logger_g,"[SEED DESCONECTADA] Seed no responde");
 		}
	}
	for(int i=0;i<tablaGossiping->elements_count;i++){
		t_gossip* memoria = list_get(tablaGossiping,i);
		//log_info(logger,"N1: %s , N2: %s",memoria->nombre_memoria,nombre_memoria);
		if(string_equals_ignore_case(memoria->nombre_memoria,nombre_memoria)){ //log_info(logger,"MISMA MEMORIA NO HAGO NADA!");
		} else {
			//log_info(logger,"ENTRE POR ACA, CUALQUIERA");
			int socketMemoria = crear_conexion(memoria->ip_memoria,memoria->puerto_memoria);
			if(socketMemoria!=-1) {
				enviar_gossiping(socketMemoria);
				close(socketMemoria);
			} else { list_remove(tablaGossiping,i );
			free(memoria->ip_memoria);
			free(memoria->nombre_memoria);
			free(memoria->puerto_memoria);
			free(memoria);
			}
		}
	}

}
void enviar_gossiping(int socketMemoria){
	t_operacion cod_op=GOSSPING;
	send(socketMemoria, &cod_op, sizeof(t_operacion), MSG_WAITALL);
	enviar_mi_tabla_de_gossiping(socketMemoria);
	recibir_tabla_de_gossiping(socketMemoria);
}
int resolver_insert_para_consola(t_instruccion_lql insert,char* memoria_principal, t_list* tablas){
	log_info(logger, "[SOLICITUD INSERT] CONSOLA");
	log_warning(logger, "TABLA: %s", insert.parametros.INSERT.tabla);
	log_warning(logger, "KEY: %i", insert.parametros.INSERT.key);
	log_warning(logger, "VALUE: %s", insert.parametros.INSERT.value);
	log_warning(logger, "TS: %i", insert.parametros.INSERT.timestamp);
	long tsss =insert.parametros.INSERT.timestamp;
	char* tabla = insert.parametros.INSERT.tabla;
	uint16_t key = insert.parametros.INSERT.key;
	char* dato = insert.parametros.INSERT.value;
			;
	segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
	int lruu= lru(memoria_principal,tablas);
	int registroAInsertar = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);
	if(registroAInsertar==-1){

	}
	if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)&&(registroAInsertar==-1)){ //
		log_error(logger_mostrado, "[MEMORIA FULL] No hay lugar en la memoria, debe realizarse JOURNAL", tabla);
		journaling(memoria_principal, tablas);
		return -1;
	} else {
	if(registroAInsertar==-1){ // no está el registro, hay que agregarlo -> bit mod en 1
		if(segmentoBuscado==NULL) // si el segmento no existe lo creo, y luego la pagina.
			{
			list_add(tablas, crearSegmento(tabla));
			paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
			return 1;
			} else { // existe el segmento, agrego la pagina al segmento
					log_error(logger, "[REGISTRO INEXISTENTE] No existe el registro, se procede a crear el mismo");
					paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
					return 1;
					}
	} else // el registro con esa key ya existe
			//modificarRegistro(key,dato,(unsigned)time(NULL),reg2,memoria_principal,tablas);
			modificarRegistro(key,dato,tsss,registroAInsertar,memoria_principal,tablas);
			cambiarBitModificado(tabla, key,tablas, memoria_principal);
			return 1;
	}
	}


int resolver_select_para_consola(t_instruccion_lql instruccion_select,char* memoria_principal, t_list* tablas){

	log_info(logger, "[SOLICITUD SELECT] CONSOLA");
	log_warning(logger, "TABLA: %s KEY %d", instruccion_select.parametros.SELECT.tabla, instruccion_select.parametros.SELECT.key);

	char* tabla = instruccion_select.parametros.SELECT.tabla;
	uint16_t key = instruccion_select.parametros.SELECT.key;
	int reg = 0;

	reg = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);


	if(reg==-1){
		//log_info(logger, "El registro con key '%d' NO se encuentra en memoria principal", key);
		int lruu = lru(memoria_principal,tablas);
		if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)){ //
								/*
								 * NO HAY ESPACIO PROXIMO DISPONIBLE Y LRU DICE QUE ESTÁ TODO OCPUADO
								 * -> hay que hacer journaling
								 *
								 */
								log_error(logger_mostrado, "[MEMORIA FULL] No hay lugar en la memoria, debe realizarse JOURNAL(automatico)", tabla);
								journaling(memoria_principal, tablas);
								return -1;
		} else {
			log_info(logger, "Hay espacio en memoria, se procede a realizar la peticion a LFS", key);
			enviar_paquete_select_consola(socket_conexion_lfs, instruccion_select);
			t_status_solicitud* status= desearilizar_status_solicitud(socket_conexion_lfs);
			if(status->es_valido){
				t_registro* registro = obtener_registro(status->mensaje->palabra);
				log_warning(logger_mostrado, "[REGISTRO TRAIDO DESDE LFS] %s ; %d ; %s ; %i | (TABLA;KEY;VALUE;TS)", tabla, registro->key, registro->value,registro->timestamp);
				segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
					if(segmentoBuscado==NULL)
						{
							log_info(logger, "[SEGMENTO INEXISTENTE] Segmento: %s", tabla);
							list_add(tablas, crearSegmento(tabla));
							paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas);
						} else { paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas); }
					free(registro->value);
					free(registro);
					return 1;
			}
			else{
				//para que esta este else??????
				log_warning(logger_mostrado, "[REGISTRO INEXISTENTE EN LFS] No existe el registro buscado");
				return 1;
				//Mostrar el status.mensaje o enviarlo a Kernel
			}
		}
	}	else {
			pagina_concreta* paginalala= traerPaginaDeMemoria(reg,memoria_principal);
			log_warning(logger_mostrado, "[REGISTRO EN MEMORIA] Posicion %i: (%i,%s,%i) | (KEY;VALUE;TS)", reg,paginalala->key, paginalala->value,paginalala->timestamp);
			free(paginalala->value);
			free(paginalala);
			return 1;
	}
}
/**
 * * @NAME: resolver_select_para_kernel
 	* @DESC: resuelve el select
 	*
 	*/
int resolver_select_para_kernel (int socket_kernel_fd, int socket_conexion_lfs,char* memoria_principal, t_list* tablas){
		//log_info(logger, "Se realiza SELECT a pedido de Kernel--");
		t_paquete_select* consulta_select = deserializar_select(socket_kernel_fd);

		//log_info(logger_mostrado, "SELECT");
		log_info(logger, "[SOLICITUD SELECT] TABLA: %s KEY %d", consulta_select->nombre_tabla->palabra, consulta_select->key);

		char* tabla = consulta_select->nombre_tabla->palabra;
		uint16_t key = consulta_select->key;
		int reg = 0;
		reg = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);

		if(reg==-1){
				//log_info(logger, "El registro con key '%d' NO se encuentra en memoria principal", key);
				int lruu = lru(memoria_principal,tablas);
				if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)){ //
										/*
										 * NO HAY ESPACIO PROXIMO DISPONIBLE Y LRU DICE QUE ESTÁ TODO OCPUADO
										 * -> hay que hacer journaling
										 *
										 */
										log_error(logger, "[MEMORIA FULL] No hay lugar en la memoria, debe realizarse JOURNAL", tabla);
										//journaling(memoria_principal, tablas);
										//consulta_select_a_usar = consulta_select;
										char* mensaje = "Memoria full";
										t_status_solicitud* status = crear_paquete_status(false,mensaje);
										enviar_status_resultado(status, socket_kernel_fd);
										eliminar_paquete_select(consulta_select);
										return 1;
				}  else {
					//log_info(logger, "Hay espacio en memoria, se procede a realizar la peticion a LFS", key);
					enviar_paquete_select(socket_conexion_lfs, consulta_select);
					t_status_solicitud* status= desearilizar_status_solicitud(socket_conexion_lfs);
					if(status->es_valido){
						t_registro* registro = obtener_registro(status->mensaje->palabra);
						log_info(logger,"[REGISTRO TRAIDO DESDE LFS] %s ; %d ; %s ; %i | (TABLA;KEY;VALUE;TS)", tabla, registro->key, registro->value,registro->timestamp);
						segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
							if(segmentoBuscado==NULL)
								{
									log_warning(logger, "[SEGMENTO INEXISTENTE] Segmento: %s", tabla);
									list_add(tablas, crearSegmento(tabla));
									paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas);
								} else { paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas); }

							enviar_status_resultado(status, socket_kernel_fd);
							//log_info(logger, "[ENVIO RESULTADO A KERNEL] OK - EXITO");
							free(registro->value);
							free(registro);
							eliminar_paquete_select(consulta_select);

							return 1;
					}
					else{
						//para que esta este else??????
						//log_error(logger, "No existe el registro buscado");
						log_error(logger, "[ENVIO RESULTADO A KERNEL] OK - REGISTRO INEXISTENTE");
						enviar_status_resultado(status, socket_kernel_fd);
						eliminar_paquete_select(consulta_select);

						return 1;
						//Mostrar el status.mensaje o enviarlo a Kernel
					}
				}
			}	else {
					pagina_concreta* paginalala= traerPaginaDeMemoria(reg,memoria_principal);
					log_info(logger, "[REGISTRO EN MEMORIA] Posicion %i: (%i,%s,%i) | (KEY;VALUE;TS)", reg,paginalala->key, paginalala->value,paginalala->timestamp);
					//log_info(logger, "[ENVIO RESULTADO A KERNEL] OK - EXITO");
					char* resultado = generar_registro_string(paginalala->timestamp, paginalala->key, paginalala->value);
					t_status_solicitud* status = crear_paquete_status(true, resultado);
					enviar_status_resultado(status, socket_kernel_fd);
					free(paginalala->value);
					free(paginalala);

					free(resultado);
					eliminar_paquete_select(consulta_select);

					return 1;
			}
		}
void resolver_despues_de_journaling (int socket_kernel_fd, t_paquete_select* consulta_select ,int socket_conexion_lfs,char* memoria_principal, t_list* tablas){
		log_info(logger, "Se realiza SELECT");
		log_info(logger, "Consulta en la tabla: %s", consulta_select->nombre_tabla->palabra);
		log_info(logger, "Consulta por key: %d", consulta_select->key);

		char* tabla = consulta_select->nombre_tabla->palabra;
		uint16_t key = consulta_select->key;
		int reg = 0;
		reg = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);

		if(reg==-1){
				log_info(logger, "El registro con key '%d' NO se encuentra en memoria principal", key);
				int lruu = lru(memoria_principal,tablas);
				if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)){ //
										/*
										 * NO HAY ESPACIO PROXIMO DISPONIBLE Y LRU DICE QUE ESTÁ TODO OCPUADO
										 * -> hay que hacer journaling
										 *
										 */
										log_error(logger, "No hay lugar en la memoria, debe realizarse JOURNAL", tabla);
										journaling(memoria_principal, tablas);
										consulta_select_a_usar = consulta_select;
				}  else {
					log_info(logger, "Hay espacio en memoria, se procede a realizar la peticion a LFS", key);
					enviar_paquete_select(socket_conexion_lfs, consulta_select);
					t_status_solicitud* status= desearilizar_status_solicitud(socket_conexion_lfs);
					if(status->es_valido){
						t_registro* registro = obtener_registro(status->mensaje->palabra);
						log_info(logger, "%s ; %d ; %s ; %i | (TABLA;KEY;VALUE;TS)", tabla, registro->key, registro->value,registro->timestamp);
						segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
							if(segmentoBuscado==NULL)
								{
									log_info(logger, "No existe el segmento: %s", tabla);
									list_add(tablas, crearSegmento(tabla));
									paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas);
								} else { paginaNueva(key,registro->value,registro->timestamp,tabla,memoria_principal,tablas); }
							enviar_status_resultado(status, socket_kernel_fd);
							free(registro->value);
							free(registro);
							free(consulta_select);

					}
					else{
						//para que esta este else??????
						log_error(logger, "No existe el registro buscado");
						log_info(logger, "Se procede a enviar el error a kernel");
						enviar_status_resultado(status, socket_kernel_fd);
						free(consulta_select);

						//Mostrar el status.mensaje o enviarlo a Kernel
					}
				}
			}	else {
					pagina_concreta* paginalala= traerPaginaDeMemoria(reg,memoria_principal);
					log_info(logger, "Posicion %i: (%i,%s,%i)", reg,paginalala->key, paginalala->value,paginalala->timestamp);
					log_info(logger, "Se procede a enviar el dato a kernel");
					//char* resultado = generar_registro_string(paginalala->timestamp, paginalala->key, paginalala->value);
					//t_status_solicitud* status = crear_paquete_status(true, resultado);
					//enviar_status_resultado(status, socket_kernel_fd);
					free(paginalala->value);
					free(paginalala);
					free(consulta_select);

			}
		}

		/*if(reg==-1){
			log_info(logger, "El registro con key '%d' NO se encuentra en memoria y procede a realizar la peticion a LFS", key);
			enviar_paquete_select(socket_conexion_lfs, consulta_select);
			t_status_solicitud* status= desearilizar_status_solicitud(socket_conexion_lfs);
			if(status){
				t_registro* registro = obtener_registro(status->mensaje->palabra);
				//Enviar el dato a kernel (no parsearlo)
				free(registro);
			}
			else{
			//Mostrar el status.mensaje o enviarlo a Kernel
			}

		eliminar_paquete_select(consulta_select);
		}
		else {
		log_info(logger, "El registro con key '%d' se encuentra en memoria en la posicion $i", key,reg);
		pagina_concreta* paginalala= traerPaginaDeMemoria(reg,memoria_principal);
		log_info(logger, "Se bajo de la memoria el registro: (%i,%s,%i)", paginalala->key, paginalala->value,paginalala->timestamp);
		log_info(logger, "Se procede a enviar el dato a kernel");
		free(paginalala->value);
		free(paginalala);
		}
	}*/

/**
 	* @NAME: buscarSegmento
 	* @DESC: Busca un segmento y lo logea
 	*
 	*/
void buscarSegmento(char* segment,t_list* tablas){
		segmento* unS = encontrarSegmento(segment, tablas);
		//log_info(logger,"SEGMENTO ENCONTRADO: %s", unS->nombreTabla);
	}
/**
 	* @NAME: traerPaginaDeMemoria
 	* @DESC: pasando una posicion y la memoria devuelve el dato contenido en el mismo
 	*
 	*/
pagina_concreta* traerPaginaDeMemoria(unsigned int posicion,char* memoria_principal){
	pagina_concreta* pagina= malloc(sizeof(pagina_concreta));
	memcpy(&(pagina->key), &memoria_principal[posicion*tamanio_pagina], sizeof(uint16_t));
	memcpy(&(pagina->timestamp), &memoria_principal[posicion*tamanio_pagina+sizeof(uint16_t)], sizeof(long));
	pagina->value = malloc(max_value);
	strcpy(pagina->value, &memoria_principal[posicion*tamanio_pagina+sizeof(uint16_t)+sizeof(long)]);
	return pagina;
	}
//void traerPaginaDeMemoria2(unsigned int posicion,char* memoriappal,pagina_concreta* pagina){
//	//pagina_concreta* pagina= malloc(sizeof(pagina_concreta));
//	memcpy(&(pagina->key), &memoriappal[posicion*tamanio_pagina], sizeof(uint16_t));
//	memcpy(&(pagina->timestamp), &memoriappal[posicion*tamanio_pagina+sizeof(uint16_t)], sizeof(long));
//	pagina->value = malloc(20);
//	strcpy(pagina->value, &memoriappal[posicion*tamanio_pagina+sizeof(uint16_t)+sizeof(long)]);
//	//return pagina;
//	}

/**
 	* @NAME: crearPagina
 	* @DESC: crea una pagina con una key y le asigna una posicion de memoria SE USA ADENTRO DE paginaNueva
 	*
 	*/
pagina* crearPagina(){
	pagina* paginaa = malloc(sizeof(pagina));
	paginaa->modificado=0;
	unsigned int posicionDondePonerElDatoEnMemoria;
	if(posicionProximaLibre>=cantidad_paginas){
			posicionDondePonerElDatoEnMemoria = lru(memoria_principal, tablas);
			eliminarPagina(posicionDondePonerElDatoEnMemoria, memoria_principal,tablas);
			} else {
				posicionDondePonerElDatoEnMemoria=posicionProximaLibre;
	}
	paginaa->posicionEnMemoria=posicionDondePonerElDatoEnMemoria;
	paginaa->ultimaLectura=(unsigned)time(NULL);;
	posicionProximaLibre+=1;
	return paginaa;
}
pagina* crearPaginaInsert(){
	pagina* paginaa = malloc(sizeof(pagina));
	paginaa->modificado=1;
	unsigned int posicionDondePonerElDatoEnMemoria;
	if(posicionProximaLibre>=cantidad_paginas){
		posicionDondePonerElDatoEnMemoria = lru(memoria_principal, tablas);
		eliminarPagina(posicionDondePonerElDatoEnMemoria, memoria_principal,tablas); // hay q eliminar la pagina!
		log_info(logger,"[REEMPLAZO DE PAGINA LRU] Posicion elegida:%i | PosicionProx: %i - CantPagsDisdp: %i",posicionDondePonerElDatoEnMemoria, posicionProximaLibre,cantidad_paginas-posicionProximaLibre);
		// log_info(logger,"[REEMPLAZO DE PAGINA LRU] posicionProxima:%i, cantidad:%i(lru encontro espacio)-> %i", posicionProximaLibre,cantidad_paginas,posicionDondePonerElDatoEnMemoria);
		} else {
			posicionDondePonerElDatoEnMemoria=posicionProximaLibre;
			log_info(logger,"[CREACION DE PAGINA POR ESPACIO DISPOBILE] Posicion en memoria:%i | CantPagsDisp: %i", posicionProximaLibre,cantidad_paginas-posicionProximaLibre);
			//log_info(logger,"[CREACION DE PAGINA POR ESPACIO DISPOBILE] posicionProxima:%i, cantidad:%i(hay espacio de una)", posicionProximaLibre,cantidad_paginas-posicionProximaLibre);
			posicionProximaLibre+=1;
	}
	paginaa->posicionEnMemoria=posicionDondePonerElDatoEnMemoria;
	paginaa->ultimaLectura=(unsigned)time(NULL);;
	//log_info(logger,"[INSERT EXITOSO] OK", paginaa->modificado);
	return paginaa;
}
void eliminarPagina(int posicionDondePonerElDatoEnMemoria, char* memoria_princial, t_list* tablas) {
	for(int j=0; j<tablas->elements_count; j++) {
		segmento* unSegmento=list_get(tablas, j);
		for(int i=0 ; i < unSegmento->paginas->elements_count ; i++)
		{
			pagina* pagin = list_get(unSegmento->paginas,i);
			if(pagin->posicionEnMemoria == posicionDondePonerElDatoEnMemoria){
				list_remove(unSegmento->paginas,i);
				free(pagin);
			}
		}
	}
}



/**
 	* @NAME: paginaNueva
 	* @DESC: crea una pagina en la tabla de paginas(bit,key,posicion) y la vuelca en memoria(asumiendo que existe el segmento) PUES RECIBE LOS DATOS DEL LFS (select)
 	* NOTA: DATOS EN MEMORIA KEY-TS-VALUE
 	*/
void paginaNueva(uint16_t key, char* value, long ts, char* tabla, char* memoria,t_list* tablas){
	pagina* pagina = crearPagina();	// deberia ser con malloc?
	agregarPaginaASegmento(tabla,pagina,tablas);
	//log_info(logger,"POSICION EN MMORIA: %i", pagina->posicionEnMemoria);
	memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina],&key,sizeof(uint16_t)); 					//deberia ser &key? POR ACA SEGMENTATION FAULT
	memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)],&ts,sizeof(long));			// mismo que arriba
	if(string_size(value)<= max_value){
		memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)],value,string_size(value));
	} else {
		memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)],value,max_value-1);
		strcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)+max_value-1], "");
	}

	/* memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)],value,max_value-1);
	strcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)+max_value-1], ""); */

	//memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)+max_value-1],'\0',1);
	//strcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)], value);
	//log_info(logger,"POSICION PROXIMA EN MMORIA DISPONIBLE: %i", posicionProximaLibre);
	}
void paginaNuevaInsert(uint16_t key, char* value, long ts, char* tabla, char* memoria,t_list* tablas){
	pagina* pagina = crearPaginaInsert();	// deberia ser con malloc?
	agregarPaginaASegmento(tabla,pagina,tablas);
	//log_info(logger,"POSICION EN MMORIA: %i", pagina->posicionEnMemoria);
	memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina],&key,sizeof(uint16_t)); 					//deberia ser &key? POR ACA SEGMENTATION FAULT
	memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)],&ts,sizeof(long));			// mismo que arriba
	if(string_size(value)<= max_value){
		memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)],value,string_size(value));
	} else {
		memcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)],value,max_value-1);
		strcpy(&memoria[(pagina->posicionEnMemoria)*tamanio_pagina+sizeof(uint16_t)+sizeof(long)+max_value-1], "");
	}


	//log_info(logger,"POSICION PROXIMA EN MMORIA DISPONIBLE: %i", posicionProximaLibre);
	}

/**
 	* @NAME: agregarPaginaASegmento
 	* @DESC: agrega una pagina a un segmento
 	*
 	*/
void agregarPaginaASegmento(char* tabla, pagina* pagina, t_list* tablas){
	segmento* segmentoBuscado = encontrarSegmento(tabla, tablas);
	pagina->ultimaLectura=(unsigned)time(NULL);
	list_add(segmentoBuscado->paginas, pagina);
	//log_info(logger,"Se agrego la pagina con posicion en memoria: %i , al segmento: %s , con ultimo tiempo de lectura = %i", pagina->posicionEnMemoria, segmentoBuscado->nombreTabla, pagina->ultimaLectura);
}
/**
 	* @NAME: crearSegmento
 	* @DESC: crea un segmento y lo agrega a la lista de segmentos
 	*
 	*/
segmento* crearSegmento(char* nombreTabla)
	{
	segmento* segmento1 = malloc(sizeof(segmento));
	segmento1->paginas = list_create();
	segmento1->nombreTabla=strdup(nombreTabla);
	log_info(logger, "[CREACION DE SEGMENTO] OK - Segmento %s", segmento1->nombreTabla);
	return segmento1;
	}
/**
	 	* @NAME: buscarRegistroEnTabla
	 	* @DESC:  dada una solicitud select, con el nombre de la tabla y la key busca en memoria el dato.
	 	* Si el dato no está en memoria, o la tabla no está en memoria retorna -1, caso contrario retorna la posicion.
	 	*
	 	*
	 	*/

int buscarRegistroEnTabla(char* tabla, uint16_t key, char* memoria_principal,t_list* tablas){
		segmento* segment = encontrarSegmento(tabla,tablas);
		if(segment==NULL){
			log_error(logger,"[REGISTRO INEXISTENTE] El registro no se encuentra en memoria");
			return -1;}
		for(int i=0 ; i < segment->paginas->elements_count ; i++)
		{
			pagina* pagin = list_get(segment->paginas,i);
			uint16_t pos = pagin->posicionEnMemoria;
			//log_info(logger,"Posicion en memoria: %i",pagin->posicionEnMemoria);
			pagina_concreta * pagc = traerPaginaDeMemoria(pos,memoria_principal);
			//log_info(logger,"Pagina Key: %i",pagc->key);
			int posicion=pagin->posicionEnMemoria;
			if(pagc->key == key){
				//log_info(logger,"Ultima lectura anterior: %i",pagin->ultimaLectura);
				//sleep(1); // si no hago sleep el ts es el mismo pq la ejecucion es super veloz jaja
				pagin->ultimaLectura=(unsigned)time(NULL);;
				//log_info(logger,"TS: %i",(unsigned)time(NULL));
				log_info(logger,"[LECTURA DE PAGINA] Se leyo la pagina %i en el TS: %i",pagin->posicionEnMemoria,pagin->ultimaLectura);
				free(pagc->value);
				free(pagc);
				return posicion;
			}
			free(pagc->value);
			free(pagc);
		}
		log_error(logger,"[REGISTRO INEXISTENTE] El registro no se encuentra en memoria");
		return -1;
}
/**
 	* @NAME: resolver_insert_para_kernel
 	* @DESC: resuelve el insert
 	*
 	*/
	int resolver_insert_para_kernel(int socket_kernel_fd, int socket_conexion_lfs,char* memoria_principal, t_list* tablas){
	t_paquete_insert* consulta_insert= deserealizar_insert(socket_kernel_fd);
	log_info(logger, "[SOLICITUD INSERT] KERNEL");
	log_warning(logger, "TABLA: %s", consulta_insert->nombre_tabla->palabra);
	log_warning(logger, "KEY: %i", consulta_insert->key);
	log_warning(logger, "VALUE: %s", consulta_insert->valor->palabra);
	log_warning(logger, "TS: %i", consulta_insert->timestamp);
	long tsss =consulta_insert->timestamp;
	char* tabla = consulta_insert->nombre_tabla->palabra;
	uint16_t key = consulta_insert->key;
	char* dato = consulta_insert->valor->palabra;
	segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
	int lruu= lru(memoria_principal,tablas);
	int registroAInsertar = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);

	if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)&&(registroAInsertar==-1)){ //
		log_error(logger, "[MEMORIA FULL] No hay lugar en la memoria, debe realizarse JOURNAL");
		bool respuesta = false;
		send(socket_kernel_fd,&respuesta,sizeof(bool),MSG_WAITALL);
		char* mensaje = "Memoria full";
		int tamanio = (int) strlen(mensaje)+1;
		send(socket_kernel_fd,&tamanio,sizeof(int),MSG_WAITALL);
		send(socket_kernel_fd,mensaje,tamanio,MSG_WAITALL);
		//journaling(memoria_principal, tablas);
		//consulta_insert_a_usar = consulta_insert;
		eliminar_paquete_insert(consulta_insert);
		return 1;
	} else {
	if(registroAInsertar==-1){ // no está el registro, hay que agregarlo -> bit mod en 1
		if(segmentoBuscado==NULL) // si el segmento no existe lo creo, y luego la pagina.
			{
			list_add(tablas, crearSegmento(tabla));
			paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
			eliminar_paquete_insert(consulta_insert);
			bool respuesta = true;
			send(socket_kernel_fd,&respuesta,sizeof(bool),MSG_WAITALL);
			return 1;
			} else { // existe el segmento, agrego la pagina al segmento
					//log_error(logger, "no encontre el registro en la tabla");
					paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
					eliminar_paquete_insert(consulta_insert);
					bool respuesta = true;
					send(socket_kernel_fd,&respuesta,sizeof(bool),MSG_WAITALL);
					log_error(logger, "[INSERT] Se agrego la key y el dato en memoria. OK.");
					return 1;
					}
	} else // el registro con esa key ya existe
			//modificarRegistro(key,dato,(unsigned)time(NULL),reg2,memoria_principal,tablas);
			modificarRegistro(key,dato,tsss,registroAInsertar,memoria_principal,tablas);
			cambiarBitModificado(tabla, key,tablas, memoria_principal);
			eliminar_paquete_insert(consulta_insert);
			bool respuesta = true;
			send(socket_kernel_fd,&respuesta,sizeof(bool),MSG_WAITALL);
			log_error(logger, "[INSERT] La key existe en memoria, se modifico en memoria. OK.");
			return 1;
	}
	/*}
	if(posicionProximaLibre+1>cantidad_paginas){
		/*
		 * PROCESO LRU
		 * proceso de creacion/modificacion de registro
		 */
/*
	} else {
		if(segmentoBuscado==NULL)
		{
			log_info(logger, "No existe el segmento: $s", consulta_insert->nombre_tabla->palabra);
			list_add(tablas, crearSegmento(consulta_insert->nombre_tabla->palabra));
			paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
		} else
			{
			int reg2 = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);
			if(reg2==-1)
				{
					log_info(logger, "El segmento existe y se encuentra en memoria, pero no la pagina");
					paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
				} else
					{
					log_info(logger, "El registro se encuentra en la posicion de memoria: %i , se procede a modificarlo",reg2);
					//modificarRegistro(key,dato,(unsigned)time(NULL),reg2,memoria_principal,tablas);
					modificarRegistro(key,dato,tsss,reg2,memoria_principal,tablas);
					cambiarBitModificado(tabla, key,tablas,memoria_principal);
					cambiarBitModificado(tabla,key,tablas,memoria_principal);
					}
			}
	}*/
	}

	void resolver_insert_despues_de_journaling(t_paquete_insert* consulta_insert, int socket_conexion_lfs,char* memoria_principal, t_list* tablas){
		log_info(logger, "Se realiza INSERT");
		log_info(logger, "Se busca insertar en la tabla: %s", consulta_insert->nombre_tabla->palabra);
		log_info(logger, "en la key: %i", consulta_insert->key);
		log_info(logger, "el dato: %s", consulta_insert->valor->palabra);
		log_info(logger, "El ts es: %i", consulta_insert->timestamp);
		long tsss =consulta_insert->timestamp;
		char* tabla = consulta_insert->nombre_tabla->palabra;
		uint16_t key = consulta_insert->key;
		char* dato = consulta_insert->valor->palabra;
		segmento* segmentoBuscado = encontrarSegmento(tabla,tablas);
		int lruu= lru(memoria_principal,tablas);
		int registroAInsertar = buscarRegistroEnTabla(tabla, key,memoria_principal,tablas);

		if(posicionProximaLibre>=cantidad_paginas&&(lruu==-1)&&(registroAInsertar==-1)){ //
			log_error(logger, "No hay lugar en la memoria, debe realizarse JOURNAL", tabla);
			journaling(memoria_principal, tablas);
			consulta_insert_a_usar = consulta_insert;

		} else {
		if(registroAInsertar==-1){ // no está el registro, hay que agregarlo -> bit mod en 1
			if(segmentoBuscado==NULL) // si el segmento no existe lo creo, y luego la pagina.
				{
				list_add(tablas, crearSegmento(tabla));
				paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
				free(consulta_insert);
				} else { // existe el segmento, agrego la pagina al segmento
						log_error(logger, "no encontre el registro en la tabla");
						paginaNuevaInsert(key,dato,tsss,tabla,memoria_principal,tablas);
						free(consulta_insert);

						}
		} else // el registro con esa key ya existe
				//modificarRegistro(key,dato,(unsigned)time(NULL),reg2,memoria_principal,tablas);
				modificarRegistro(key,dato,tsss,registroAInsertar,memoria_principal,tablas);
				cambiarBitModificado(tabla, key,tablas, memoria_principal);
				free(consulta_insert);

		}
	}

	void cambiarBitModificado(char* tabla, uint16_t key,t_list* tablas, char* memoria_principal){
			segmento* segment = encontrarSegmento(tabla,tablas);
			if(segment==NULL){log_error(logger,"No existe el segmento");}
			for(int i=0 ; i < segment->paginas->elements_count ; i++)
			{
				pagina* pagin = list_get(segment->paginas,i);
				uint16_t pos = pagin->posicionEnMemoria;

				pagina_concreta* pagc = traerPaginaDeMemoria(pos,memoria_principal);
				if(pagc->key == key){
					pagin->ultimaLectura=(unsigned)time(NULL);;
					pagin->modificado=1;
					log_warning(logger,"[ACTUALIZACION BIT MODIFICADO]Pagina con posicion en memoria %i modificada. - UTLIMO ACCESO: %i - BitMOD = %i",pagin->posicionEnMemoria,pagin->ultimaLectura, pagin->modificado);
					//log_info(logger,"vuelta %i",i);
				}
				free(pagc->value);
				free(pagc);
			}
	}
	void modificarRegistro(uint16_t key,char* dato,long tss,int posicion, char* memoria_principal, t_list* tablas){
		memcpy(&memoria_principal[posicion*tamanio_pagina],&key,sizeof(uint16_t)); 					//deberia ser &key? POR ACA SEGMENTATION FAULT
		memcpy(&memoria_principal[posicion*tamanio_pagina+sizeof(uint16_t)],&tss,sizeof(long));			// mismo que arriba
		strcpy(&memoria_principal[posicion*tamanio_pagina+sizeof(uint16_t)+sizeof(long)], dato);
	}


/*void resolver_insert2(int socket_kernel_fd, int socket_conexion_lfs){
	t_paquete_insert* consulta_insert = deserealizar_insert(socket_kernel_fd);
	log_info(logger, "Se realiza INSERT");
	log_info(logger, "Tabla: %s", consulta_insert->nombre_tabla->palabra);
	log_info(logger, "Key: %d", consulta_insert->key);
	log_info(logger, "Valor: %s", consulta_insert->valor->palabra);
	log_info(logger, "TIMESTAMP: %d", consulta_insert->timestamp);
	enviar_paquete_insert(socket_conexion_lfs, consulta_insert);
	eliminar_paquete_insert(consulta_insert);
}*/ //#########  FUNCION QUE NO SE USA

void resolver_create (int socket_kernel_fd, int socket_conexion_lfs){
	t_paquete_create* consulta_create = deserializar_create(socket_kernel_fd);
	log_info(logger, "[SOLICITUD CREATE]");
	log_warning(logger, "TABLA: %s", consulta_create->nombre_tabla->palabra);
	log_warning(logger, "PARTICIONES: %d", consulta_create->num_particiones);
	log_warning(logger, "TIEMPO COMPACTACION: %d", consulta_create->tiempo_compac);
	log_warning(logger, "CONSISTENCIA: %d", consulta_create->consistencia);
	enviar_paquete_create(socket_conexion_lfs, consulta_create);
	t_status_solicitud* status = desearilizar_status_solicitud(socket_conexion_lfs);
	enviar_status_resultado(status, socket_kernel_fd);
	//eliminar_paquete_status(status);
	eliminar_paquete_create(consulta_create);
}
void resolver_create_consola(int scoket_conexion_lfs, t_instruccion_lql instruccion_create){
	t_paquete_create* consulta_create = crear_paquete_create(instruccion_create);
	log_info(logger_mostrado, "[SOLICITUD CREATE]");
		log_warning(logger_mostrado, "TABLA: %s", consulta_create->nombre_tabla->palabra);
		log_warning(logger_mostrado, "PARTICIONES: %d", consulta_create->num_particiones);
		log_warning(logger_mostrado, "TIEMPO COMPACTACION: %d", consulta_create->tiempo_compac);
		log_warning(logger_mostrado, "CONSISTENCIA: %d", consulta_create->consistencia);
		enviar_paquete_create(socket_conexion_lfs, consulta_create);
		t_status_solicitud* status = desearilizar_status_solicitud(socket_conexion_lfs);
		if(status->es_valido){
		log_info(logger_mostrado,"[SOLICITUD CREATE] OK");
		} else {
			log_error(logger_mostrado,"[SOLICITUD CREATE] FALLO EN CREATE");
			log_error(logger_mostrado,"[SOLICITUD CREATE] Mensaje: %s",status->mensaje->palabra);
		}
		eliminar_paquete_status(status);
		eliminar_paquete_create(consulta_create);
}

void resolver_describe_drop(int socket_kernel_fd, int socket_conexion_lfs, char* operacion){
	t_paquete_drop_describe* consulta_describe_drop = deserealizar_drop_describe(socket_kernel_fd);
	//log_info(logger, "Se realiza %s", operacion);
	log_info(logger, "[SOLICITUD DROP] KERNEL");
	log_warning(logger, "TABLA: %s", consulta_describe_drop->nombre_tabla->palabra);
	if (operacion == "DROP"){ //TODO: cambiar para que reciba el enum y en el log usar una funcion que devuelva el string
		consulta_describe_drop->codigo_operacion=DROP;
	}else{
		consulta_describe_drop->codigo_operacion=DESCRIBE;
	}
	enviar_paquete_drop_describe(socket_conexion_lfs, consulta_describe_drop);

	t_status_solicitud* status = desearilizar_status_solicitud(socket_conexion_lfs);
	enviar_status_resultado(status, socket_kernel_fd);

	eliminar_paquete_drop_describe(consulta_describe_drop);
}
void resolver_drop_consola(t_instruccion_lql instruccion){
	log_info(logger_mostrado, "[SOLICITUD DROP] CONSOLA");
	log_warning(logger_mostrado, "Tabla: %s", instruccion.parametros.DROP.tabla);
	t_paquete_drop_describe* consulta = crear_paquete_drop_describe(instruccion);
	enviar_paquete_drop_describe(socket_conexion_lfs, consulta);
	t_status_solicitud* status =  desearilizar_status_solicitud(socket_conexion_lfs);
	log_info(logger_mostrado, "Respuesta: %s", status->mensaje->palabra);
	eliminar_paquete_drop_describe(consulta);
}

void resolver_describe_para_kernel(int socket_kernel_fd, int socket_conexion_lfs, char* operacion){
	t_paquete_drop_describe* consulta_describe = deserealizar_drop_describe(socket_kernel_fd);
	consulta_describe->codigo_operacion=DESCRIBE;

	if(string_is_empty(consulta_describe->nombre_tabla->palabra)){
		//Si es un DESCRIBE global, le pido al LFS el numero de tablas. Luego mando el numero de tablas
		//al kernel. Luego voy a deserializar la metadata tantas veces como sea necesario y mandar a Kernel.

		log_info(logger, "[SOLICITUD DESCRIBE GLOBAL] Enviando peticion");
		enviar_paquete_drop_describe(socket_conexion_lfs, consulta_describe);
		int cant_tablas= recibir_numero_de_tablas (socket_conexion_lfs);
		log_warning(logger, "CANTIDAD DE TABLAS EN LFS: %d", cant_tablas);
		enviar_numero_de_tablas(socket_kernel_fd, cant_tablas);
		for(int i=0; i<cant_tablas; i++){
			t_metadata* metadata = deserealizar_metadata(socket_conexion_lfs);
			enviar_paquete_metadata(socket_kernel_fd, metadata);
			eliminar_metadata(metadata);
		}
	}
	else{
		//Si es un DESCRIBE de una tabla especifica...
		log_info(logger, "[SOLICITUD DESCRIBE ESECIFICO] Enviando peticion de tabla %s", consulta_describe->nombre_tabla->palabra);
		enviar_paquete_drop_describe(socket_conexion_lfs, consulta_describe);
		t_status_solicitud* status = desearilizar_status_solicitud(socket_conexion_lfs);
		enviar_status_resultado(status, socket_kernel_fd);
		//eliminar_paquete_status(status);
		if (status->es_valido){
			t_metadata* metadata = deserealizar_metadata(socket_conexion_lfs);
			enviar_paquete_metadata(socket_kernel_fd, metadata);
			eliminar_metadata(metadata);
		}
//		WARNING: Tiene un free de mas que aca estalla y en el resto no
//		eliminar_paquete_status(status);
	}
	eliminar_paquete_drop_describe(consulta_describe);
}
void resolver_describe_consola(t_instruccion_lql instruccion){
	t_paquete_drop_describe* consulta = malloc(sizeof(t_paquete_drop_describe));
	consulta->nombre_tabla = malloc(sizeof(t_buffer));
	int size_nombre_tabla = strlen(instruccion.parametros.DESCRIBE.tabla)+1;
	log_info(logger,"Tabla %s , largo %i",instruccion.parametros.DESCRIBE.tabla,size_nombre_tabla);
	consulta->nombre_tabla->size =  size_nombre_tabla;
	consulta->nombre_tabla->palabra = malloc(size_nombre_tabla);
	memcpy(consulta->nombre_tabla->palabra ,instruccion.parametros.DESCRIBE.tabla, size_nombre_tabla);
	consulta->codigo_operacion=DESCRIBE;

	if(string_is_empty(consulta->nombre_tabla->palabra)){
		//Si es un DESCRIBE global, le pido al LFS el numero de tablas. Luego mando el numero de tablas
		//al kernel. Luego voy a deserializar la metadata tantas veces como sea necesario y mandar a Kernel.
		log_info(logger_mostrado,"[SOLICITUD DESCRIBE GLOBAL] Enviando peticion");
		enviar_paquete_drop_describe(socket_conexion_lfs, consulta);
		int cant_tablas= recibir_numero_de_tablas(socket_conexion_lfs);
		log_warning(logger, "CANTIDAD DE TABLAS EN LFS: %d", cant_tablas);
		for(int i=0; i<cant_tablas; i++){
			t_metadata* metadata = deserealizar_metadata(socket_conexion_lfs);
			log_warning(logger_mostrado, "NOMBRE: %s",metadata->nombre_tabla->palabra);
			log_warning(logger_mostrado, "CONSISTENCIA: %i",metadata->consistencia);
			log_warning(logger_mostrado, "PARTICIONES: %i",metadata->n_particiones);
			log_warning(logger_mostrado, "T. COMPACTACION: %i",metadata->tiempo_compactacion);
			free(metadata->nombre_tabla->palabra);
			free(metadata->nombre_tabla);
			free(metadata);
		}
	}
	else{
		//Si es un DESCRIBE de una tabla especifica...
		log_info(logger_mostrado, "[SOLICITUD DESCRIBE ESPECIFICO] Enviando peticion de tabla %s", consulta->nombre_tabla->palabra);
		enviar_paquete_drop_describe(socket_conexion_lfs, consulta);
		t_status_solicitud* status = desearilizar_status_solicitud(socket_conexion_lfs);
		//eliminar_paquete_status(status);
		if (status->es_valido){
			t_metadata* metadata = deserealizar_metadata(socket_conexion_lfs);
			log_warning(logger_mostrado, "NOMBRE: %s",metadata->nombre_tabla->palabra);
			log_warning(logger_mostrado, "CONSISTENCIA: %i",metadata->consistencia);
			log_warning(logger_mostrado, "PARTICIONES: %i",metadata->n_particiones);
			log_warning(logger_mostrado, "T. COMPACTACION: %i",metadata->tiempo_compactacion);
			free(metadata->nombre_tabla->palabra);
			free(metadata->nombre_tabla);
			free(metadata);
		}else{
			log_error(logger_mostrado, "[DESCRIBE ERROR]Error: %s",status->mensaje->palabra);
		}

	}

}

 void iniciar_logger(char* pathlog) {
	logger_mostrado = log_create(pathlog, "Memoria", 1, LOG_LEVEL_INFO);
	logger = log_create(pathlog, "Memoria", 0, LOG_LEVEL_INFO);
 }
 void iniciar_logger_gossip(char* pathlog) {
	logger_g = log_create(pathlog, "Memoria", 0, LOG_LEVEL_INFO);
 }

 /**
 	* @NAME: leer_config
 	* @DESC: lee(abre)el archivo config
 	*
 	*/
 	void leer_config(char* path) {
 	archivoconfig = config_create(path);
 	}
/**
	* @NAME: terminar_programa
	* @DESC: termina el programa
	*
	*/
 	void terminar_programa(int conexionKernel, int conexionALFS)
	{
	liberar_conexion(conexionKernel);
	liberar_conexion(conexionALFS);
	log_destroy(logger);
	config_destroy(archivoconfig);
}
/**
	* @NAME: ejecutar_API_desde_Kernel
	* @DESC: resuelve la operacion recibida
	*
	*/
	void ejecutar_API_desde_Kernel(int socket_memoria, t_operacion cod_op,char* memoria_principal, t_list* tablas){
	switch(cod_op)
				{
				case HANDSHAKE:
					//log_info(logger, "Inicia handshake con %i", socket_memoria);
					recibir_mensaje(logger, socket_memoria);
					enviar_handshake(socket_memoria, "OK");
					log_info(logger_mostrado, "Conexion exitosa con KERNEL stocket %i OK", socket_memoria);
					break;
				case SELECT:
					//log_info(logger, "%i solicitó SELECT", socket_memoria);
					//resolver_select_para_kernel(socket_memoria, socket_conexion_lfs,memoria_principal,tablas);
					pthread_mutex_lock(&mutexMemoria);
					if(resolver_select_para_kernel(socket_memoria, socket_conexion_lfs,memoria_principal,tablas)==-1){resolver_despues_de_journaling(socket_memoria,consulta_select_a_usar,socket_conexion_lfs,memoria_principal, tablas);}
					pthread_mutex_unlock(&mutexMemoria);
					//log_info(logger, "SELECT Desde KERNEL OK");
					//aca debería enviarse el mensaje a LFS con SELECT
					break;
				case INSERT:
					//log_info(logger, "%i solicitó INSERT", socket_memoria);
					pthread_mutex_lock(&mutexMemoria);
					if(resolver_insert_para_kernel(socket_memoria, socket_conexion_lfs,memoria_principal, tablas)==-1) {resolver_insert_despues_de_journaling(consulta_insert_a_usar, socket_conexion_lfs,memoria_principal,tablas);}
					pthread_mutex_unlock(&mutexMemoria);
					//log_info(logger, "INSERT Desde KERNEL OK");
					//aca debería enviarse el mensaje a LFS con INSERT
					break;
				case CREATE:
					//log_info(logger, "%i solicitó CREATE", socket_memoria);
					resolver_create(socket_memoria, socket_conexion_lfs);
					//log_info(logger, "CREATE Desde KERNEL OK");
					//aca debería enviarse el mensaje a LFS con CREATE
					break;
				case DESCRIBE:
					//log_info(logger, "%i solicitó DESCRIBE", socket_memoria);
					resolver_describe_para_kernel(socket_memoria, socket_conexion_lfs, "DESCRIBE");
					//log_info(logger, "DESCRIBE Desde KERNEL OK");
					//aca debería enviarse el mensaje a LFS con DESCRIBE
					break;
				case DROP:
					//log_info(logger, "%i solicitó DROP", socket_memoria);
					resolver_describe_drop(socket_memoria, socket_conexion_lfs, "DROP");
					//log_info(logger, "DROP Desde KERNEL OK");
					//aca debería enviarse el mensaje a LFS con DROP
					break;
				case SOLICITUD_TABLA_GOSSIPING:
					enviar_mi_tabla_de_gossiping(socket_memoria);
					log_info(logger, "Tablas de GOSSIPING enviadas a KERNEL OK");
					break;
				case GOSSPING:
					//log_info(logger, "La memoria %i solicitó GOSSIPING", socket_memoria);
					resolver_gossiping(socket_memoria);
					//log_info(logger, "GOSSIPING de %i OK", socket_memoria);
					break;
				case JOURNAL:
					//log_info(logger, "Se soilicito Journaling");
					pthread_mutex_lock(&mutexMemoria);
					journaling(memoria_principal,tablas);
					pthread_mutex_unlock(&mutexMemoria);
					log_warning(logger_mostrado, "JOURNAL FORZADO DESDE KERNEL OK");
					break;
				case -1:
					log_error(logger, "el cliente se desconecto. Terminando conexion con %i", socket_memoria);
					break;
				default:
					log_error(logger_mostrado, "OPERACION DESCONOCIDA");
					break;
				}
	}

/**
	* @NAME: levantar_datos_memoria
	* @DESC: levanto datos desde el archivo de configuracion(mi puerto, mi ip, mi nombre, mi tamaño)
	*
	*/
	void levantar_datos_memoria(){
	ip_memoria = config_get_string_value(archivoconfig, "IP_MEMORIA"); // asignamos IP de memoria a conectar desde CONFIG
	log_info(logger, "La IP de la memoria es %s",ip_memoria );
	puerto_memoria = config_get_string_value(archivoconfig, "PUERTO_MEMORIA"); // asignamos puerto desde CONFIG
	log_info(logger, "El puerto de la memoria es %s",puerto_memoria);
	nombre_memoria = config_get_string_value(archivoconfig, "MEMORY_NUMBER"); // asignamos NOMBRE desde CONFIG
	log_info(logger, "El nombre de la memoria es %s",nombre_memoria);
	tamanio_memoria = config_get_int_value(archivoconfig, "TAM_MEM");
	log_info(logger, "El tamaño de la memoria es: %i",tamanio_memoria);
	retardo_journaling = config_get_int_value(archivoconfig, "RETARDO_JOURNAL")/1000;
	log_info(logger, "El retardo del journaling automatico es: %i",retardo_journaling);
	retardo_gossiping = config_get_int_value(archivoconfig, "RETARDO_GOSSIPING")/1000;
	log_info(logger, "El retardo de gossiping automatico es: %i",retardo_gossiping);
	}

/**
	* @NAME: levantar_datos_lfs
	* @DESC: levanto datos desde el archivo de configuracion(puerto del lfs, ip del lfs)
	*
	*/
	void levantar_datos_lfs(){
	ip__lfs = config_get_string_value(archivoconfig, "IP_LFS"); // asignamos IP de memoria a conectar desde CONFIG
	log_info(logger, "La IP del LFS es %s", ip__lfs);
	puerto__lfs = config_get_string_value(archivoconfig, "PUERTO_LFS"); // asignamos puerto desde CONFIG
	log_info(logger, "El puerto del LFS es %s", puerto__lfs);
	}
/**
	* @NAME: intentar_handshake_a_lfs
	* @DESC: se intenta hacer handshake con lfs
	*
	*/
	void intentar_handshake_a_lfs(int alguien){
	char* mensajee = "Hola me conecto soy la memoria: ";
	char* mensaje_env = malloc(strlen(mensajee)+strlen(nombre_memoria)+1);
	memcpy(mensaje_env,mensajee,strlen(mensajee));
	memcpy(mensaje_env+strlen(mensajee),nombre_memoria,strlen(nombre_memoria)+1);
		log_info(logger, "[SOLICITUD HANDSHAKE]Trato de realizar un hasdshake");
		if (enviar_handshake(alguien,mensaje_env)){
			log_warning(logger, "Se envió el mensaje %s", mensaje_env);
			free(mensaje_env);
			recibir_datos(logger, alguien);
			log_warning(logger_mostrado,"Conexion exitosa con LFS");
		}
	}

/**
	* @NAME: recibir_datos
	* @DESC: recibe datos en el handshake
	*
	*/
	void recibir_datos(t_log* logger,int socket_fd){
	int cod_op = recibir_operacion(socket_fd);
	if (cod_op == HANDSHAKE){
		recibir_max_value(logger, socket_fd);
	}else{
		log_error(logger, "[ERROR HANDSHAKE] No se realizó correctamente el HANDSHAKE");
	}
	}
/**
	* @NAME: recibir_max_value
	* @DESC: usada en el handsake para recibir el max_value que es el valor maximo de value(determina tamanio_pagina)
	*
	*/
	void recibir_max_value(t_log* logger, int socket_cliente)
	{
	int size;
	char* buffer = recibir_buffer(&size, socket_cliente);
	log_info(logger, "[MAX VALUE] El tamaño maximo de value es %s", buffer);
	max_value = atoi(buffer);
	tamanio_pagina=sizeof(uint16_t)+(sizeof(char)*max_value)+sizeof(long);
	cantidad_paginas = tamanio_memoria/tamanio_pagina;
	log_info(logger, "[MAX VALUE] Luego el tamaño de la pagina será %i", tamanio_pagina);
	log_warning(logger, "[MAX VALUE] La memoria de %i b, tendrá %i paginas", tamanio_memoria, tamanio_memoria/tamanio_pagina);
	free(buffer);
	}

/**
	* @NAME: iniciar_servidor_memoria_y_esperar_conexiones_kernel
	* @DESC:
	*
	*/
	void iniciar_servidor_memoria_y_esperar_conexiones_kernel(){
	log_info(logger, "Memoria lista para recibir a peticiones de Kernel");
	log_info(logger, "Memoria espera peticiones");
	socket_kernel_conexion_entrante = esperar_cliente(server_memoria);
	log_info(logger, "Memoria se conectó con Kernel");
	}

/**
	* @NAME: iniciarTablaDeGossiping
	* @DESC: inicia la tabla de gossiping
	*
	*/
	void iniciarTablaDeGossiping(){
		tablaGossiping=list_create();
		t_gossip* primerElemento = malloc(sizeof(t_gossip));
		primerElemento->ip_memoria = malloc(strlen(ip_memoria)+1);
		memcpy(primerElemento->ip_memoria,ip_memoria,strlen(ip_memoria)+1);
		primerElemento->puerto_memoria = malloc(strlen(puerto_memoria)+1);
		memcpy(primerElemento->puerto_memoria,puerto_memoria,strlen(puerto_memoria)+1);
		primerElemento->nombre_memoria = malloc(strlen(nombre_memoria)+1);
		memcpy(primerElemento->nombre_memoria ,nombre_memoria,strlen(nombre_memoria)+1);
		list_add(tablaGossiping,primerElemento);
	}
/**
	* @NAME: levantarPuertosSeeds
	* @DESC: levanta las seeds
	*
	*/
	char** levantarSeeds(){
	return config_get_array_value(archivoconfig, "IP_SEEDS");
	}
/**
	* @NAME: levantarPuertosSeeds
	* @DESC: levanta los puertos de las seeds
	*
	*/
	char** levantarPuertosSeeds(){
	return config_get_array_value(archivoconfig, "PUERTO_SEEDS");
	}
/**
	* @NAME: iniciarGossip
	* @DESC: inicia proceso de gossiping
	*
	*/
/**
	* @NAME: seedsCargadas
	* @DESC: logea las seeds cargadas
	*
	*/
	void seedsCargadas(){
	int i=0;
	while(seeds[i]!= NULL){
		log_info(logger_mostrado, "Se obtuvo la seed con ip: %s , y puerto: %s",seeds[i],puertosSeeds[i]);
		i++;
	}
	log_info(logger, "Se obtuvieron %i seeds correctamente.",i);
	}

/**
	* @NAME: select_esperar_conexiones_o_peticiones
	* @DESC:
	*
	*/
	void select_esperar_conexiones_o_peticiones(char* memoria_principal,t_list* tablas){
		FD_SET(server_memoria, &master); // agrego el socket de esta memoria(listener, está a la escucha)al conjunto maestro
		fdmin = server_memoria;
		fdmax = server_memoria; // por ahora el socket de mayor valor es este, pues es el unico ;)
		log_info(logger,"A la espera de nuevas solicitudes");
		for(;;) 	// bucle principal
		{
			read_fds = master; // cópialo
				if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1)
					{
						log_error(logger_mostrado, "Fallo accion select.");;
						exit(1);
					}
					// explorar conexiones existentes en busca de datos que leer
					for(int i = 0; i <= fdmax; i++) {
						if (FD_ISSET(i, &read_fds)) //  pregunta si "i" está en el conjunto ¡¡tenemos datos!! read_fds es como una lista de sockets con conexiones entrantes
							{
								if (i == server_memoria) // si estoy parado en el socket que espera conexiones nuevas (listen)
									{
									memoriaNuevaAceptada = esperar_cliente(server_memoria);
									FD_SET(memoriaNuevaAceptada, &master); // añadir al conjunto maestro
									if (memoriaNuevaAceptada > fdmax)
										{    // actualizar el máximo
										fdmax = memoriaNuevaAceptada;
										}
									//log_info(logger,"[NUEVA CONEXION] %i",memoriaNuevaAceptada);
									} else 				// // gestionar datos de un cliente
									{
										int cod_op;
										nbytes = recv(i, &cod_op, sizeof(int), 0);
										if (nbytes <= 0) {
											// error o conexión cerrada por el cliente
											if (nbytes == 0) {
											// conexión cerrada
												//log_error(logger, "[CONEXION TERMINADA] %i", i);
											} else {
												perror("recv");
											}
											close(i); // bye!
											FD_CLR(i, &master); // eliminar del conjunto maestro
										}
										else {
																	// tenemos datos de algún cliente
											ejecutar_API_desde_Kernel(i,cod_op,memoria_principal,tablas);
											// log_info(logger, "Se recibio una operacion de %i", i);

										}}
							}
					}
		}
	}

/**
	* @NAME: journaling
	* @DESC:
	*  envia todos los datos modificados de la memoria al lfs
	*
	*
	*/
void journaling(char* memoria_principal,t_list* tablas){
	log_info(logger,"[JOURNALING AUTOMATICO] INICIO");
	for(int i=0;i<tablas->elements_count;i++){
		segmento* unSegmento=list_get(tablas,i);
		//log_info(logger,"[CHEQUEANDO TABLA] TABLA : %s ",unSegmento->nombreTabla);

		for(int j=0;j<unSegmento->paginas->elements_count;j++) {
			pagina* unaPagina = list_get(unSegmento->paginas,j);
			pagina_concreta* datosPagina = traerPaginaDeMemoria(unaPagina->posicionEnMemoria,memoria_principal);
			/* envio todo */
			t_paquete_insert* paquete_insert = malloc(sizeof(t_paquete_insert));
			paquete_insert->codigo_operacion=INSERT;
			memcpy(&(paquete_insert->key),&(datosPagina->key),sizeof(uint16_t));
			memcpy(&(paquete_insert->timestamp),&(datosPagina->timestamp),sizeof(long));
			char* nombre = datosPagina->value;
			char* tablan = unSegmento->nombreTabla;
			paquete_insert->valor = malloc(sizeof(int)+string_size(nombre));
			paquete_insert->nombre_tabla=malloc(sizeof(int)+string_size(tablan));
			paquete_insert->valor->palabra= malloc(string_size(nombre));
			paquete_insert->valor->size=string_size(nombre);
			memcpy(paquete_insert->valor->palabra,nombre,string_size(nombre));
			paquete_insert->nombre_tabla->palabra = malloc(string_size(tablan));
			memcpy(paquete_insert->nombre_tabla->palabra,tablan,string_size(tablan));
			paquete_insert->nombre_tabla->size=string_size(tablan);
			log_warning(logger,"[INSERT]( %i , %i , %s , %s ) - ( KEY , TIMESTAMP , VALUE , TABLA ) ",paquete_insert->key,paquete_insert->timestamp,paquete_insert->valor->palabra,paquete_insert->nombre_tabla->palabra);
			//log_info(logger,"Ts %i",paquete_insert->timestamp);
			//log_info(logger,"Valor %s",paquete_insert->valor->palabra);
			//log_info(logger,"Tabla %s",paquete_insert->nombre_tabla->palabra);
			enviar_paquete_insert(socket_conexion_lfs,paquete_insert);
			t_status_solicitud* status =  desearilizar_status_solicitud(socket_conexion_lfs);
			//esto se puede mostrar o no, pero hay qe recibirlo si o si
			log_info(logger, "[RESULTADO]: %s", status->mensaje->palabra);
			free(paquete_insert->valor->palabra);
			free(paquete_insert->nombre_tabla->palabra);
			free(paquete_insert->valor);
			free(paquete_insert->nombre_tabla);
			free(paquete_insert);
			free(datosPagina->value);
			free(datosPagina);
			free(unaPagina);
			eliminar_paquete_status(status);
		}
		list_clean(unSegmento->paginas);
		//log_info(logger, "[Paginas en segmento %s : %i]", unSegmento->nombreTabla, unSegmento->paginas->elements_count);

	} 	posicionProximaLibre=0;
		//log_info(logger,"CANTIDAD DE TABLAS: %",tablas->elements_count);
		log_info(logger,"[JOURNALING AUTOMATICO] FIN");
}

/**
	* @NAME: lru
	* @DESC: Se ejecuta el algoritmo de sustitucion, recorre todas las paginas, si encuentra
	*  paginas sin modificar las compara y devuelve la que tenga el ts mas antiguo, si no
	*  devuelve -1, lo que significa que estan todas las paginas modificadas y hay que hacer un journal.
	*
	*/
	int lru(char* memoria_principal, t_list* tablas){
			unsigned int posicionMemo=-1;
			long ultimaLectura=-1;
			int segment=0;
			int pagg=0;
			//log_info(logger, "[TABLAS= %i ]",tablas->elements_count);
			for(int i=0; i<(tablas->elements_count); i++){	// recorro segmentos
					segmento* segmente = list_get(tablas, i);
					//log_info(logger, "[Ciclo segmento %i , cant pags= %i ]",i, segmente->paginas->elements_count);
					for(int j=0; j<segmente->paginas->elements_count; j++) {// en un segmento i, recorro sus paginas
						pagina* pag = list_get(segmente->paginas, j);		// traigo pagina
						if(ultimaLectura==-1 && pag->modificado==0)
						{			// si es la primer pagina que leo sin modificar
							posicionMemo=pag->posicionEnMemoria;			// asigno la posicion de memoria para retornarla
							ultimaLectura=pag->ultimaLectura;				// guardo su ultima lectura para seguir comparando.
							//log_info(logger, "[PM=-1 // PRIMER VUELTA J=%i]Ultima lectura %i",j,ultimaLectura);
							//log_info(logger, "[PM=-1 // PRIMER VUELTA J=%i]Posicion memo %i",j,posicionMemo);
							segment=i;										// determino el segmento y
							pagg=j;											// la pagina para despues poder eliminarla de mis tablas

						} else if(pag->modificado==0)
						{// si no es la primer pagina que leo sin modificar y aparte hace mas tiempo que no se lee
							if(pag->ultimaLectura<ultimaLectura){
							posicionMemo=pag->posicionEnMemoria;							// asigo la posicion de memoria para retornarla
							ultimaLectura=pag->ultimaLectura;								// guardo su ultima lectura para seguir comparando
							//log_info(logger, "[CICLO PAG J=%i]Ultima lectura %i",j,ultimaLectura);
							//log_info(logger, "[CICLO PAG J=%i]Posicion memo %i",j,posicionMemo);
							segment=i;														// determino el segmento y
							pagg=j;
							}// la pagina para despues poder eliminarla de mis tablas
						} //else log_info(logger, "NO CUMPLE");
					}
				}
														// termino de recorrer las tablas
		if(ultimaLectura==-1) { return ultimaLectura; }	// si la posicion es -1 significa que no encontro ni una pagina que no estuviera modificada, devuevlo -1, hay que hacer journal
		//segmento* segm = list_get(tablas,segment);		// si sigue la ejecucion, es que encontro paginas, procedo a eliminar esa pagina que va a ser sustituda, traigo el segmento
		//list_remove(segm->paginas, pagg);				// elimino de la lista de paginas del segmento a la pagina indicada
		//log_info(logger, "TERMINE CICLOS? %i",posicionMemo);
		return posicionMemo;							// retorno la direccion en memoria sobre el que voy a escribir mi nuevo registro dato.
	}





/**
* @NAME: traerRegistroDeMemoria
* @DESC: Retorna una pagina desde memoria conociendo su posicion.
*
*/
pagina_concreta* traerRegistroDeMemoria(int posicion){
			pagina_concreta* pagina = malloc(sizeof(uint16_t)+(sizeof(char)*max_value)+sizeof(long));
			unsigned int desplazamiento=0;
			memcpy(pagina->key,&memoria_principal[posicion*tamanio_pagina],sizeof(uint16_t));					// agrego primero KEY
			desplazamiento+=sizeof(uint16_t);
			memcpy(pagina->timestamp,&memoria_principal[posicion*tamanio_pagina+desplazamiento],sizeof(long));	// agrego luego TIMESTAMP
			desplazamiento+=sizeof(long);
			strcpy(pagina->value,&memoria_principal[posicion*tamanio_pagina+desplazamiento]);					// agrego por ultimo VALUE(tamaño variable)
			return pagina;
}

/**
* @NAME: encontrarSegmento
* @DESC: Retorna el segmento buscado en la lista si este tiene el mismo nombre que el que buscamos.
*
*/
segmento* encontrarSegmento(char* nombredTabla, t_list* tablas) {
	int _es_el_Segmento(segmento* segmento) {
		return string_equals_ignore_case(segmento->nombreTabla, nombredTabla);
	}

	return list_find(tablas, (void*) _es_el_Segmento);
}
/**
	* @NAME: encontrarTabla
	* @DESC: Retorna ela tabla buscada en la lista si este tiene el mismo nombre que el que buscamos.
	*
	*/

t_gossip* encontrarMemoria(char* nombredMemoria, t_list* memorias) {
	int _es_la_Memoria(t_gossip* memoria) {
		return string_equals_ignore_case(memoria->nombre_memoria, nombredMemoria);
	}

	return list_find(memorias, (void*) _es_la_Memoria);
}


void* iniciar_inotify(char **argv){
	#define MAX_EVENTS 1024 /*Max. number of events to process at one go*/
	#define LEN_NAME 64 /*Assuming that the length of the filename won't exceed 16 bytes*/
	#define EVENT_SIZE  ( sizeof (struct inotify_event) ) /*size of one event*/
	#define BUF_LEN     ( MAX_EVENTS * ( EVENT_SIZE + LEN_NAME )) /*buffer to store the data of events*/

	char* path1 = malloc(strlen(argv[1])+strlen(path)+1);
	memcpy(path1,path,strlen(path));
	memcpy(path1+strlen(path),argv[1],strlen(argv[1])+1);
	int length, wd;
	int fd;
	char buffer[BUF_LEN];

	fd = inotify_init();
	if( fd < 0 ){
		log_error(logger_mostrado, "[INOTIFY ERROR] No se pudo inicializar Inotify");
	}

	/* add watch to starting directory */
	wd = inotify_add_watch(fd, path1, IN_MODIFY);

	if(wd == -1){
		log_error(logger_mostrado, "[INOTIFY ERROR] No se pudo agregar una observador para el archivo: %s\n",path);
	}else{
		//log_info(logger, "Inotify esta observando modificaciones al archivo: %s\n", path);
		log_info(logger_mostrado, "[INOTIFY] Inotify OK");

	}

	while(1){

		length = read( fd, buffer, BUF_LEN );
		if(length < 0){
			perror("read");
		}else{
			struct inotify_event *event = ( struct inotify_event * ) buffer;
			if(event->mask == IN_MODIFY){
				log_warning(logger_mostrado, "[INOTIFY] Se modifico el archivo de configuración");
				leer_config(path1);
				retardo_journaling = config_get_int_value(archivoconfig, "RETARDO_JOURNAL");
				log_warning(logger_mostrado, "[INOTIFY] El retardo del journaling automatico es: %i",retardo_journaling);
				retardo_gossiping = config_get_int_value(archivoconfig, "RETARDO_GOSSIPING");
				log_warning(logger_mostrado, "[INOTIFY] El retardo de gossiping automatico es: %i",retardo_gossiping);
			}
		}
	}
	inotify_rm_watch( fd, wd );
	close( fd );
}

void iniciar_hilo_inotify_memoria(char **argv){
	pthread_t hilo_inotify;
	if (pthread_create(&hilo_inotify, 0, iniciar_inotify, argv) !=0){
			log_error(logger_mostrado, "[INOTIFY ERROR] Error al crear el hilo de Inotify");
		}
	if (pthread_detach(hilo_inotify) != 0){
			log_error(logger_mostrado, "[INOTIFY ERROR] Murio el hilo de inotify");
		}
}
