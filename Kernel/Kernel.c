/*
 * Kernel.c
 *
 *  Created on: 7 abr. 2019
 *      Author: utnso
 */
//#include "Kernel.h"
#include "Kernel_Metrics.h"

int ID=0;
t_list* memorias_disponibles;
int QUANTUM;
int socket_memoria;
int SLEEP_EJECUCION = 0;
int CANT_EXEC = 0;
int CANT_METRICS = 0;
char* ARCHIVO_CONFIG = "/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/Kernel/kernel.config";
int contador=0;


int main(void){

	iniciar_logger(); // creamos log
	leer_config(); // abrimos config
	IP_MEMORIA = config_get_string_value(archivoconfig, "IP_MEMORIA"); // asignamos IP de memoria a conectar desde CONFIG
	PUERTO_MEMORIA = config_get_string_value(archivoconfig, "PUERTO_MEMORIA"); // asignamos puerto desde CONFIG
	log_info(logger, "La IP de la memoria es %s", IP_MEMORIA);
	log_info(logger, "El puerto de la memoria es %s", PUERTO_MEMORIA);

	leer_atributos_config();

	// CONECTO!
	socket_memoria = crear_conexion(IP_MEMORIA,PUERTO_MEMORIA); // conecto el socketKernel(ESTE PROCESO) con la memoria
	log_info(logger,"Creada la conexion para la memoria");
	char *mensaje = "Hola, me conecto, soy el Kernel";
	log_info(logger, "Trato de realizar un hasdshake");
	if (enviar_handshake(socket_memoria,mensaje)){
		log_info(logger, "Se envió el mensaje %s", mensaje);

		recibir_handshake(logger, socket_memoria);
		//log_info(logger,"Conexion exitosa con Memoria");
	}

	t_memoria* memoria_principal = malloc(sizeof(t_memoria));
	memoria_principal->ip = malloc(string_size(IP_MEMORIA));
	memcpy(memoria_principal->ip, IP_MEMORIA, string_size(IP_MEMORIA));
	memoria_principal->puerto = malloc(string_size(PUERTO_MEMORIA));
	memcpy(memoria_principal->puerto, PUERTO_MEMORIA, string_size(PUERTO_MEMORIA));

//	memoria_principal->numero_memoria = (uint16_t)malloc(sizeof(int));
	memoria_principal->numero_memoria = (uint16_t)config_get_int_value(archivoconfig, "NUMERO_MEMORIA");

	memorias_disponibles = list_create();
	strong_consistency = list_create();
	strong_hash_consistency = list_create();
	eventual_consistency = list_create();
	tablas_con_consistencias = list_create();
	list_add(memorias_disponibles, memoria_principal);
	new_queue = queue_create();
	ready_queue = queue_create();
	exec_queue = queue_create();
	exit_queue = queue_create();

	metricas = list_create();

	sem_init(&new_queue_consumer, 1, 0);
	pthread_mutex_init(&memorias_disponibles_mutex,NULL);
	pthread_mutex_init(&strong_consistency_mutex,NULL);
	pthread_mutex_init(&strong_hash_consistency_mutex,NULL);
	pthread_mutex_init(&eventual_consistency_mutex,NULL);

	iniciar_hilo_planificacion();
	iniciar_hilo_metrics();
	iniciarHiloGossiping(memorias_disponibles);
	iniciar_hilo_inotify();

	while(1){
		char* linea = readline("Consola kernel>");

		parsear_y_ejecutar(linea, 1);

		free(linea);
	}

	pthread_mutex_destroy(&memorias_disponibles_mutex);
	pthread_mutex_destroy(&strong_consistency_mutex);
	pthread_mutex_destroy(&strong_hash_consistency_mutex);
	pthread_mutex_destroy(&eventual_consistency_mutex);

	close(socket_memoria);
	terminar_programa(socket_memoria); // termina conexion, destroy log y destroy config.
}

void iniciarHiloGossiping(t_list* memorias_disponibles){ // @suppress("Type cannot be resolved")
	pthread_t hiloGossiping;
	if (pthread_create(&hiloGossiping, 0, iniciar_peticion_tablas, memorias_disponibles) !=0){
			log_error(logger, "Error al crear el hilo de Gossiping");
		}
	if (pthread_detach(hiloGossiping) != 0){
			log_error(logger, "Error al crear el hilo de Gossiping");
		}
}

void* iniciar_peticion_tablas(void* memorias_disponibles){
	t_list* tablag = (t_list *) memorias_disponibles;
	while(1){
		sleep(retardo_gossiping);
		log_info(logger, "Inicio PEDIDO DE TABLAS A MEMORIA");
		t_operacion operacion = SOLICITUD_TABLA_GOSSIPING;

		int i=0;
		pthread_mutex_lock(&memorias_disponibles_mutex);
		int tamanio = list_size(memorias_disponibles);
		pthread_mutex_unlock(&memorias_disponibles_mutex);
		while (i < tamanio){
			pthread_mutex_lock(&memorias_disponibles_mutex);
			t_memoria* memoria_a_pedir = list_get(memorias_disponibles, i);
			pthread_mutex_unlock(&memorias_disponibles_mutex);

			int socket_memoria_a_pedir = crear_conexion(memoria_a_pedir->ip,memoria_a_pedir->puerto);

			if(socket_memoria_a_pedir > 0){
				send(socket_memoria_a_pedir,&operacion,sizeof(t_operacion),MSG_WAITALL);
				recibir_tabla_de_gossiping(socket_memoria_a_pedir);
				log_info(logger, "FIN PEDIDO DE TABLAS");
				liberar_conexion(socket_memoria_a_pedir);
				i = tamanio;
			} else {
				i++;
			}
		}
	}
}

int socket_memoria_principal(){
	pthread_mutex_lock(&memorias_disponibles_mutex);
	int tamanio = list_size(memorias_disponibles);
	pthread_mutex_unlock(&memorias_disponibles_mutex);
	int i = 0;
	while(i < tamanio){
		pthread_mutex_lock(&memorias_disponibles_mutex);
		t_memoria* memoria_a_pedir = list_get(memorias_disponibles, i);
		pthread_mutex_unlock(&memorias_disponibles_mutex);

		int socket_memoria_a_pedir = crear_conexion(memoria_a_pedir->ip,memoria_a_pedir->puerto);
		if(socket_memoria_a_pedir > 0){
			return socket_memoria_a_pedir;
		}
		i++;
	}
	return -1;
}

void eliminar_t_memoria(t_memoria* memoria){
	free(memoria->ip);
	free(memoria->puerto);
	free(memoria);
}

void recibir_tabla_de_gossiping(int socket){
	int numero_memorias;
	recv(socket,&numero_memorias,sizeof(int),MSG_WAITALL);
	log_info(logger, "Memorias de tabla: %i",numero_memorias);

	pthread_mutex_lock(&memorias_disponibles_mutex);
	destruir_elementos(memorias_disponibles);
	pthread_mutex_unlock(&memorias_disponibles_mutex);

	for(int i=0;i<numero_memorias;i++){
		t_memoria* memoria=malloc(sizeof(t_memoria));

		int tamanio_ip;
		recv(socket,&tamanio_ip,sizeof(int),MSG_WAITALL);
		memoria->ip=malloc(tamanio_ip);
		//log_info(logger, "Tamanio ip %i",tamanio_ip);
		recv(socket,memoria->ip,tamanio_ip,MSG_WAITALL);
		//log_info(logger, "IP: %s",memoria->ip_memoria);

		int tamanio_nombre;
		recv(socket,&tamanio_nombre,sizeof(int),MSG_WAITALL);
		//log_info(logger, "Tamanio nombre %i",tamanio_nombre);

		char* numero_memoria=malloc(tamanio_nombre);
		recv(socket,numero_memoria,tamanio_nombre,MSG_WAITALL);
		memoria->numero_memoria = convertir_string_a_int(numero_memoria);
		free(numero_memoria);

		int tamanio_puerto;
		recv(socket,&tamanio_puerto,sizeof(int),MSG_WAITALL);
		memoria->puerto=malloc(tamanio_puerto);
		//log_info(logger, "Tamanio puerto %i",tamanio_puerto);
		recv(socket,memoria->puerto,tamanio_puerto,MSG_WAITALL);
		log_info(logger, "IP: %s , PUERTO: %s , NOMBRE: %d",memoria->ip,memoria->puerto,memoria->numero_memoria);

		pthread_mutex_lock(&memorias_disponibles_mutex);
		list_add(memorias_disponibles,memoria);
		pthread_mutex_unlock(&memorias_disponibles_mutex);

	}
	cambiar_nodos_viejos_por_nuevos();
}

void destruir_elementos(t_list* lista){
	while(list_size(lista)){
		t_memoria* nodo_a_destruir = list_remove(lista, 0);
		eliminar_t_memoria(nodo_a_destruir);
	}
}

void cambiar_nodos_viejos_por_nuevos(){
	pthread_mutex_lock(&strong_consistency_mutex);
	revisar_y_cambiar_en(strong_consistency);
	pthread_mutex_unlock(&strong_consistency_mutex);

	pthread_mutex_lock(&strong_hash_consistency_mutex);
	revisar_y_cambiar_en(strong_hash_consistency);
	pthread_mutex_unlock(&strong_hash_consistency_mutex);

	pthread_mutex_lock(&eventual_consistency_mutex);
	revisar_y_cambiar_en(eventual_consistency);
	pthread_mutex_unlock(&eventual_consistency_mutex);
}

void revisar_y_cambiar_en(t_list* lista){
	for(int indice = 0; indice < list_size(lista) ; indice++){
		t_memoria* nodo_viejo = list_get(lista, indice);

		revisa_y_cambia_si_encuentra(nodo_viejo, lista, indice);
	}
}

void revisa_y_cambia_si_encuentra(t_memoria* nodo_viejo, t_list* lista, int indice){
	int es_la_memoria(t_memoria* memoria){
		return memoria->numero_memoria == nodo_viejo->numero_memoria;
	}

	pthread_mutex_lock(&memorias_disponibles_mutex);
	t_memoria* nuevo_nodo_memoria = list_find(memorias_disponibles, (void*) es_la_memoria);
	pthread_mutex_unlock(&memorias_disponibles_mutex);

	if(nuevo_nodo_memoria == NULL){
		t_memoria* memoria_a_liberar = list_remove(lista, indice);
		eliminar_t_memoria(memoria_a_liberar);
	}
}

void parsear_y_ejecutar(char* linea, int flag_de_consola){
	t_instruccion_lql instruccion = parsear_linea(linea);
	if (instruccion.valido) {
		ejecutar_instruccion(instruccion, NULL);
		//liberar_instruccion(instruccion);
	}else{
		if (flag_de_consola){
			log_error(log_consultas, "Reingrese correctamente la instruccion");
		}
	}
}

void parsear_y_ejecutar_script(char* linea, int flag_de_consola, t_script* script_a_ejecutar){
	t_instruccion_lql instruccion = parsear_linea(linea);
	if (instruccion.valido) {
		ejecutar_instruccion(instruccion, script_a_ejecutar);
	}else{
		if (flag_de_consola){
			log_error(logger, "Reingrese correctamente la instruccion");
		}
	}
}

void liberar_instruccion(t_instruccion_lql instruccion){
	int i=0;
	while(instruccion._raw[i]!=NULL){
		instruccion._raw[i] = realloc(instruccion._raw, 0);
		free(instruccion._raw[i]);
		i=i+1;
	}
	instruccion._raw = realloc(instruccion._raw, 0);
	free(instruccion._raw);
}

void liberar_instruccion_insert(t_instruccion_lql instruccion){

	int i=0;
	while(instruccion.parametros.INSERT.split_value[i]!=NULL){
		free(instruccion.parametros.INSERT.split_value[i]);
		i=i+1;
	}
	free(instruccion.parametros.INSERT.split_value);
	i=0;
	while(instruccion.parametros.INSERT.split_campos[i]!=NULL){
		free(instruccion.parametros.INSERT.split_campos[i]);
		i=i+1;
	}
	free(instruccion.parametros.INSERT.split_campos);
	liberar_instruccion(instruccion);
}


void ejecutar_instruccion(t_instruccion_lql instruccion, t_script* script_a_ejecutar){
	t_operacion operacion = instruccion.operacion;
	switch(operacion) {
		case SELECT:
			log_info(logger, "Se solicita SELECT a memoria");
			resolver_select(instruccion, script_a_ejecutar);
			//liberar_instruccion(instruccion);
			break;
		case INSERT:
			log_info(logger, "Kernel solicitó INSERT");
			resolver_insert(instruccion, script_a_ejecutar);
			//liberar_instruccion_insert(instruccion);
			break;
		case CREATE:
			log_info(logger, "Kernel solicitó CREATE");
			resolver_create(instruccion, script_a_ejecutar);

			break;
		case DESCRIBE:
			log_info(logger, "Kernel solicitó DESCRIBE");
			resolver_describe(instruccion, script_a_ejecutar);
			//liberar_instruccion(instruccion);
			break;
		case DROP:
			log_info(logger, "Kernel solicitó DROP");
			resolver_describe_drop(instruccion, DROP, script_a_ejecutar);
			//liberar_instruccion(instruccion);
			break;
		case RUN:
			log_info(logger, "Kernel solicitó RUN");
			resolver_run(instruccion);
			//liberar_instruccion(instruccion);
			break;
		case JOURNAL:
			log_info(logger, "Kernel solicitó JOURNAL");
			resolver_journal();
			break;
		case METRICS:
			log_info(logger, "Kernel solicitó METRICS");
			resolver_metrics();
			break;
		case ADD:
			log_info(logger, "Kernel solicitó ADD");
			resolver_add(instruccion);
			//liberar_instruccion(instruccion);
			break;
		default:
			log_warning(logger, "Operacion desconocida.");
			break;
		}
}

void resolver_describe_drop(t_instruccion_lql instruccion, t_operacion operacion, t_script* script_a_ejecutar){
	//separar entre describe y drop
	t_log* log;
	if(script_a_ejecutar == NULL){
		log = log_consultas;
	}else{
		log = logger;
	}

	t_paquete_drop_describe* paquete_describe = crear_paquete_drop_describe(instruccion);
	paquete_describe->codigo_operacion=operacion;

	char* nombre_tabla = paquete_describe->nombre_tabla;
	int socket_memoria_a_usar = socket_memoria_principal();//socket_memoria;//conseguir_memoria(nombre_tabla, -1);
	if(socket_memoria_a_usar == -1){
			log_error(log, "No se pudo realizar el DESCRIBE por falta de Memorias disponibles");
	}else{
		enviar_paquete_drop_describe(socket_memoria_a_usar, paquete_describe);

		t_status_solicitud* status = desearilizar_status_solicitud(socket_memoria_a_usar);
		if(status->es_valido){
			log_info(log, "Resultado: %s", status->mensaje->palabra);
		}else{
			log_error(log, "Error: %s", status->mensaje->palabra);
		}
		eliminar_paquete_drop_describe(paquete_describe);
	}
}

void resolver_describe(t_instruccion_lql instruccion, t_script* script_a_ejecutar){
	t_log* log;
	if(script_a_ejecutar == NULL){
		log = log_consultas;
	}else{
		log = logger;
	}

	t_paquete_drop_describe* paquete_describe = crear_paquete_drop_describe(instruccion);
	paquete_describe->codigo_operacion=DESCRIBE;

//	char* nombre_tabla = paquete_describe->nombre_tabla->palabra;
//	int socket_memoria_a_usar = conseguir_memoria(nombre_tabla);
	int socket_memoria_a_usar = socket_memoria_principal();
	if(socket_memoria_a_usar == -1){
		log_error(log, "No se pudo realizar el DESCRIBE por falta de Memorias disponibles");
	}else{
		enviar_paquete_drop_describe(socket_memoria_a_usar, paquete_describe);

		log_info(log, "Se realiza DESCRIBE");
		if(string_is_empty(paquete_describe->nombre_tabla->palabra)){
			log_info(log, "Se trata de un describe global.");
			int cant_tablas= recibir_numero_de_tablas (socket_memoria_a_usar);
			log_info(log, "Cantidad de tablas en LFS: %i", cant_tablas);
			for(int i=0; i<cant_tablas; i++){
				t_metadata* metadata = deserealizar_metadata(socket_memoria_a_usar);
				log_info(log, "Metadata tabla: %s", metadata->nombre_tabla->palabra);
				log_info(log, "Consistencia: %s", tipo_consistencia(metadata->consistencia));
				log_info(log, "Numero de particiones: %d", metadata->n_particiones);
				log_info(log, "Tiempo de compactacion: %d", metadata->tiempo_compactacion);

				guardar_consistencia_tabla(metadata->nombre_tabla->palabra, metadata->consistencia);
			}
		}else{
			t_status_solicitud* status = desearilizar_status_solicitud(socket_memoria_a_usar);
			if (status->es_valido){
				t_metadata* t_metadata = deserealizar_metadata(socket_memoria_a_usar);
				//aca se está mostrando pero deberia guardarselo no?
				log_info(log, "Metadata tabla: %s", t_metadata->nombre_tabla->palabra);
				log_info(log, "Consistencia: %s", tipo_consistencia(t_metadata->consistencia));
				log_info(log, "Numero de particiones: %d", t_metadata->n_particiones);
				log_info(log, "Tiempo de compactacion: %d", t_metadata->tiempo_compactacion);
			}else{
				log_info(log, "Error: %s", status->mensaje->palabra);
			}

		}
		eliminar_paquete_drop_describe(paquete_describe);
		liberar_conexion(socket_memoria_a_usar);
	}
}

void guardar_consistencia_tabla(char* nombre_tabla, t_consistencia consistencia){
	t_consistencia_tabla* consistencia_tabla = malloc(sizeof(t_consistencia_tabla));
	char* nombre_tabla_a_cargar = malloc(string_size(nombre_tabla));
	consistencia_tabla->nombre_tabla = malloc(string_size(nombre_tabla));
	memcpy(nombre_tabla_a_cargar, nombre_tabla, string_size(nombre_tabla));
	memcpy(consistencia_tabla->nombre_tabla, nombre_tabla_a_cargar, string_size(nombre_tabla));
	consistencia_tabla->consistencia = consistencia;

	t_consistencia_tabla* tabla = conseguir_tabla(nombre_tabla);
	if(tabla == NULL){
		list_add(tablas_con_consistencias, consistencia_tabla);
	}

	free(nombre_tabla_a_cargar);
}

t_consistencia_tabla* conseguir_tabla(char* nombre_tabla){
	int existe_tabla(t_consistencia_tabla* consistencia_tabla){
		return string_equals_ignore_case(consistencia_tabla->nombre_tabla, nombre_tabla);
	}

	return list_find(tablas_con_consistencias, (void*) existe_tabla);
}

void resolver_create(t_instruccion_lql instruccion, t_script* script_a_ejecutar){
	t_log* log;
	if(script_a_ejecutar == NULL){
		log = log_consultas;
	}else{
		log = logger;
	}

	t_paquete_create* paquete_create = crear_paquete_create(instruccion);

	t_memoria* memoria_a_usar = obtener_memoria_segun_consistencia(instruccion.parametros.CREATE.consistencia, 0);
	int socket_memoria_a_usar = crear_conexion(memoria_a_usar->ip, memoria_a_usar->puerto);

	enviar_paquete_create(socket_memoria_a_usar, paquete_create);
	t_status_solicitud* status = desearilizar_status_solicitud(socket_memoria_a_usar);
	if(status->es_valido){
		log_info(log, "Resultado: %s", status->mensaje->palabra);
	}else{
		log_error(log, "Error: %s", status->mensaje->palabra);
		if(script_a_ejecutar != NULL){
			script_a_ejecutar->error = 1;
		}
	}

	guardar_consistencia_tabla(instruccion.parametros.CREATE.tabla, instruccion.parametros.CREATE.consistencia);

	eliminar_paquete_status(status);
	eliminar_paquete_create(paquete_create);
}

void resolver_select(t_instruccion_lql instruccion, t_script* script_a_ejecutar){
	t_log* log;
	if(script_a_ejecutar == NULL){
		log = log_consultas;
	}else{
		log = logger;
	}

	t_paquete_select* paquete_select = crear_paquete_select(instruccion);

	char* nombre_tabla = paquete_select->nombre_tabla->palabra;
	t_memoria* memoria_a_usar = conseguir_memoria(nombre_tabla, paquete_select->key);

	if(memoria_a_usar == -1){
		log_error(log, "No se pudo realizar el SELECT ya que no se ha encontrado la tabla.");
	}else if(memoria_a_usar == -2){
		log_error(log, "No se pudo realizar el SELECT ya que no se ha encontrado Memoria");
	}else{
		int socket_memoria_a_usar = crear_conexion(memoria_a_usar->ip, memoria_a_usar->puerto);
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		int timestamp_origen = spec.tv_sec;

		enviar_paquete_select(socket_memoria_a_usar, paquete_select);

		t_status_solicitud* status = desearilizar_status_solicitud(socket_memoria_a_usar);
		if(status->es_valido){
			log_info(log, "Resultado: %s", status->mensaje->palabra);
		}else{
			log_error(log, "Error: %s", status->mensaje->palabra);
			if (strcmp(status->mensaje->palabra, "Memoria full" )==0){
				t_operacion operacion_a_enviar = JOURNAL;
				send(socket_memoria_a_usar, &operacion_a_enviar, sizeof(t_operacion), MSG_WAITALL);
				log_info(log, "Se ha realizado el JOURNAL a la memoria con socket: %d", socket_memoria_a_usar);
				enviar_paquete_select(socket_memoria_a_usar, paquete_select);
				log_info(log, "Se ha realizado el SELECT nuevamente a la memoria con socket: %d", socket_memoria_a_usar);
				t_status_solicitud* status2 = desearilizar_status_solicitud(socket_memoria_a_usar);
				if(status->es_valido){
					log_info(log, "Resultado: %s", status2->mensaje->palabra);
				}else{
					log_error(log, "Error: %s", status2->mensaje->palabra);
				}
				eliminar_paquete_status(status2);

			}
		}
		clock_gettime(CLOCK_REALTIME, &spec);
		int timestamp_destino = spec.tv_sec;
		int diferencia_timestamp = timestamp_destino - timestamp_origen;

		registrar_metricas(memoria_a_usar->numero_memoria, diferencia_timestamp); //Ahora es 1 porque es la única memoria que conocemos, pero cambia a nombre_memoria

		liberar_conexion(socket_memoria_a_usar);
		eliminar_paquete_status(status);
		eliminar_paquete_select(paquete_select);
	}
}

void resolver_insert (t_instruccion_lql instruccion, t_script* script_a_ejecutar){
	t_log* log;
	if(script_a_ejecutar == NULL){
		log = log_consultas;
	}else{
		log = logger;
	}

	t_paquete_insert* paquete_insert = crear_paquete_insert(instruccion);

	char* nombre_tabla = paquete_insert->nombre_tabla->palabra;
	t_memoria* memoria_a_usar = conseguir_memoria(nombre_tabla, paquete_insert->key);

	if(memoria_a_usar == -1){
		log_error(log, "No se pudo realizar el INSERT ya que no se ha encontrado la tabla.");
		if(script_a_ejecutar != NULL){
			script_a_ejecutar->error = 1;
		}
	}else if(memoria_a_usar == -2){
		log_error(log, "No se pudo realizar el INSERT ya que no se ha encontrado Memoria");
		if(script_a_ejecutar != NULL){
			script_a_ejecutar->error = 1;
		}
	}else{
		int socket_memoria_a_usar = crear_conexion(memoria_a_usar->ip, memoria_a_usar->puerto);
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		int timestamp_origen = spec.tv_sec;

		enviar_paquete_insert(socket_memoria_a_usar, paquete_insert);

		bool respuesta;
		recv(socket_memoria_a_usar,&respuesta,sizeof(bool),MSG_WAITALL);
		if(respuesta) {
			log_info(log, "Insert OK");
		}else{
			int largo;
			recv(socket_memoria_a_usar,&largo,sizeof(int),MSG_WAITALL);
			char* mensaje = malloc(largo);
			recv(socket_memoria_a_usar,mensaje,largo,MSG_WAITALL);
			log_error(log, "%s",mensaje);
			if (strcmp(mensaje, "Memoria full" )==0){
				t_operacion operacion_a_enviar = JOURNAL;
				send(socket_memoria_a_usar, &operacion_a_enviar, sizeof(t_operacion), MSG_WAITALL);
				log_info(log, "Se ha realizado el JOURNAL a la memoria con socket: %d", socket_memoria_a_usar);
				enviar_paquete_insert(socket_memoria_a_usar, paquete_insert);
				log_info(log, "Se ha realizado el SELECT nuevamente a la memoria con socket: %d", socket_memoria_a_usar);
				bool respuesta;
				recv(socket_memoria_a_usar,&respuesta,sizeof(bool),MSG_WAITALL);
				if(respuesta) {
					log_info(log, "Insert OK");
				} else {
					int largo;
					recv(socket_memoria_a_usar,&largo,sizeof(int),MSG_WAITALL);
					char* mensaje = malloc(largo);
					recv(socket_memoria_a_usar,mensaje,largo,MSG_WAITALL);
					log_error(log, "%s",mensaje);
				}
			}else{
				if(script_a_ejecutar != NULL){
					script_a_ejecutar->error = 1;
				}
			}

			free(mensaje);

		}

		clock_gettime(CLOCK_REALTIME, &spec);
		int timestamp_destino = spec.tv_sec;
		int diferencia_timestamp = timestamp_destino - timestamp_origen;

		registrar_metricas_insert(memoria_a_usar->numero_memoria, diferencia_timestamp);
		liberar_conexion(socket_memoria_a_usar);
		eliminar_paquete_insert(paquete_insert);
	}
}



void ejecutar_script(t_script* script_a_ejecutar){
	char* path = script_a_ejecutar->path;
	FILE* archivo = fopen(path,"r");

		fseek(archivo, script_a_ejecutar->offset, SEEK_SET);

		char ultimo_caracter_leido = leer_archivo(archivo, script_a_ejecutar);

		if(ultimo_caracter_leido != EOF && script_a_ejecutar->error == 0){
			script_a_ejecutar->offset = ftell(archivo) - 1;
		} else {
			script_a_ejecutar->offset = NULL;
		}

		fclose(archivo);
}



void resolver_run(t_instruccion_lql instruccion){
	char* path = instruccion.parametros.RUN.path_script;
	FILE* archivo = fopen(path,"r");
	if(archivo == NULL) {
		log_error(log_consultas, "La ruta es incorrecta");
	} else {
		fclose(archivo);
		queue_push(new_queue,path);
		log_info(log_plani, "El archivo %s pasa a estado NEW", path);
		sem_post(&new_queue_consumer);
	}
}


void resolver_add (t_instruccion_lql instruccion){
	uint16_t numero_memoria = instruccion.parametros.ADD.numero_memoria;
	t_consistencia consistencia = instruccion.parametros.ADD.consistencia;

	int es_la_memoria(t_memoria* memoria){
		return memoria->numero_memoria == numero_memoria;
	}

	pthread_mutex_lock(&memorias_disponibles_mutex);
	t_memoria* memoria = list_find(memorias_disponibles, (void*) es_la_memoria);
	pthread_mutex_unlock(&memorias_disponibles_mutex);

	char* consistencia_deseada = tipo_consistencia(consistencia);
	if(memoria != NULL){

		pthread_mutex_lock(&strong_consistency_mutex);
		if(consistencia == STRONG && list_size(strong_consistency) == 1) {
			list_remove(strong_consistency, 0);
		}
		pthread_mutex_unlock(&strong_consistency_mutex);

		t_memoria* nuevo_nodo_memoria = crear_nuevo_nodo_memoria(memoria);
		int asignado = asignar_consistencia(nuevo_nodo_memoria, consistencia);
		if(asignado == 1){
			log_info(log_consultas, "Se ha añadido la Memoria: %d a la Consistencia: %s", numero_memoria, consistencia_deseada);
		}else if(asignado == 0){
			log_error(log_consultas, "La Memoria: %d ya se encontraba en la Consistencia: %s", numero_memoria, consistencia_deseada);
		}else{
			log_error(log_consultas, "Ingrese correctamente el tipo de Consistencia");
		}
	}else{
		log_error(log_consultas, "No se ha podido añadir la Memoria: %d a la Consistencia: %s", numero_memoria, consistencia_deseada);
	}
}

t_memoria* crear_nuevo_nodo_memoria(t_memoria* memoria){
	t_memoria* nuevo_nodo = malloc(sizeof(t_memoria));
	nuevo_nodo->ip = malloc(string_size(memoria->ip));
	nuevo_nodo->puerto = malloc(string_size(memoria->puerto));

	nuevo_nodo->numero_memoria = memoria->numero_memoria;
	strcpy(nuevo_nodo->ip, memoria->ip);
	strcpy(nuevo_nodo->puerto, memoria->puerto);

	return nuevo_nodo;
}
void resolver_metrics(){
	t_metrics* metrica_uno;

	if(list_size(metricas) == 0){
		log_info(log_consultas, "Aun no hay Métricas para informar");
	}else{
		for(int i = 0; i < list_size(metricas); i++){
			metrica_uno = list_get(metricas, i);
			float memory_load = ((metrica_uno->cant_reads + metrica_uno->cant_writes) / CANT_METRICS);

			log_info(log_consultas, "En la memoria %d el tiempo promedio de un SELECT fue de %d desde el ultimo informe", metrica_uno->nombre_memoria, metrica_uno->read_latency);
			log_info(log_consultas, "En la memoria %d el tiempo promedio de un INSERT fue de %d desde el ultimo informe", metrica_uno->nombre_memoria, metrica_uno->write_latency);
			log_info(log_consultas, "En la memoria %d la cantidad de SELECT fue de %d desde el ultimo informe", metrica_uno->nombre_memoria, metrica_uno->cant_reads);
			log_info(log_consultas, "En la memoria %d la cantidad de INSERT fue de %d desde el ultimo informe", metrica_uno->nombre_memoria, metrica_uno->cant_writes);
			log_info(log_consultas, "En la memoria %d el MEMORY LOAD fue de %f", metrica_uno->nombre_memoria, memory_load);
		}
	}
}

void resolver_journal(){
	pthread_mutex_lock(&memorias_disponibles_mutex);
	for(int i=0; i<list_size(memorias_disponibles); i++){
		t_memoria* memoria = list_get(memorias_disponibles, i);
		int socket_conexion = crear_conexion(memoria->ip, memoria->puerto);
		t_operacion operacion_a_enviar = JOURNAL;
		send(socket_conexion, &operacion_a_enviar, sizeof(t_operacion), MSG_WAITALL);

		liberar_conexion(socket_conexion);
		log_info(log_consultas, "Se ha realizado el JOURNAL a la memoria: %d", memoria->numero_memoria);
	}
	pthread_mutex_unlock(&memorias_disponibles_mutex);
	log_info(log_consultas, "Se ha realizado el JOURNAL a todas las memorias");
}

int asignar_consistencia(t_memoria* memoria, t_consistencia consistencia){
	uint16_t numero_memoria = memoria->numero_memoria;
	int es_la_memoria(t_memoria* memoria){
		return memoria->numero_memoria == numero_memoria;
	}
	int agregada = 0;
	switch(consistencia){
		case STRONG:
			pthread_mutex_lock(&strong_consistency_mutex);
			list_add(strong_consistency, memoria);
			pthread_mutex_unlock(&strong_consistency_mutex);
			return 1;
		case STRONG_HASH:
			pthread_mutex_lock(&strong_hash_consistency_mutex);
			if(list_find(strong_hash_consistency, (void*) es_la_memoria) == NULL){
				list_add(strong_hash_consistency, memoria);
				agregada = 1;
			}
			pthread_mutex_unlock(&strong_hash_consistency_mutex);
			if(agregada){
				resolver_journal_hash();
			}

			return agregada;
		case EVENTUAL:
			pthread_mutex_lock(&eventual_consistency_mutex);
			if(list_find(eventual_consistency, (void*) es_la_memoria) == NULL){
				list_add(eventual_consistency, memoria);
				agregada = 1;
			}
			pthread_mutex_unlock(&eventual_consistency_mutex);
			return agregada;
		default:
			return -1;
	}
}

void resolver_journal_hash(){
	for(int i=0; i<list_size(strong_hash_consistency); i++){
		pthread_mutex_lock(&strong_hash_consistency_mutex);
		t_memoria* memoria = list_get(strong_hash_consistency, i);
		pthread_mutex_unlock(&strong_hash_consistency_mutex);

		int socket_conexion = crear_conexion(memoria->ip, memoria->puerto);
		t_operacion operacion_a_enviar = JOURNAL;
		send(socket_conexion, &operacion_a_enviar, sizeof(t_operacion), MSG_WAITALL);
		liberar_conexion(socket_conexion);
		log_info(logger, "Se ha realizado el JOURNAL a la memoria: %d", memoria->numero_memoria);
	}

	log_info(logger, "Se ha realizado el JOURNAL a todas las memorias del criterio STRONG HASH");
}

char* tipo_consistencia(t_consistencia consistencia){
	switch(consistencia){
		case STRONG:
			return "STRONG";
		case STRONG_HASH:
			return "STRONG HASH";
		case EVENTUAL:
		default:
			return "EVENTUAL";
	}
}

t_memoria* conseguir_memoria(char *nombre_tabla, uint16_t key){
	t_memoria* memoria;
	t_consistencia_tabla* tabla_en_uso;

	tabla_en_uso = conseguir_tabla(nombre_tabla);
	if(tabla_en_uso == NULL){
		log_error(logger, "Si no me haces un describe no tengo idea de que me decis");
		return -1;
	}else{
		memoria = obtener_memoria_segun_consistencia(tabla_en_uso->consistencia, key);
		if (memoria == NULL){
			log_error(logger, "Si no agregas la memoria a la consistencia no te puedo ejecutar nada, crack");
			return -2;
		} else {
			return memoria;
		}
	}

}

t_memoria* obtener_memoria_segun_consistencia(t_consistencia consistencia, uint16_t key){
	int maximo_indice;
	int indice_random;
	int indice_hash;
	t_memoria* memoria_segun_consistencia;
	switch(consistencia){
		case STRONG:
			pthread_mutex_lock(&strong_consistency_mutex);
			memoria_segun_consistencia = list_get(strong_consistency, 0);
			pthread_mutex_unlock(&strong_consistency_mutex);

			return memoria_segun_consistencia;
		case STRONG_HASH:
			pthread_mutex_lock(&strong_hash_consistency_mutex);
			indice_hash = funcion_hash_magica(key);
			memoria_segun_consistencia = list_get(strong_hash_consistency, indice_hash);
			pthread_mutex_unlock(&strong_hash_consistency_mutex);

			return memoria_segun_consistencia;
		case EVENTUAL:
		default:
			pthread_mutex_lock(&eventual_consistency_mutex);
			maximo_indice = list_size(eventual_consistency);
			if(maximo_indice == 0){
				pthread_mutex_unlock(&eventual_consistency_mutex);
				return NULL;
			}else if(maximo_indice == 1){
				memoria_segun_consistencia = list_get(eventual_consistency, 0);
				pthread_mutex_unlock(&eventual_consistency_mutex);

				return memoria_segun_consistencia;
			}else{
				indice_random = get_random(maximo_indice);
				memoria_segun_consistencia =list_get(eventual_consistency, indice_random);
				pthread_mutex_unlock(&eventual_consistency_mutex);

				return memoria_segun_consistencia;
			}
	}
}

int funcion_hash_magica(uint16_t ki){
	int tamanio= 0, indice = ki;
	tamanio = list_size(strong_hash_consistency);

	while(indice >= tamanio ){
		indice = indice - tamanio;
	}

	return indice;

}

int get_random(int maximo){
	int tamanio = maximo;

	for(int i = 0; i < tamanio; i++){
		if(contador!=tamanio-1){
			contador++;
		} else {
			contador = 0;
		}
		return contador;
	}
}

char leer_archivo(FILE* archivo, t_script* script_en_ejecucion){
	char* linea = NULL;
	int lineas_leidas = 0;
	int i;
	char letra;
	while((letra = fgetc(archivo)) != EOF && lineas_leidas < QUANTUM && script_en_ejecucion->error != 1){
		linea = (char*)realloc(NULL, sizeof(char));
		i = 0;
		do{
			linea = (char*)realloc(linea, (i+1));
			linea[i] = letra;
			i++;
		}while((letra = fgetc(archivo)) != '\n' && letra != EOF);

		linea = (char*)realloc(linea, (i+1));
		linea[i] = 0;
		parsear_y_ejecutar_script(linea, 0, script_en_ejecucion);
		free(linea);
		lineas_leidas++;
		linea = NULL;
	}
	return letra;
}

void iniciar_logger() { 							// CREACION DE LOG
	logger = crear_log("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/Kernel/kernel.log", 0);
	log_consultas = crear_log("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/Kernel/kernel_Consultas.log", 1);
}

t_log* crear_log(char* path, int debe_aparecer_en_consola){
	return log_create(path, "kernel", debe_aparecer_en_consola, LOG_LEVEL_INFO);
}

void leer_config() {								// APERTURA DE CONFIG
	archivoconfig = config_create(ARCHIVO_CONFIG);
}

void leer_atributos_config(){
	QUANTUM = config_get_int_value(archivoconfig, "QUANTUM");
	CANT_EXEC = config_get_int_value(archivoconfig, "NIVEL_MULTIPROCESAMIENTO");
	SLEEP_EJECUCION = config_get_int_value(archivoconfig, "SLEEP_EJECUCION") / 1000;
	retardo_gossiping = config_get_int_value(archivoconfig, "RETARDO_GOSSIPING") / 1000;
}

int generarID(){
	ID++;
	return ID;
}

uint16_t convertir_string_a_int(char* string){
	uint16_t convertido = (uint16_t)atol(string);
	return convertido;
}

void terminar_programa(int conexion)
{
	liberar_conexion(conexion);
	log_destroy(logger);
	config_destroy(archivoconfig);
	//Y por ultimo, para cerrar, hay que liberar lo que utilizamos (conexion, log y config) con las funciones de las commons y del TP mencionadas en el enunciado
}

void iniciar_hilo_inotify(){
	pthread_t hilo_inotify;
	if (pthread_create(&hilo_inotify, 0, iniciar_inotify, NULL) !=0){
			log_error(logger, "Error al crear el hilo de Inotify");
		}
	if (pthread_detach(hilo_inotify) != 0){
			log_error(logger, "Error al crear el hilo de Inotify");
		}
}
void iniciar_inotify(){
	#define MAX_EVENTS 1024 /*Max. number of events to process at one go*/
	#define LEN_NAME 64 /*Assuming that the length of the filename won't exceed 16 bytes*/
	#define EVENT_SIZE  ( sizeof (struct inotify_event) ) /*size of one event*/
	#define BUF_LEN     ( MAX_EVENTS * ( EVENT_SIZE + LEN_NAME )) /*buffer to store the data of events*/

	char* path = ARCHIVO_CONFIG;
	int length, i = 0, wd;
	int fd;
	char buffer[BUF_LEN];

	fd = inotify_init();
	if( fd < 0 ){
		log_error(logger, "No se pudo inicializar Inotify");
	}

	/* add watch to starting directory */
	wd = inotify_add_watch(fd, path, IN_MODIFY);

	if(wd == -1){
		log_error(logger, "No se pudo agregar una observador para el archivo: %s\n",path);
	}else{
		log_info(logger, "Inotify esta observando modificaciones al archivo: %s\n", path);
	}

	while(1){
		length = read( fd, buffer, BUF_LEN );
		if(length < 0){
			perror("read");
		}else{
			struct inotify_event *event = ( struct inotify_event * ) buffer;
			if(event->mask == IN_MODIFY){
				log_info(logger, "Se modifico el archivo de configuración");
				leer_config();
				leer_atributos_config();

				log_info(logger, "El Quantum es de %d", QUANTUM);
			}
		}
	}
	inotify_rm_watch( fd, wd );
	close( fd );
}
