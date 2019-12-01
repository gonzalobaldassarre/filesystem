/*
 * LFS.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "LFS.h"
int main(void)
{
	char *montaje;
	iniciar_loggers();
	leer_config(); // abrimos config
	ip_lfs = config_get_string_value(archivoconfig, "IP_LFS"); // asignamos IP de memoria a conectar desde CONFIG
	log_info(logger, "La IP del LFS es %s",ip_lfs );
	puerto_lfs = config_get_string_value(archivoconfig, "PUERTO_LFS"); // asignamos puerto desde CONFIG
	log_info(logger, "El puerto del LFS es %s",puerto_lfs);
	montaje = config_get_string_value(archivoconfig, "PUNTO_MONTAJE");
	max_size_value = config_get_int_value(archivoconfig, "MAX_SIZE_VALUE");
	leer_tiempo_dump_y_retardo_del_config();

	log_info(logger, "La IP de la memoria es %s",ip_lfs );
	log_info(logger, "El puerto de la memoria es %s",puerto_lfs);


	iniciarMutexBitmap();
	iniciarMutexMemtable();
	iniciarMutexDump();
	iniciarMutexTablasLFS();
	//iniciarMutexInstBloqueadas();
	//iniciarMutexTemp();
	levantar_lfs(montaje);
	crear_hilo_server();
	crear_hilo_consola();
	crear_hilo_dump();
	crear_hilo_inotify();

	void *ret;
	if (pthread_join(hilo_consola, &ret) != 0){
		log_error(logger_consola, "Error al frenar hilo");
	}
	log_info(logger_consola,"Desconectando..");
	desconectar_lfs();
	return EXIT_SUCCESS;
}
void levantar_lfs(char* montaje){

	log_info(logger, "Inicia filesystem");
	path_montaje = malloc(string_size(montaje));
	memcpy(path_montaje, montaje,string_size(montaje) );
	fin_de_programa=false;
	memtable = list_create();
	hilos_memorias=list_create();
	instrucciones_bloqueadas_por_tabla = dictionary_create();
	obtener_info_metadata();
	obtener_bitmap();
	obtener_tablas_en_lfs();

}

void desconectar_lfs(){

	desconectar_clientes();
	log_info(logger, "Valor sem en hilo principal: %d", mutexDump);
	pthread_mutex_lock(&mutexDump);
	log_info(logger,"Comenzando dump forzado");
	pthread_cancel(hilo_dump);
	dump_proceso();
	log_info(logger_dump,"Finaliza LFS. Finalizado Dump");
	eliminar_tablas_logicas();
	pthread_mutex_lock(&mutexMemtable);
	list_destroy_and_destroy_elements(memtable, eliminar_tabla);
	pthread_mutex_unlock(&mutexMemtable);
	pthread_mutex_lock(&mutexBitmap);
	munmap(bmap, size_bitmap.st_size);
	bitarray_destroy(bitarray);
	pthread_mutex_unlock(&mutexBitmap);
	dictionary_destroy(instrucciones_bloqueadas_por_tabla);
	config_destroy(archivoconfig);
	log_destroy(logger_dump);
	log_destroy(logger_compactacion);
	log_info(logger,"Fin LFS.");
	log_destroy(logger);
	log_info(logger_consola,"Fin LFS.");
	log_destroy(logger_consola);
	pthread_cancel(hilo_inotify);
	free(path_montaje);
}

void eliminar_tablas_logicas(){
	for(int i = 0; i< list_size(tablas_en_lfs); i++){
		t_tabla_logica* tabla_logica = list_get(tablas_en_lfs, i);
		pthread_mutex_lock(&(tabla_logica->mutex_compactacion));
		pthread_cancel(tabla_logica->id_hilo_compactacion);
		log_info(logger_compactacion,"Finaliza LFS. Finalizado Compactacion para tabla %s", tabla_logica->nombre);
		pthread_mutex_unlock(&(tabla_logica->mutex_compactacion));
	}
	pthread_mutex_lock(&mutexTablasLFS);
	list_destroy_and_destroy_elements(tablas_en_lfs, (void*)liberar_tabla_logica);
	pthread_mutex_unlock(&mutexTablasLFS);
}

void liberar_tabla_logica(t_tabla_logica* tabla){
	free(tabla->nombre);
	free(tabla);
}

void desconectar_clientes(){
	for (int i=0 ; i<list_size(hilos_memorias); i++){
		pthread_t hilo_memoria = list_get(hilos_memorias, i);
		pthread_cancel(hilo_memoria);
	}
	list_destroy(hilos_memorias);
	log_info(logger,"Finaliza Hilo Server");
	pthread_cancel(hilo_server);
}

void iniciarMutexBitmap(){
	if(pthread_mutex_init(&mutexBitmap,NULL)==0){
		log_info(logger, "mutexBitmap inicializado correctamente");
	} else {
		log_error(logger, "Fallo inicializacion de mutexBitmap");
	};
}

void iniciarMutexTemp(){
	if(pthread_mutex_init(&mutex_temp,NULL)==0){
		log_info(logger, "mutexBitmap inicializado correctamente");
	} else {
		log_error(logger, "Fallo inicializacion de mutexBitmap");
	};
}

void iniciarMutexMemtable(){
	if(pthread_mutex_init(&mutexMemtable,NULL)==0){
		log_info(logger, "MutexMemtable inicializado correctamente");
	} else {
		log_error(logger, "Fallo inicializacion de MutexMemtable");
	};
}

void iniciarMutexDump(){
	if(pthread_mutex_init(&mutexDump,NULL)==0){
		log_info(logger, "MutexDump inicializado correctamente");

	} else {
		log_error(logger, "Fallo inicializacion de MutexDump");
	};
}

void iniciarMutexHilosMemoria(){

	if(pthread_mutex_init(&mutexHilosMemoria,NULL)==0){
			log_info(logger, "MutexHilosMemoria inicializado correctamente");

		} else {
			log_error(logger, "Fallo inicializacion de MutexHilosMemoria");
		};
}

void iniciarMutexInstBloqueadas(){
	if(pthread_mutex_init(&mutexInstBloqueadas,NULL)==0){
			log_info(logger, "MutexInstBloqueadas inicializado correctamente");

		} else {
			log_error(logger, "Fallo inicializacion de MutexInstBloqueadas");
		};

}

void iniciarMutexTablasLFS(){
	if(pthread_mutex_init(&mutexTablasLFS,NULL)==0){
				log_info(logger, "mutexTablasLFS inicializado correctamente");

			} else {
				log_error(logger, "Fallo inicializacion de mutexTablasLFS");
			};

	}



void *atender_pedido_memoria (void* memoria_fd){

	int socket_memoria = *((int *) memoria_fd);
	pthread_t id_hilo = pthread_self();
	bool _es_hilo_memoria(pthread_t hilo) {
		return hilo==id_hilo;
	}
	while(1){
		int cod_op = recibir_operacion(socket_memoria);
		if (resolver_operacion(socket_memoria, cod_op)!=0){
			pthread_mutex_lock(&mutexHilosMemoria);
			list_remove_by_condition(hilos_memorias,_es_hilo_memoria);
			pthread_mutex_unlock(&mutexHilosMemoria);
			pthread_cancel(id_hilo);
			free(memoria_fd);

		}
	}
	    //free(i);
}

void crear_hilo_memoria(int socket_memoria){
	pthread_t hilo_memoria;
	int *memoria_fd = malloc(sizeof(*memoria_fd));
	*memoria_fd = socket_memoria;

	if (pthread_create(&hilo_memoria, 0, atender_pedido_memoria, memoria_fd) !=0){
		log_error(logger, "Error al crear el hilo");
	}
	if (pthread_detach(hilo_memoria) != 0){
		log_error(logger, "Error al frenar hilo");
	}
	pthread_mutex_lock(&mutexHilosMemoria);
	list_add(hilos_memorias, hilo_memoria);
	pthread_mutex_unlock(&mutexHilosMemoria);
}

void agregar_cliente(fd_set* master, int cliente, int* fdmax){
	FD_SET(cliente, master);//se agrega nuevo fd al set
	log_info(logger, "LFS se conectó con memoria");
	if (cliente > *fdmax)//actualizar el máximo
		*fdmax = cliente;
	char *mensaje_handshake = "Conexion aceptada de LFS";
	if (enviar_handshake(cliente,mensaje_handshake)!=-1){
		log_info(logger, "Se envió el mensaje %s", mensaje_handshake);
		recibir_handshake(logger, cliente);
		log_info(logger,"Conexion exitosa con Memoria");
	}else{
		log_error(logger, "No pudo realizarse handshake con el nuevo cliente en el socket %d", cliente);
	}

}

int resolver_operacion(int socket_memoria, t_operacion cod_op){
	t_status_solicitud* status;
	switch((int)cod_op)
		{
		case HANDSHAKE:
			log_info(logger, "Inicia handshake con memoria");
			recibir_mensaje(logger, socket_memoria);
			char*max_size=string_itoa(max_size_value);
			enviar_handshake(socket_memoria, max_size);
			//TODO: ver
			free(max_size);
			log_info(logger, "Conexion exitosa con memoria");
			break;
		case SELECT:
			log_info(logger, "memoria solicitó SELECT");
			t_paquete_select* select = deserializar_select(socket_memoria);

//			if (tabla_esta_bloqueada(select->nombre_tabla->palabra)){
//				log_info(logger, "La tabla %s esta bloqueada. Creo una nueva instruccion bloqueada select para ejecutar dsp", select->nombre_tabla->palabra);
//				agregar_instruccion_bloqueada(crear_instruccion_select_bloqueada(select, socket_memoria), select->nombre_tabla->palabra);
//			}else{
				//log_info(logger, "La tabla %s no esta bloqueada. Se procede a ejecutar la consulta select", select->nombre_tabla->palabra);
			status = resolver_select(select->nombre_tabla->palabra, select->key);
			enviar_status_resultado(status, socket_memoria);
			log_info(logger, "Se envió resultado SELECT a memoria %d", socket_memoria);
			//}
			eliminar_paquete_select(select);
			break;
		case INSERT:
			log_info(logger, "memoria solicitó INSERT");
			t_paquete_insert* consulta_insert = deserealizar_insert(socket_memoria);
//			if (tabla_esta_bloqueada(consulta_insert->nombre_tabla->palabra)){
//				log_info(logger, "La tabla %s esta bloqueada. Creo una nueva instruccion bloqueada insert para ejecutar dsp", consulta_insert->nombre_tabla->palabra);
//				agregar_instruccion_bloqueada(crear_instruccion_insert_bloqueada(consulta_insert, socket_memoria), consulta_insert->nombre_tabla->palabra);
//			}else{
				//log_info(logger, "La tabla %s no esta bloqueada. Se procede a ejecutar la consulta INSERT", consulta_insert->nombre_tabla->palabra);
				status = resolver_insert(logger, consulta_insert->nombre_tabla->palabra, consulta_insert->key, consulta_insert->valor->palabra, consulta_insert->timestamp);
				enviar_status_resultado(status, socket_memoria);
				log_info(logger, "Se envió status a memoria %d", socket_memoria);
			//}
			eliminar_paquete_insert(consulta_insert);
			break;
		case CREATE:
			log_info(logger, "memoria solicitó CREATE");
			t_paquete_create* create = deserializar_create (socket_memoria);
			status = resolver_create(logger, create->nombre_tabla->palabra, create->consistencia, create->num_particiones, create->tiempo_compac);
			enviar_status_resultado(status, socket_memoria);
			eliminar_paquete_create(create);
			//aca debería enviarse el mensaje a LFS con CREATE
			break;
		case DESCRIBE:
			log_info(logger, "memoria solicitó DESCRIBE");
			t_paquete_drop_describe* consulta_describe = deserealizar_drop_describe(socket_memoria);
			if (validar_datos_describe(consulta_describe->nombre_tabla->palabra, socket_memoria)){
				resolver_describe(consulta_describe->nombre_tabla->palabra, socket_memoria);
			}
			eliminar_paquete_drop_describe(consulta_describe);
			//aca debería enviarse el mensaje a LFS con DESCRIBE
			break;
		case DROP:
			log_info(logger, "memoria solicitó DROP");
			t_paquete_drop_describe* consulta_drop = deserealizar_drop_describe(socket_memoria);
			status = resolver_drop(logger, consulta_drop->nombre_tabla->palabra);
			enviar_status_resultado(status, socket_memoria);
			eliminar_paquete_drop_describe(consulta_drop);
			//aca debería enviarse el mensaje a LFS con DROP
			break;
		case -1:
			log_error(logger, "el cliente se desconecto.");
			return EXIT_FAILURE;

		default:
			log_warning(logger, "Operacion desconocida.");
			return EXIT_FAILURE;
		}
	return EXIT_SUCCESS;
}

t_instruccion_bloqueada* crear_instruccion_select_bloqueada(t_paquete_select* select, int socket_memoria){

	t_instruccion_lql* ret = malloc(sizeof(t_instruccion_lql));
	ret->valido = true;
	ret->operacion=SELECT;
	ret->parametros.SELECT.key = select->key;
	ret->parametros.SELECT.tabla=malloc(string_size( select->nombre_tabla->palabra));
	memcpy(ret->parametros.SELECT.tabla, select->nombre_tabla->palabra,string_size( select->nombre_tabla->palabra));

	t_instruccion_bloqueada* bloqueada = malloc(sizeof(t_instruccion_bloqueada));

	bloqueada->instruccion = ret;
	bloqueada->socket_memoria=socket_memoria;
	return bloqueada;
}

t_instruccion_bloqueada* crear_instruccion_insert_bloqueada(t_paquete_insert* insert, int socket_memoria){
	t_instruccion_bloqueada* bloqueada = malloc(sizeof(t_instruccion_bloqueada));
	t_instruccion_lql* ret = malloc(sizeof(t_instruccion_lql));
	ret->valido = true;
	ret->operacion=INSERT;
	ret->parametros.INSERT.key = insert->key;
	ret->parametros.INSERT.tabla=malloc(string_size(insert->nombre_tabla->palabra));
	ret->parametros.INSERT.timestamp=insert->timestamp;
	ret->parametros.INSERT.value = malloc(string_size(insert->valor->palabra));
	memcpy(ret->parametros.INSERT.value,insert->valor->palabra,string_size(insert->valor->palabra));
	memcpy(ret->parametros.INSERT.tabla,insert->nombre_tabla->palabra,string_size(insert->nombre_tabla->palabra));

	bloqueada->instruccion = ret;
	bloqueada->socket_memoria=socket_memoria;
	return bloqueada;
}

void agregar_instruccion_bloqueada(t_instruccion_bloqueada* instruccion_bloqueada, char* tabla){
	t_queue* instrucciones_bloqueadas ;
	pthread_mutex_lock(&mutexInstBloqueadas);
	if( dictionary_has_key(instrucciones_bloqueadas_por_tabla, tabla)){
		instrucciones_bloqueadas = dictionary_get(instrucciones_bloqueadas_por_tabla, tabla);

	}else{
		instrucciones_bloqueadas = queue_create();
		char* nombre_tabla = malloc(string_size(tabla));
		memcpy(nombre_tabla, tabla, string_size(tabla));
		dictionary_put(instrucciones_bloqueadas_por_tabla, nombre_tabla,  instrucciones_bloqueadas);
	}
	pthread_mutex_unlock(&mutexInstBloqueadas);
	queue_push(instrucciones_bloqueadas, instruccion_bloqueada);
}

t_tabla_logica* buscar_tabla_logica_con_nombre(char*nombre_tabla){
	bool _es_tabla_con_nombre(t_tabla_logica* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}

	return list_find(tablas_en_lfs, _es_tabla_con_nombre);
}

bool tabla_esta_bloqueada(char* nombre_tabla){

	t_tabla_logica* tabla_logica = buscar_tabla_logica_con_nombre(nombre_tabla);
	if (tabla_logica!=NULL) {
		log_info(logger, "verificando si la tabla esta bloqueada. NULL:%d - Bloqueada: %d", tabla_logica !=NULL, tabla_logica->esta_bloqueado);
	}else{
		log_info(logger, "La tabla %s no existe", nombre_tabla);
	}

	return (tabla_logica != NULL) && (tabla_logica->esta_bloqueado);

}

t_status_solicitud* resolver_drop(t_log* log_a_usar, char* nombre_tabla){
	sleep(retardo);

	t_status_solicitud* paquete_a_enviar;
	log_info(log_a_usar, "Se realiza DROP");
	log_info(log_a_usar, "Tabla: %s",nombre_tabla);
	char* dir_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	if (existe_tabla_fisica(nombre_tabla)){
		t_tabla_logica* tabla_logica = buscar_tabla_logica_con_nombre(nombre_tabla);
		pthread_mutex_lock(&(tabla_logica->mutex_compactacion));
		pthread_mutex_lock(&(tabla_logica->mutex_select_drop));
		eliminar_tabla_memtable(nombre_tabla);
		log_info(logger_compactacion, "Se elimina la tabla %s. Se procede a frenar el hilo.", nombre_tabla);
		eliminar_parametros_compactacion(tabla_logica->parametros);
		if (pthread_cancel(tabla_logica->id_hilo_compactacion) != 0){
			log_error(logger_compactacion, "Error al cancelar hilo de compactacion");
		}
		pthread_mutex_t mutex = (tabla_logica->mutex_select_drop);
		pthread_mutex_unlock(&(tabla_logica->mutex_compactacion));
		eliminar_tabla_logica(nombre_tabla);
		liberar_bloques_tabla(dir_tabla);
		eliminar_directorio(dir_tabla);
		paquete_a_enviar = crear_paquete_status(true, "OK");
		pthread_mutex_unlock(&mutex);


	}else{
		char * mje_error = string_from_format("La tabla %s no existe", nombre_tabla);
		log_error(log_a_usar, mje_error);
		paquete_a_enviar = crear_paquete_status(false, mje_error);
		free(mje_error);
	}
	free(dir_tabla);
	return paquete_a_enviar;
}
void liberar_bloques_tabla(char* path_tabla){

	//pthread_mutex_lock(&mutexBloques);
	DIR * dir = opendir(path_tabla);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 ) && ( archivo_es_del_tipo(entry->d_name,"temp") || archivo_es_del_tipo(entry->d_name,"tempc") || archivo_es_del_tipo(entry->d_name,"bin"))) {
			char* dir_archivo = string_from_format("%s/%s", path_tabla, entry->d_name);

			liberar_bloques_archivo(dir_archivo);

			free(dir_archivo);
		}
		entry = readdir(dir);
	}
	closedir(dir);
	//pthread_mutex_unlock(&mutexBloques);
}

void liberar_bloques_archivo(char* path_archivo){
	pthread_mutex_lock(&mutexBitmap);
	log_info(logger, "se trata de liberar bloques del archivo %s", path_archivo);
	t_config* archivo = config_create(path_archivo);
	int size_files = config_get_int_value(archivo, "SIZE");
	char **bloques = config_get_array_value(archivo, "BLOCKS");
	int ind_bloques=0;

	while(bloques[ind_bloques]!=NULL){
		liberar_bloque(atoi(bloques[ind_bloques]));
		free(bloques[ind_bloques]);
		ind_bloques = ind_bloques + 1;

	}
	free(bloques);
	config_destroy(archivo);
	pthread_mutex_unlock(&mutexBitmap);
}

void eliminar_tabla_memtable(char* nombre_tabla){
	int _es_tabla_con_nombre(t_cache_tabla* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}
	pthread_mutex_lock(&mutexMemtable);
	t_list* tabla_cache_eliminada = list_remove_by_condition(memtable, _es_tabla_con_nombre);
	pthread_mutex_unlock(&mutexMemtable);
	if (tabla_cache_eliminada!=NULL){
		list_destroy(tabla_cache_eliminada);
	}
}

void eliminar_directorio(char* path_tabla){

	DIR * dir = opendir(path_tabla);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 ) {
			char* dir_archivo = string_from_format("%s/%s", path_tabla, entry->d_name);
			if (unlink(dir_archivo) == 0)
				log_info(logger, "Eliminado archivo: %s\n", entry->d_name);
			else
				log_info(logger, "No se puede eliminar archivo: %s\n", entry->d_name);
			free(dir_archivo);
		}
		entry = readdir(dir);
	}
	if (rmdir(path_tabla) == 0)
		log_info(logger, "Eliminado dir tabla: %s\n", path_tabla);
	else
		log_info(logger, "No se puede eliminar dir tabla: %s\n", path_tabla);

	closedir(dir);
}


bool validar_datos_describe(char* nombre_tabla, int socket_memoria){
	t_status_solicitud* status;
	bool es_valido = true;
	if(!string_is_empty(nombre_tabla)){
		if (existe_tabla_fisica(nombre_tabla)) {
			status = crear_paquete_status(true, "OK");
			enviar_status_resultado(status, socket_memoria);
		}else {
			char * mje_error = string_from_format("La tabla %s no existe", nombre_tabla);
			log_error(logger, mje_error);
			status = crear_paquete_status(false, mje_error);
			enviar_status_resultado(status, socket_memoria);
			free(mje_error);
			es_valido = false;
		}
	}
	return es_valido;
}


void obtener_tablas_en_lfs(){
	tablas_en_lfs = list_create();
	char* path_tablas = string_from_format("%s/Tables", path_montaje);
	DIR * dir = opendir(path_tablas);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (entry->d_type == DT_DIR &&  ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 )){
			char* nombre_tabla = entry->d_name;
			crear_tabla_logica(nombre_tabla);
		}
		entry = readdir(dir);
	}
	closedir(dir);
	free(path_tablas);
}

void agregar_tabla_logica(char* nombre_tabla){
	t_tabla_logica* tabla_fisica = crear_tabla_logica(nombre_tabla);
	pthread_mutex_lock(&mutexTablasLFS);
	list_add(tablas_en_lfs, tabla_fisica);
	pthread_mutex_unlock(&mutexTablasLFS);
}

void eliminar_tabla_logica(char* nombre_tabla){
	bool _es_tabla_con_nombre(t_tabla_logica* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}
	pthread_mutex_lock(&mutexTablasLFS);
	t_tabla_logica* tabla_logica = list_remove_by_condition(tablas_en_lfs, _es_tabla_con_nombre);
	pthread_mutex_unlock(&mutexTablasLFS);
	if (tabla_logica!=NULL){

		free(tabla_logica->nombre);
		free(tabla_logica);
	}

}

void eliminar_parametros_compactacion (t_parametros_compactacion* parametros){
	free(parametros->nombre);
	free(parametros->path_tabla);
	free(parametros);
}

t_tabla_logica* crear_tabla_logica(char* nombre_tabla){

	t_tabla_logica* tabla = malloc(sizeof(t_tabla_logica));
	tabla->esta_bloqueado=false;
	tabla->nombre=malloc(string_size(nombre_tabla));
	memcpy(tabla->nombre, nombre_tabla, string_size(nombre_tabla));
	pthread_mutex_t mutex_compactacion_tabla;
	if(pthread_mutex_init(&mutex_compactacion_tabla,NULL)==0){
		log_info(logger, "MutexCompactacion para tabla %s se inicializo correctamente", nombre_tabla);
	} else {
		log_error(logger, "Fallo inicializacion de MutexCompactacion para tabla %s", nombre_tabla);
	}
	pthread_mutex_t mutex_select, mutex_sel_drop;
	if(pthread_mutex_init(&mutex_select,NULL)==0){
		log_info(logger, "mutex_compac_select para tabla %s se inicializo correctamente", nombre_tabla);
	} else {
		log_error(logger, "Fallo inicializacion de mutex_compac_select para tabla %s", nombre_tabla);
	}
	if(pthread_mutex_init(&mutex_sel_drop,NULL)==0){
			log_info(logger, "mutex_select_drop para tabla %s se inicializo correctamente", nombre_tabla);
		} else {
			log_error(logger, "Fallo inicializacion de mutex_compac_select para tabla %s", nombre_tabla);
		}
	tabla->mutex_compactacion = mutex_compactacion_tabla;
	tabla->mutex_compac_select = mutex_select;
	tabla->mutex_select_drop = mutex_sel_drop;
	pthread_mutex_lock(&mutexTablasLFS);
	list_add(tablas_en_lfs, tabla);
	pthread_mutex_unlock(&mutexTablasLFS);
	t_parametros_compactacion* parametros = crear_parametros_compactacion(tabla->nombre);
	pthread_t hilo = crear_hilo_compactacion(parametros);
	tabla->id_hilo_compactacion = hilo;
	tabla->parametros = parametros;
	log_info(logger, "Hilo %d de compactacion de tabla %s esta iniciado", hilo, nombre_tabla);
	return tabla;
}

void obtener_bitmap(){

	char* nombre_bitmap = string_from_format("%s/Metadata/Bitmap.bin", path_montaje);

	int fd = open(nombre_bitmap, O_RDWR);
    if (fstat(fd, &size_bitmap) < 0) {
        printf("Error al establecer fstat\n");
        close(fd);
    }

    bloques_disponibles=0;
    bmap = mmap(NULL, size_bitmap.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,	fd, 0);
	bitarray = bitarray_create_with_mode(bmap, blocks/8, MSB_FIRST);
	for(int i=0;i< bitarray_get_max_bit(bitarray);i++){
		 if(bitarray_test_bit(bitarray, i) == 0){
			 bloques_disponibles = bloques_disponibles+1;
		 }
	}
	log_info(logger, "Cantidad de bloques disponibles: %d", bloques_disponibles);
	free(nombre_bitmap);
}

t_metadata* obtener_info_metadata_tabla(char* dir_tabla, char* nombre_tabla){
	char* path_metadata_tabla = string_from_format("%s/Metadata", dir_tabla);

	t_config* metadata_tabla_config  = config_create(path_metadata_tabla);

	char* consistencia_string;
	long compactacion ;

	int num_particiones = config_get_int_value(metadata_tabla_config, "PARTITIONS");

	consistencia_string= config_get_string_value(metadata_tabla_config, "CONSISTENCY");

	compactacion =  config_get_long_value(metadata_tabla_config, "COMPACTION_TIME");

	t_metadata* metadata_tabla = crear_paquete_metadata(nombre_tabla, get_valor_consistencia(consistencia_string), num_particiones, compactacion);
	free(path_metadata_tabla);
	config_destroy(metadata_tabla_config);
	return metadata_tabla;

}

void obtener_info_metadata(){

	char* path_metadata = string_from_format("%s/Metadata/Metadata", path_montaje);
	t_config* metadata  = config_create(path_metadata);
	block_size = config_get_int_value(metadata, "BLOCK_SIZE");
	blocks = config_get_int_value(metadata, "BLOCKS");

	config_destroy(metadata);
	free(path_metadata);
}

t_status_solicitud* resolver_create (t_log* log_a_usar,char* nombre_tabla, t_consistencia consistencia, int num_particiones, long compactacion){
	sleep(retardo);

	/*
	 * Verificar que la tabla no exista en el file system. Por convención, una tabla existe si ya hay
		otra con el mismo nombre. Para dichos nombres de las tablas siempre tomaremos sus
		valores en UPPERCASE (mayúsculas). En caso que exista, se guardará el resultado en un
		archivo .log y se retorna un error indicando dicho resultado.
		2. Crear el directorio para dicha tabla.
		3. Crear el archivo Metadata asociado al mismo.
		4. Grabar en dicho archivo los parámetros pasados por el request.
		5. Crear los archivos binarios asociados a cada partición de la tabla y asignar a cada uno un
		bloque
	 * */
	t_status_solicitud* status;
	log_info(log_a_usar, "Se realiza CREATE");
	log_info(log_a_usar, "Tabla: %s",nombre_tabla);
	log_info(log_a_usar, "Num Particiones: %d",num_particiones);
	log_info(log_a_usar, "Tiempo compactacion: %d", compactacion);
	log_info(log_a_usar, "Consistencia: %d", consistencia);

	if(existe_tabla_fisica(nombre_tabla)){
		char * mje_error = string_from_format("La tabla %s ya existe", nombre_tabla);
		log_error(log_a_usar, mje_error);
		status = crear_paquete_status(false, mje_error);
		free(mje_error);
	}else{

		if (bloques_disponibles < num_particiones ){
			char * mje_error = string_from_format("No hay suficientes bloques disponibles para crear la tabla %s", nombre_tabla);
			log_error(log_a_usar, mje_error);
			status = crear_paquete_status(false, mje_error);
			free(mje_error);
		}else{
			char* dir_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
			if (crear_directorio_tabla(dir_tabla)){
				crear_archivo_metadata_tabla(dir_tabla, num_particiones, compactacion, consistencia);
				crear_particiones(dir_tabla, num_particiones);
				crear_tabla_logica(nombre_tabla);
				status = crear_paquete_status(true, "OK");
			}else{
				char * mje_error = string_from_format("No pudo crearse la tabla %s", nombre_tabla);
				log_error(log_a_usar, mje_error);
				status = crear_paquete_status(false, mje_error);
				free(mje_error);
			}
			free(dir_tabla);
		}


	}

	return status;

}

void crear_particiones(char* dir_tabla,int  num_particiones){

	int ind;
	for(ind =0;ind < num_particiones; ind++){
		char* dir_particion = string_from_format("%s/%i.bin", dir_tabla, ind);
		t_list* bloques = list_create();
		//pthread_mutex_lock(&mutexBloques);
		int num_bloque= obtener_bloque_disponible();
		//pthread_mutex_unlock(&mutexBloques);
		list_add(bloques, num_bloque);   //paso la direccion de memoria de num_bloque, que adentro tiene el bloque disponible

		crear_archivo(dir_particion, 0, bloques);
		free(dir_particion);
		list_destroy(bloques);
	}
}

void crear_archivo(char* dir_archivo, int size, t_list* array_bloques){
	//pthread_mutex_lock(&mutex_temp);
	FILE* file = fopen(dir_archivo, "wb+");
	fclose(file);

	guardar_datos_particion_o_temp(dir_archivo, size, array_bloques);
	//pthread_mutex_unlock(&mutex_temp);
}

void guardar_datos_particion_o_temp(char* dir_archivo, int size, t_list* array_bloques){
	char * array_bloques_string = array_int_to_array_char(array_bloques);
	char * char_size = string_itoa(size);

	t_config* particion_tabla = config_create(dir_archivo);
	dictionary_put(particion_tabla->properties,"SIZE", char_size);
	dictionary_put(particion_tabla->properties, "BLOCKS", array_bloques_string);

	config_save(particion_tabla);
	//TODO:ver si esto da bien
	config_destroy(particion_tabla);
}

char* array_int_to_array_char(t_list* array_int){
	char * array_char = string_new();
	string_append(&array_char, "[");

	void _agregar_como_string(int* valor){
		char* valor_string=string_itoa((int)valor);
		string_append(&array_char, valor_string);
		string_append(&array_char, ",");
		free(valor_string);
	}

	list_iterate(array_int, (void*)_agregar_como_string);

	char* array_char_sin_ultima_coma = string_substring_until(array_char, string_length(array_char) -1);
	string_append(&array_char_sin_ultima_coma,"]");
	free(array_char);
	//TODO: ver si esto esta bien
	return array_char_sin_ultima_coma;
}

void liberar_bloque(int num_bloque){

	char* dir_bloque = string_from_format("%s/Bloques/%d.bin", path_montaje, num_bloque);
	FILE* fpFile = fopen(dir_bloque,"wb");
	fclose(fpFile);
	log_info(logger_compactacion, "Se libera bloque %s", dir_bloque);
	truncate(dir_bloque, block_size);
	bitarray_clean_bit(bitarray, num_bloque);
	msync(bmap, sizeof(bitarray), MS_SYNC);
	bloques_disponibles=bloques_disponibles+1;
	log_info(logger, "Cantidad de bloques disponibles: %d", bloques_disponibles);
	free(dir_bloque);
}

int obtener_bloque_disponible(){
	pthread_mutex_lock(&mutexBitmap);
	bool esta_ocupado=true;
	int nro_bloque=0;
	while(esta_ocupado == true){
		esta_ocupado = bitarray_test_bit(bitarray, nro_bloque);
		nro_bloque=nro_bloque+1;
	}
	nro_bloque=nro_bloque-1;
	bitarray_set_bit(bitarray,nro_bloque);
	bloques_disponibles = bloques_disponibles -1;
	log_info(logger, "Cantidad de bloques disponibles: %d", bloques_disponibles);
	log_info(logger_compactacion, "Se ocupa bloque %d", nro_bloque);
	msync(bmap, sizeof(bitarray), MS_SYNC);
	pthread_mutex_unlock(&mutexBitmap);
	return nro_bloque;

}

void crear_archivo_metadata_tabla(char* dir_tabla, int num_particiones,long compactacion,t_consistencia consistencia){

	char* dir_metadata_tabla = string_from_format("%s/Metadata", dir_tabla);
	FILE* file = fopen(dir_metadata_tabla, "wb+");
	fclose(file);

	t_config* metadata_tabla = config_create(dir_metadata_tabla);
	dictionary_put(metadata_tabla->properties,"CONSISTENCY", consistencia_to_string(consistencia) );
	dictionary_put(metadata_tabla->properties, "PARTITIONS", string_itoa(num_particiones));
	dictionary_put(metadata_tabla->properties, "COMPACTION_TIME", string_itoa(compactacion));

	free(dir_metadata_tabla);
	config_save(metadata_tabla);
	config_destroy(metadata_tabla);
}


int crear_directorio_tabla (char* dir_tabla){

	return !(mkdir(dir_tabla, 0777) != 0 && errno != EEXIST);
}

void resolver_describe(char* nombre_tabla, int socket_memoria){
	sleep(retardo);
	log_info(logger, "Se realiza DESCRIBE");
	if(string_is_empty(nombre_tabla)){
		int cant_tablas = obtener_cantidad_tablas_LFS();
		enviar_numero_de_tablas(socket_memoria, cant_tablas);
		log_info(logger, "Se trata de un describe global.");
		enviar_metadata_todas_tablas(socket_memoria);
	}else{
		enviar_tabla_para_describe(socket_memoria, nombre_tabla);
	}
}

int obtener_cantidad_tablas_LFS(){
	int cant_tablas = 0;
	char* path_tablas = string_from_format("%s/Tables", path_montaje);
	DIR * dir = opendir(path_tablas);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (entry->d_type == DT_DIR &&  ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 )) {
			log_info(logger,"Nombre: %s", entry->d_name);
			cant_tablas = cant_tablas + 1;
		}
		entry = readdir(dir);
	}
	closedir(dir);
	free(path_tablas);
	return cant_tablas;
}

void enviar_metadata_todas_tablas (int socket_memoria){
	char* path_tablas = string_from_format("%s/Tables", path_montaje);
	DIR * dir = opendir(path_tablas);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (entry->d_type == DT_DIR &&  ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 )){
			char* nombre_tabla = entry->d_name;
			enviar_tabla_para_describe(socket_memoria, nombre_tabla);
		}
		entry = readdir(dir);
	}
	closedir(dir);
	free(path_tablas);
}

void enviar_tabla_para_describe(int socket_memoria, char* nombre_tabla){
	log_info(logger, "Tabla: %s", nombre_tabla);
	char* dir_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	t_metadata* metadata_tabla = obtener_info_metadata_tabla(dir_tabla, nombre_tabla);
	enviar_paquete_metadata(socket_memoria, metadata_tabla);
	log_info(logger, "Metadata tabla: %s", metadata_tabla->nombre_tabla->palabra);
	log_info(logger, "Consistencia: %s", consistencia_to_string(metadata_tabla->consistencia));
	log_info(logger, "Numero de particiones: %d", metadata_tabla->n_particiones);
	log_info(logger, "Tiempo de compactacion: %d", metadata_tabla->tiempo_compactacion);
	eliminar_metadata(metadata_tabla);
	free(dir_tabla);
}

void resolver_describe_drop (int socket_memoria, char* operacion){
	t_paquete_drop_describe* consulta_describe_drop = deserealizar_drop_describe(socket_memoria);
	log_info(logger, "Se realiza %s", operacion);
	log_info(logger, "Tabla: %s", consulta_describe_drop->nombre_tabla->palabra);
	eliminar_paquete_drop_describe(consulta_describe_drop);
}

t_status_solicitud*  resolver_insert(t_log* log_a_usar, char* nombre_tabla, uint16_t key, char* value, long timestamp){
	sleep(retardo);

	t_status_solicitud* paquete_a_enviar;
	log_info(log_a_usar, "Se realiza INSERT");
	log_info(log_a_usar, "Consulta en la tabla: %s", nombre_tabla );
	log_info(log_a_usar, "Consulta por key: %d", key);
	log_info(log_a_usar, "Valor: %s", value);
	log_info(log_a_usar, "TIMESTAMP: %d",timestamp);
	if (existe_tabla_fisica(nombre_tabla)){
		t_tabla_logica* tabla = buscar_tabla_logica_con_nombre(nombre_tabla);
		//pthread_mutex_lock(&tabla->mutex_compac_select);
		//llenar los datos de consistencia, particion que estan en la metadata de la tabla (ingresar al directorio de la tabla) Metadata
		pthread_mutex_lock(&mutexMemtable);
		agregar_registro_memtable(crear_registro(value, key,  timestamp), nombre_tabla);
		pthread_mutex_unlock(&mutexMemtable);
		log_info(logger, "Se agregó registro a la Memtable");
		paquete_a_enviar = crear_paquete_status(true, "OK");
		//pthread_mutex_unlock(&tabla->mutex_compac_select);

	}else{
		char * mje_error = string_from_format("La tabla %s no existe", nombre_tabla);
		log_error(log_a_usar, mje_error);
		paquete_a_enviar = crear_paquete_status(false, mje_error);
		free(mje_error);
	}

	return paquete_a_enviar;

}



t_registro* crear_registro(char* value, uint16_t key, long timestamp){

	t_registro* nuevo_registro = malloc(sizeof(t_registro));
	nuevo_registro->key=key;
	nuevo_registro->timestamp = timestamp;
	nuevo_registro->value=malloc(string_size(value));
	memcpy(nuevo_registro->value, value, string_size(value));

	return nuevo_registro;
}

void agregar_registro_memtable(t_registro* registro_a_insertar, char * nombre_tabla){

	t_cache_tabla* tabla_cache = obtener_tabla_memtable(nombre_tabla);

	list_add(tabla_cache->registros, registro_a_insertar );

}

t_cache_tabla* obtener_tabla_memtable(char* nombre_tabla){

	t_cache_tabla* tabla_cache = buscar_tabla_memtable(nombre_tabla);
	if(tabla_cache == NULL){
		tabla_cache = crear_tabla_cache(nombre_tabla);
		list_add(memtable, tabla_cache);
	}
	return tabla_cache;

}

t_cache_tabla* crear_tabla_cache(char* nombre_tabla){

	t_cache_tabla* nueva_tabla_cache = malloc(sizeof(t_cache_tabla));
	nueva_tabla_cache->nombre = malloc(string_size(nombre_tabla));
	memcpy(nueva_tabla_cache->nombre, nombre_tabla, string_size(nombre_tabla));
	nueva_tabla_cache->registros = list_create();
	return nueva_tabla_cache;
}


t_cache_tabla* buscar_tabla_memtable(char* nombre_tabla){
	bool _es_tabla_con_nombre(t_cache_tabla* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}

	return list_find(memtable, (void*) _es_tabla_con_nombre);
}

bool existe_tabla_fisica(char* nombre_tabla){
//	char* path_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
//
//	struct stat sb;
//
//	return (stat(path_tabla, &sb) == 0 && S_ISDIR(sb.st_mode));
	bool _es_tabla_con_nombre(t_tabla_logica* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}


	return list_any_satisfy(tablas_en_lfs, _es_tabla_con_nombre);
}


t_status_solicitud* resolver_select (char* nombre_tabla, uint16_t key){
	sleep(retardo);


	t_status_solicitud* status;
	log_info(logger, "Se realiza SELECT");
	log_info(logger, "Consulta en la tabla: %s", nombre_tabla);
	log_info(logger, "Consulta por key: %d", key);
	t_list* registros_encontrados = list_create();
	if (existe_tabla_fisica(nombre_tabla)){
		t_tabla_logica* tabla = buscar_tabla_logica_con_nombre(nombre_tabla);

		pthread_mutex_lock(&tabla->mutex_compac_select);

		pthread_mutex_lock(&tabla->mutex_select_drop);
		log_info(logger,  "pase todos los mutex en el select");
		t_list* registros_memtable = buscar_registros_memtable(nombre_tabla, key);
		t_list* registros_temporales = buscar_registros_temporales(nombre_tabla, key);
		t_list* registros_particion = buscar_registros_en_particion(nombre_tabla, key);

		list_add_all(registros_encontrados, registros_memtable);
		list_add_all(registros_encontrados, registros_temporales);
		list_add_all(registros_encontrados, registros_particion);
		log_info(logger,  "Se realizó la busqueda de todos los registros para resolver Select");
		t_registro* registro_buscado = buscar_registro_actual(registros_encontrados);
		if(registro_buscado !=NULL){
			log_info(logger,  "Se encontró resultado. Se procede a enviar el resultado a memoria");
			char* resultado = generar_registro_string(registro_buscado->timestamp, registro_buscado->key, registro_buscado->value);
			status = crear_paquete_status(true, resultado );
			//TODO: ver si esto esta bien
			free(resultado);

		}else{
			char * mje_error = string_from_format("No se encontró registro con key: %d en la tabla %s", key, nombre_tabla);
			log_error(logger, mje_error);
			status = crear_paquete_status(false, mje_error);
			free(mje_error);
		}
		//TODO: ver si esto esta bien
		list_destroy(registros_memtable);
		list_destroy_and_destroy_elements(registros_temporales, eliminar_registro);
		list_destroy_and_destroy_elements(registros_particion, eliminar_registro);
		pthread_mutex_unlock(&tabla->mutex_select_drop);

		pthread_mutex_unlock(&tabla->mutex_compac_select);

	}else{
		char * mje_error = string_from_format("La tabla %s no existe", nombre_tabla);
		log_error(logger, mje_error);
		status = crear_paquete_status(false, mje_error);
		free(mje_error);
	}
	list_destroy(registros_encontrados);
	return status;

}
t_list* buscar_registros_en_particion(char* nombre_tabla,uint16_t key){
	//pthread_mutex_lock(&mutexBloques);

	char* path_metadata = string_from_format("%s/Tables/%s/Metadata", path_montaje, nombre_tabla);
	t_config* metadata = config_create(path_metadata);
	int num_particiones = config_get_int_value(metadata, "PARTITIONS");
	t_list* registros_encontrados = list_create();
	int particion = key % num_particiones;
	char* path_particion = string_from_format("%s/Tables/%s/%d.bin", path_montaje, nombre_tabla, particion);

	t_list* registros_en_particion = obtener_registros_de_archivo(path_particion);
	t_list* registros_con_key=	filtrar_registros_con_key(registros_en_particion, key);
	list_add_all(registros_encontrados, registros_con_key);
	list_destroy(registros_en_particion);
	list_destroy(registros_con_key);
	config_destroy(metadata);
	//TODO:ver
	free(path_metadata);
	free(path_particion);
	//pthread_mutex_unlock(&mutexBloques);
	return registros_encontrados;
}

t_list* buscar_registros_temporales(char* nombre_tabla, uint16_t key){
	//pthread_mutex_lock(&mutexBloques);
	char* path_tablas = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	t_list* registros_encontrados = list_create();
	DIR * dir = opendir(path_tablas);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 ) && (archivo_es_del_tipo(entry->d_name,"temp") || archivo_es_del_tipo(entry->d_name,"tempc"))){
			char* path_archivo_temporal = string_from_format("%s/%s", path_tablas, entry->d_name);
			t_list* registros_en_temporal = obtener_registros_de_archivo(path_archivo_temporal);
			t_list* registros_con_key=	filtrar_registros_con_key(registros_en_temporal, key);
			list_add_all(registros_encontrados, registros_con_key);
			//TODO:ver si esta bien
			free(path_archivo_temporal);
			list_destroy(registros_con_key);
			list_destroy(registros_en_temporal);
		}
		entry = readdir(dir);
	}
	closedir(dir);
	//TODO:ver si esta bien
	//pthread_mutex_unlock(&mutexBloques);
	free(path_tablas);
	return registros_encontrados;
}

t_list* filtrar_registros_con_key(t_list* registros, uint16_t key){

	bool _es_registro_con_key(t_registro* registro){
		return registro->key== key;
	}
	t_list* registros_filtrados = list_create();
	t_list* registros_descartados=list_create();

	void _add_if_apply(t_registro* registro) {
		if (registro->key== key) {
			list_add(registros_filtrados, registro);
		}else{
			list_add(registros_descartados, registro);
		}
	}

	list_iterate(registros, _add_if_apply);
	list_destroy_and_destroy_elements(registros_descartados, eliminar_registro);

	return  registros_filtrados;
}

t_list* obtener_registros_de_archivo(char* path_archivo_temporal){


	char* buffer_registros = leer_bloques_de_archivo(path_archivo_temporal);

	return obtener_registros_de_buffer(buffer_registros);

}

char* leer_bloques_de_archivo(char* path_archivo){
	t_config* archivo = config_create(path_archivo);
	int size_files = config_get_int_value(archivo, "SIZE");
	//log_info(logger, "linea 1071. Al leer el archivo con path %s , SIZE= %d", path_archivo, size_files);
	char **bloques = config_get_array_value(archivo, "BLOCKS");
	int resto_a_leer = size_files;
	int size_a_leer;
	int ind_bloques=0;
	char* buffer;
	char* buffer_bloques = string_new();

	if (size_files!=0){
		while(bloques[ind_bloques]!=NULL){
			if (resto_a_leer-block_size > 0){
				size_a_leer = block_size;
				resto_a_leer=resto_a_leer-block_size;
			}else{
				size_a_leer = resto_a_leer;
			}
			//log_info(logger, "linea 1087. El numero de Bloque a abrir es: %s", bloques[ind_bloques]);
			int num_bloque=atoi(bloques[ind_bloques]);

			char* dir_bloque = string_from_format("%s/Bloques/%i.bin", path_montaje, num_bloque);
			FILE* file = fopen(dir_bloque, "rb+");
//			fseek(file, 0, SEEK_END);
//			// int tamanioArchivo = sizeof(char) * ftell(fp);
//			long tamanioArchivo = ftell(file);
//			fseek(file, 0, SEEK_SET);
			buffer = malloc(size_a_leer+1);

			size_t read_count = fread(buffer, sizeof(char), size_a_leer, file);
			//log_info(logger, "linea 1099. Cantidad de bytes leidos son: %d", read_count);
			buffer[read_count]='\0';
			string_append(&buffer_bloques, buffer);
			fclose(file);
			free(buffer);
			free(dir_bloque);
			free(bloques[ind_bloques]);
			ind_bloques = ind_bloques+1;
		}
	}
	//buffer_bloques[size_files]="\0";
	free(bloques);
	config_destroy(archivo);
	//log_info(logger, "linea 1112. Buffer de todos los bloques en el path: %s", path_archivo);
	return buffer_bloques;

}

t_list* obtener_registros_de_buffer(char* buffer){
	t_list* registros_de_bloques = list_create();
	char **array_buffer_registro = string_split(buffer, "\n");
	//log_info(logger, "1118. BUFFER recibido en los bloques  %s", buffer);
	int ind_registros=0;
	while (array_buffer_registro[ind_registros]!=NULL){
//		log_info(logger, "nuevo buffer registro");
//		log_info(logger, "1120. aca puede romper %s", array_buffer_registro[ind_registros]);
		char** string_registro = string_split(array_buffer_registro[ind_registros], ";");
		//log_info(logger, "1122. aca puede romper %s", string_registro[1]);
		uint16_t key = (uint16_t) atol(string_registro[1]);
		long timestamp = (long)atol(string_registro[0]);
		t_registro* registro= crear_registro(string_registro[2], key, timestamp);
		list_add(registros_de_bloques, registro);
		free(string_registro[0]);
		free(string_registro[1]);
		free(string_registro[2]);
		free(string_registro);
		free(array_buffer_registro[ind_registros]);
		ind_registros = ind_registros+1;


	}
	free(array_buffer_registro);
	free(buffer);
//	sem_post(&bloques_consumer);
	//TODO: ver qe hacer con ese array_buffer_registro
	return registros_de_bloques;

}

t_list* buscar_registros_memtable(char* nombre_tabla, uint16_t key){

	int _es_tabla_con_nombre(t_cache_tabla* tabla) {
			return string_equals_ignore_case(tabla->nombre, nombre_tabla);
		}
	int _es_registro_con_key(t_registro* registro){
		return registro->key== key;
	}

	pthread_mutex_lock(&mutexMemtable);
	t_list* registros_encontrados;
	t_cache_tabla* tabla_cache= list_find(memtable, (void*) _es_tabla_con_nombre);
	if(tabla_cache!=NULL){
		registros_encontrados=list_filter(tabla_cache->registros,(void*) _es_registro_con_key);
	}else{
		registros_encontrados = list_create();
	}
	pthread_mutex_unlock(&mutexMemtable);
	return registros_encontrados;
}

t_registro* buscar_registro_actual(t_list* registros_encontrados){
	t_registro* registro_actual = NULL;
	int _registro_mayor_por_timestamp(t_registro* registroA, t_registro* registroB){
			return registroA->timestamp>registroB->timestamp;
	}
	if (!list_is_empty(registros_encontrados)){
		 list_sort(registros_encontrados, _registro_mayor_por_timestamp);
		 registro_actual= list_get(registros_encontrados,0);
	}
	return registro_actual;
}

 void iniciar_loggers() { 								// CREACION DE LOG
	logger = log_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/logs/lfs.log", "LFS", 0, LOG_LEVEL_INFO);
	logger_dump = log_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/logs/dump_lfs.log", "LFS", 0, LOG_LEVEL_INFO);
	logger_compactacion = log_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/logs/compactacion_lfs.log", "LFS", 0, LOG_LEVEL_INFO);
	logger_consola = log_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/logs/consola_lfs.log", "LFS", 1, LOG_LEVEL_INFO);
}

void leer_config() {				// APERTURA DE CONFIG
	archivoconfig = config_create("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/lfs.config");
}

void leer_tiempo_dump_y_retardo_del_config(){
	tiempo_dump = config_get_int_value(archivoconfig,"TIEMPO_DUMP");
	retardo = config_get_long_value(archivoconfig, "RETARDO");
}

void* iniciar_inotify_lfs(){
	#define MAX_EVENTS 1024 /*Max. number of events to process at one go*/
	#define LEN_NAME 64 /*Assuming that the length of the filename won't exceed 16 bytes*/
	#define EVENT_SIZE  ( sizeof (struct inotify_event) ) /*size of one event*/
	#define BUF_LEN     ( MAX_EVENTS * ( EVENT_SIZE + LEN_NAME )) /*buffer to store the data of events*/

	char* path = "/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/LFS/lfs.config";
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
				leer_tiempo_dump_y_retardo_del_config();

				log_info(logger, "El dump es de %d", tiempo_dump);
			}
		}
	}
	inotify_rm_watch( fd, wd );
	close( fd );
}

void crear_hilo_inotify(){
	if (pthread_create(&hilo_inotify, 0, iniciar_inotify_lfs, NULL) !=0){
			log_error(logger, "Error al crear el hilo de Inotify");
		}
	if (pthread_detach(hilo_inotify) != 0){
			log_error(logger, "Error al crear el hilo de Inotify");
		}
}

