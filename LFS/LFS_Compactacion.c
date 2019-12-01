/*
 * LFS_Compactacion.c
 *
 *  Created on: 27 jun. 2019
 *      Author: utnso
 */
#include "LFS_Compactacion.h"

void *compactar(void* args){
	t_parametros_compactacion* parametros = (t_parametros_compactacion* ) args;
	//char* tabla = (char*) nombre_tabla; Agregar linea comentada si descubro que void* rompe en ejecucion y necesito castera char*
	char* nombre_tabla = parametros->nombre;
	char* path_tabla = parametros->path_tabla;
	//char* path_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	long tiempo_compactacion = obtener_tiempo_compactacion(path_tabla);
	t_tabla_logica* tabla_logica = buscar_tabla_logica_con_nombre(nombre_tabla);
	log_info(logger_compactacion, "Tiempo retardo compactacion: [%i]", tiempo_compactacion);
	while(1){
		log_info(logger_compactacion, "%i", tiempo_compactacion);
		sleep(tiempo_compactacion/1000);
		//log_info(logger_compactacion,  "previo mutex mutex_compactacion en compactar : %d", tabla_logica->mutex_compactacion);
		pthread_mutex_lock(&(tabla_logica->mutex_compactacion));
		//log_info(logger_compactacion,  "previo mutex compac select en compactar : %d", tabla_logica->mutex_compac_select);
		pthread_mutex_lock(&(tabla_logica->mutex_compac_select));
		log_info(logger_compactacion,  "Pase todos los mutex en compactacion: %d", tabla_logica->mutex_compac_select);
		if (!hay_temporales(path_tabla)) {
			log_info(logger_compactacion, "Compactacion- No hay datos para compactar en: %s.", path_tabla);
			pthread_mutex_unlock(&(tabla_logica->mutex_compac_select));
		}else{
			log_info(logger_compactacion, "Se realiza compactacion en: %s.", path_tabla);

			t_list* temporales_a_compactar = renombrar_archivos_para_compactar(path_tabla);
			log_info(logger_compactacion, "Se renombraron %d archivos", list_size(temporales_a_compactar));
			//bloquear_tabla(nombre_tabla);
			t_list* registros = leer_registros_temporales(temporales_a_compactar);
			t_list* registros_por_particiones= filtrar_registros_duplicados_segun_particiones(path_tabla, registros); //esto podria devolver una matris filtrando los datos respecto de la particion a la que corresponde, devolveria una lista de lista donde cada posicin de la lista es el index de la particion, y la lista en esa posicion contiene los registros filtrados en base a ese archivo

			list_destroy_and_destroy_elements(temporales_a_compactar, free);
			list_destroy(registros);

			log_info(logger_compactacion, "Comienza bloqueo de tabla: %s por compactacion.", nombre_tabla);
			int comienzo = time(NULL);

			realizar_compactacion(path_tabla, registros_por_particiones);
			list_destroy_and_destroy_elements(registros_por_particiones, eliminar_registro);
			pthread_mutex_unlock(&tabla_logica->mutex_compac_select);


			//desbloquear_tabla(nombre_tabla);
			log_info(logger_compactacion, "Fin bloqueo de tabla: %s por compactacion.", nombre_tabla);
			int tiempo_operatoria = comienzo - time(NULL);
			log_info(logger_compactacion, "La tabla: %s estuvo bloqueada %d milisegundos por compactacion.", nombre_tabla, tiempo_operatoria);

		}
		pthread_mutex_unlock(&(tabla_logica->mutex_compactacion));
	}
	//TODO:VER
}

t_parametros_compactacion* crear_parametros_compactacion(char* nombre_tabla){

	t_parametros_compactacion* parametros = malloc(sizeof(t_parametros_compactacion));
	parametros->nombre=malloc(string_size(nombre_tabla));
	memcpy(parametros->nombre, nombre_tabla, string_size(nombre_tabla));
	char* path_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	parametros->path_tabla=malloc(string_size(path_tabla));
	memcpy(parametros->path_tabla, path_tabla, string_size(path_tabla));
	free(path_tabla);
	return parametros;


}

void bloquear_tabla(char* nombre_tabla){
	int _es_tabla_con_nombre(t_cache_tabla* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}

	t_tabla_logica* tabla_a_bloquear = list_find(tablas_en_lfs, _es_tabla_con_nombre);
	tabla_a_bloquear->esta_bloqueado=1;
}

void desbloquear_tabla(char* nombre_tabla){
	int _es_tabla_con_nombre(t_cache_tabla* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}

	t_tabla_logica* tabla_a_bloquear = list_find(tablas_en_lfs, _es_tabla_con_nombre);
	tabla_a_bloquear->esta_bloqueado=false;
	log_info(logger_compactacion, "if de instrucciones bloqueadas" );
	if (dictionary_has_key(instrucciones_bloqueadas_por_tabla, nombre_tabla)){
		t_queue* instrucciones_bloqueadas = dictionary_get(instrucciones_bloqueadas_por_tabla, nombre_tabla);
		log_info(logger_compactacion, "entre al if de instrucciones bloqueadas" );
		for(int i=0; i<queue_size(instrucciones_bloqueadas); i++){
			log_info(logger_compactacion, "Comienza ejecucion de instrucciones bloqueadas en tabla %s.", nombre_tabla);
			t_instruccion_bloqueada* instruccion_bloqueada = queue_pop(instrucciones_bloqueadas);
			t_status_solicitud* status;
			if (instruccion_bloqueada->instruccion->operacion == SELECT){
				if (instruccion_bloqueada->socket_memoria==NULL){
					log_info(logger_compactacion, "Comienza select bloqueado en tabla %s solicitado por consola.", nombre_tabla);
					resolver_select_consola(nombre_tabla, instruccion_bloqueada->instruccion->parametros.SELECT.key);
					log_info(logger_compactacion, "Fin select bloqueado en tabla %s solicitado por c.", nombre_tabla);
				}else{
					log_info(logger_compactacion, "Comienza select bloqueado en tabla %s solicitado por memoria.", nombre_tabla);
					status= resolver_select(nombre_tabla, instruccion_bloqueada->instruccion->parametros.SELECT.key);
					enviar_status_resultado(status, instruccion_bloqueada->socket_memoria);
					log_info(logger_compactacion, "Fin select bloqueado en tabla %s solicitado por memoria.", nombre_tabla);
					//eliminar_paquete_status(status);
				}
				free(instruccion_bloqueada->instruccion->parametros.SELECT.tabla);
				free(instruccion_bloqueada->instruccion);
				free(instruccion_bloqueada);

			}else{
				log_info(logger_compactacion, "Comienza insert bloqueado en tabla %s solicitado.", nombre_tabla);
				status = resolver_insert(logger, nombre_tabla, instruccion_bloqueada->instruccion->parametros.INSERT.key, instruccion_bloqueada->instruccion->parametros.INSERT.value, instruccion_bloqueada->instruccion->parametros.INSERT.timestamp);
				if (instruccion_bloqueada->socket_memoria!=NULL){
					enviar_status_resultado(status, instruccion_bloqueada->socket_memoria);
					log_info(logger_compactacion, "Se envia status de insert bloqueado en tabla %s solicitado.", nombre_tabla);
				}
				log_info(logger_compactacion, "Fin insert bloqueado en tabla %s solicitado.", nombre_tabla);
				//eliminar_paquete_status(status);
				free(instruccion_bloqueada->instruccion->parametros.INSERT.tabla);
				free(instruccion_bloqueada->instruccion->parametros.INSERT.value);
				free(instruccion_bloqueada->instruccion);
				free(instruccion_bloqueada);
			}

		}
//		pthread_mutex_lock(&mutexInstBloqueadas);
//		dictionary_remove(instrucciones_bloqueadas_por_tabla, nombre_tabla);
//		pthread_mutex_unlock(&mutexInstBloqueadas);

	}

}

pthread_t crear_hilo_compactacion(t_parametros_compactacion* parametros){
	pthread_t hilo_compactacion;
	if (pthread_create(&hilo_compactacion, 0, compactar, parametros) !=0){
		log_error(logger_compactacion, "Error al crear el hilo para proceso de compactacion");
	}
	if (pthread_detach(hilo_compactacion) != 0){
		log_error(logger_consola, "Error al frenar hilo");
	}
	return hilo_compactacion;
}

t_list* obtener_num_bloques_de_archivo(char* path_tabla){
	t_list* bloques_num = list_create();
	t_config* particion = config_create(path_tabla);
	char** bloques = config_get_array_value(particion,"BLOCKS");

	int ind_bloques=0;

	while(bloques[ind_bloques]!=NULL){
		list_add(bloques_num,(atoi(bloques[ind_bloques])));
		free(bloques[ind_bloques]);
		ind_bloques = ind_bloques + 1;

	}
	free(bloques);
	config_destroy(particion);
	return bloques_num;
}

long obtener_tiempo_compactacion(char* path_tabla){
	char* path_metadata = string_from_format("%s/Metadata", path_tabla);

	t_config* metadata_tabla = config_create(path_metadata);
	long tiempo_compactacion = config_get_long_value(metadata_tabla,"COMPACTION_TIME");
	config_destroy(metadata_tabla);
	//TODO:VER
	free(path_metadata);
	return tiempo_compactacion;
}

bool hay_temporales(char* path_tabla){
	return cantidad_archivos_actuales(path_tabla, "temp")>0;

}

t_list* renombrar_archivos_para_compactar(char* path_tabla){
	//pthread_mutex_lock(&mutexBloques);
	t_list* temporales_a_compactar = list_create();
	//pthread_mutex_lock(&mutex_temp);
	int cantidad_temporales = 0;
	DIR * dir = opendir(path_tabla);
	struct dirent * entry = readdir(dir);

	while(entry != NULL){
		if (( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 ) && archivo_es_del_tipo(entry->d_name, "temp")) {
			cantidad_temporales ++;
			char* temp = string_from_format("%s/%s", path_tabla, entry->d_name);

			char ** nombre_y_extension = string_split(entry->d_name, ".");
			char* tempc = string_from_format("%s/%s.tempc", path_tabla, nombre_y_extension[0]);
			rename(temp, tempc);
			list_add(temporales_a_compactar, tempc);
			//TODO:VER
			free(temp);
			free(nombre_y_extension[0]);
			free(nombre_y_extension[1]);
			free(nombre_y_extension);
			//free a todos esos char?
		}
		entry = readdir(dir);
	}
	closedir(dir);
	//pthread_mutex_unlock(&mutex_temp);
	//pthread_mutex_unlock(&mutexBloques);

	log_info(logger_compactacion, "Renombrando %d archivos temp a tempc en: %s.", cantidad_temporales ,path_tabla);
	return temporales_a_compactar;
}

t_list* leer_registros_temporales(t_list* temporales_a_compactar){
	//pthread_mutex_lock(&mutexBloques);
	t_list* registros = list_create();
	int cantidad_temporales = list_size(temporales_a_compactar);
	log_info(logger_compactacion, "Se leen registros de %d archivos temporales.", cantidad_temporales );
	for(int i=0 ; i<cantidad_temporales; i++){
		//char* path_archivo = string_from_format("%s/%d.tempc", path_tabla, i);
		char* path_archivo = list_get(temporales_a_compactar, i);
		t_list* registros_de_archivo = obtener_registros_de_archivo(path_archivo);
		log_info(logger_compactacion, "Se encontraron %d registros para compactar en el archivo %s", list_size(registros_de_archivo), path_archivo);
		list_add_all(registros, registros_de_archivo );
//		t_config* tempc = config_create(path_archivo);
//		char* bloques = config_get_string_value(tempc,"BLOCKS");
//		int size = config_get_int_value(tempc,"SIZE");
//
//		//t_list* bloques_ints = chars_to_ints(bloques);
//		list_add_all(registros, leer_registros_bloques(bloques,size));
//
//		//free(bloques)
//		//free(path_archivo)
//		config_destroy(tempc);

		list_destroy(registros_de_archivo);
	}

	//pthread_mutex_unlock(&mutexBloques);
	return registros;
}

t_list* leer_registros_particiones(char* path_tabla){

	//pthread_mutex_lock(&mutexBloques);
	char* path_metadata = string_from_format("%s/Metadata", path_tabla);
	t_config* metadata = config_create(path_metadata);
	int num_particiones = config_get_int_value(metadata, "PARTITIONS");

	t_list* registros = list_create();

	for(int i=0 ; i<num_particiones; i++){
		char* path_archivo = string_from_format("%s/%d.bin", path_tabla, i);
		t_list* registros_en_particion = obtener_registros_de_archivo(path_archivo);
		log_info(logger_compactacion, "En la particion se encontraron %d registros", list_size(registros_en_particion));
		list_add(registros, registros_en_particion);
		free(path_archivo);
	}

	config_destroy(metadata);
	free(path_metadata);
	//pthread_mutex_unlock(&mutexBloques);
	return registros;
}

t_registro* crear_t_registros_por_particion(char* value, uint16_t key, long timestamp){

	t_registro* nuevo_registro = malloc(sizeof(t_registro));
	nuevo_registro->key=key;
	nuevo_registro->timestamp = timestamp;
	nuevo_registro->value=malloc(string_size(value));
	memcpy(nuevo_registro->value, value, string_size(value));

	return nuevo_registro;
}
/*
 * t_list* chars_to_ints(char* bloques){
	t_list* bloques_int = list_create();

	for(int i=1 ; bloques[i] != ']'; i++){
		list_add(bloques_int, atoi(bloques[i]));
	}

	return bloques_int;
}*/

/*t_list* leer_registros_bloques(char* bloques, int size_total){
	char* registros = string_new();

	for(int i=1 ; bloques[i] != ']'; i++){
		int bloque = atoi(&bloques[i]);

		string_append(&registros, leer_registros_char_de_bloque(bloque));
	}
	string_append(&registros, "\0");

	t_list* registros_finales = transformar_registros(registros);
	free(registros);

	return registros_finales;
}

char* leer_registros_char_de_bloque(int bloque){
	char* registros = string_new();

	char* dir_bloque = string_from_format("%s/Bloques/%i.bin", path_montaje, bloque);
	FILE* file = fopen(dir_bloque, "rb+");

	char* buffer = (char*) malloc(sizeof(char));

	while(!feof(file)){
		fread(buffer, sizeof(char), 1, file);
		string_append(&registros, buffer);
	}
	fclose(file);
	free(buffer);
	free(dir_bloque);

	return registros;
}

t_list* transformar_registros(char* registros){
	t_list* registros_finales = list_create();

	for(int i=0 ; registros[i] != '\0'; i++){
		int l=0;


		char* timestamp_char = (char*) malloc(sizeof(char)*11);
		while(registros[i] != ';'){
			timestamp_char[l] = registros[i];
			i++;
			l++;
		}
		timestamp_char[l]= '\0';
		long timestamp = atol(timestamp_char);
		free(timestamp_char);
		i++;
		l=0;


		char* key_char = (char*) malloc(sizeof(char));
		while(registros[i] != ';'){
			key_char[l] = registros[i];
			key_char = (char*) realloc(key_char,sizeof(char));
			i++;
			l++;
		}
		key_char[l]='\0';
		uint16_t key = (uint16_t) atol(key_char);
		free(key_char);
		i++;
		l=0;


		char* value= (char*) malloc(sizeof(char));
		while(registros[i] != '\n'){
			value[l] = registros[i];
			value = (char*) realloc(value,sizeof(char));
			i++;
			l++;
		}
		value[l]='\0';

		t_registro* registro_nuevo = crear_registro(value, key, timestamp);
		list_add(registros_finales, registro_nuevo);
	}

	return registros_finales;
}*/

t_list* filtrar_registros_duplicados_segun_particiones(char* path_tabla, t_list* registros_nuevos){
	t_list* registros_por_particiones = leer_registros_particiones(path_tabla);
	int num_particiones = list_size(registros_por_particiones);

	t_dictionary* particiones_tocadas = dictionary_create();

	while(!list_is_empty(registros_nuevos)){
		t_registro* un_registro = list_remove(registros_nuevos, 0);

		int particion = un_registro->key % num_particiones;
		char* particion_string = (char*)string_itoa(particion);
		dictionary_put(particiones_tocadas, particion_string , (bool*) true);
		actualizar_registro(list_get(registros_por_particiones,particion), un_registro); //PRECAUCION: Capaz list_get no funciona, necesitaria que trabaje con el puntero de la lista
		free(particion_string);
	}

	vaciar_datos_de_listas_no_tocadas(registros_por_particiones, particiones_tocadas);
	dictionary_destroy(particiones_tocadas);
	log_info(logger_compactacion, "Se obtienen registros filtrados para: %s.", path_tabla);
	return registros_por_particiones;
}

void vaciar_datos_de_listas_no_tocadas(t_list* registros_particiones, t_dictionary* particiones_tocadas){
	for(int i= 0; list_get(registros_particiones, i)!= NULL; i++ ){
		char* num_particion_string= string_itoa(i);
		if(dictionary_get(particiones_tocadas, num_particion_string )){
		}else{
			t_list* registros_no_tocados = list_replace(registros_particiones, i, list_create());
			list_destroy_and_destroy_elements(registros_no_tocados,(void*) eliminar_registro);
		}
		free(num_particion_string);
	}

}

void actualizar_registro(t_list* registros_de_particion, t_registro* un_registro){

	int _es_registro_con_key(t_registro* registro){
		return registro->key== un_registro->key;
	}

	t_registro* registro_viejo = list_find(registros_de_particion, (void*) _es_registro_con_key);

	if(registro_viejo == NULL){
		list_add(registros_de_particion, un_registro);
	}else{
		if(registro_viejo->timestamp <= un_registro->timestamp){
			list_remove_by_condition(registros_de_particion, (void*) _es_registro_con_key);
			eliminar_registro(registro_viejo);
			list_add(registros_de_particion, un_registro);
		}else{
			eliminar_registro(un_registro);
		}
	}

}
void liberar_bloques_compactacion(t_list* bloques_a_liberar){

	//pthread_mutex_lock(&mutexBloques);

	for (int i = 0; i< list_size(bloques_a_liberar);i++){
		int bloque_a_liberar = list_get(bloques_a_liberar, i);
		log_info(logger_compactacion, "Desbloqueo de bloque por Compactacion: %i", bloque_a_liberar);
		liberar_bloque(bloque_a_liberar);
	}
	//pthread_mutex_unlock(&mutexBloques);
}

bool pertenece_a_lista_particiones(t_list* particiones_a_liberar,char* nombre_archivo){
	char ** nombre_y_extension = string_split(nombre_archivo, ".");
	if (nombre_y_extension[0]== NULL){
		free(nombre_y_extension);
		return false;
	}else{
		int particion = atoi(nombre_y_extension[0]);
		free(nombre_y_extension[0]);
		free(nombre_y_extension[1]);
		free(nombre_y_extension);
		bool _es_igual(int elemento_lista){
			return particion == elemento_lista;
		}

		return list_any_satisfy(particiones_a_liberar, _es_igual);
	}
}

void realizar_compactacion(char* path_tabla, t_list* registros_por_particion){



	//t_list* particiones_a_liberar = encontrar_particiones_tocadas(registros_por_particion);
	//log_info(logger_compactacion, "Cantidad de particiones a liberar por particion porque fueron modificadas en %s: %d", path_tabla, list_size(particiones_a_liberar));
	//borra todos tempc y .bin para una tabla
	t_list* bloques_ocupados = eliminar_temp_y_bin_tabla(path_tabla, registros_por_particion);

	liberar_bloques_compactacion(bloques_ocupados);

	for(int i=0 ; !list_is_empty(registros_por_particion); i++){
		t_list* registros_de_particion = list_remove(registros_por_particion, 0);

		if(!list_is_empty(registros_de_particion)){
			char* path_archivo = string_from_format("%s/%d.bin", path_tabla, i);

			//pthread_mutex_lock(&mutexBloques);
			escribir_registros_y_crear_archivo(logger_compactacion, registros_de_particion, path_archivo);
			//pthread_mutex_unlock(&mutexBloques);
			free(path_archivo);
		}

		list_destroy(registros_de_particion);
	}
	list_destroy(bloques_ocupados);
	//list_destroy(particiones_a_liberar);
}

t_list* encontrar_particiones_tocadas(t_list* registros_filtrados){
	t_list* listas_tocadas =  list_create();
	int index= 0;

	void no_es_vacia(t_list* elemento){
		if(!list_is_empty(elemento)){

			list_add(listas_tocadas, index);
		}
		index++;
	}
	list_iterate(registros_filtrados, (void*) no_es_vacia);
	return listas_tocadas;
}

bool es_particion_a_modificar(t_list* registros_por_particion_a_modificar, char* nombre_particion){
	char ** nombre_y_extension = string_split(nombre_particion, ".");
	if (nombre_y_extension[0]== NULL){
		free(nombre_y_extension);
		return false;
	}else{
		int particion = atoi(nombre_y_extension[0]);
		free(nombre_y_extension[0]);
		free(nombre_y_extension[1]);
		free(nombre_y_extension);
		return !(list_is_empty(list_get(registros_por_particion_a_modificar, particion)));
	}
}

t_list* eliminar_temp_y_bin_tabla(char* path_tabla, t_list* registros_por_particion_a_modificar){

	//pthread_mutex_lock(&mutexBloques);
	t_list* bloques_ocupados = list_create();
	DIR * dir = opendir(path_tabla);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && strcmp(entry->d_name, "Metadata")!=0  && !archivo_es_del_tipo(entry->d_name, "temp")) {
			//if((archivo_es_del_tipo(entry->d_name, "tempc") || pertenece_a_lista_particiones(particiones_a_liberar,entry->d_name))){
			if((archivo_es_del_tipo(entry->d_name, "tempc") || es_particion_a_modificar(registros_por_particion_a_modificar, entry->d_name))){

				char* dir_archivo = string_from_format("%s/%s", path_tabla, entry->d_name);
				t_list* bloques_num = obtener_num_bloques_de_archivo(dir_archivo);
				list_add_all(bloques_ocupados, bloques_num);

				list_destroy(bloques_num);
				//liberar_bloques_compactacion(path_tabla, particiones_a_liberar);
				if (unlink(dir_archivo) == 0)
					log_info(logger_compactacion, "Eliminado archivo: %s\n", entry->d_name);
				else
					log_info(logger_compactacion, "No se puede eliminar archivo: %s\n", entry->d_name);
				free(dir_archivo);
			}
		}
		entry = readdir(dir);
	}
	closedir(dir);
	return bloques_ocupados;
	//pthread_mutex_unlock(&mutexBloques);
}
