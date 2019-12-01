/*
 * LFS_Consola.c
 *
 *  Created on: 23 may. 2019
 *      Author: utnso
 */
#include "LFS_Consola.h"

void crear_hilo_consola(){

	if (pthread_create(&hilo_consola, 0, levantar_consola, NULL) !=0){
		log_error(logger_consola, "Error al crear el hilo");
	}
}

void *levantar_consola(){

	while(1){

		printf("Ingrese las operaciones que desea realizar. Para finalizar: EXIT \n");
		char* linea = readline("Consola LFS >");
		t_instruccion_lql instruccion = parsear_linea(linea);
		if (instruccion.valido) {
			resolver_operacion_por_consola(instruccion);
		}else{
			printf("Reingrese la instruccion\n");
		}
		free(linea);
	}
}

int resolver_operacion_por_consola(t_instruccion_lql instruccion){
	t_status_solicitud* status;
	switch(instruccion.operacion)
		{
		case SELECT:
			log_info(logger_consola, "Se solicito SELECT por consola");
//			if (tabla_esta_bloqueada(instruccion.parametros.SELECT.tabla)){
//				t_paquete_select* paquete = crear_paquete_select(instruccion) ;
//				agregar_instruccion_bloqueada(crear_instruccion_select_bloqueada( paquete, NULL), instruccion.parametros.SELECT.tabla);
//				eliminar_paquete_select(paquete);
//			}
//			else{
			resolver_select_consola(instruccion.parametros.SELECT.tabla, instruccion.parametros.SELECT.key);
			//}
			break;
		case INSERT:
			log_info(logger_consola, "Se solicitó INSERT por consola");
//			if (tabla_esta_bloqueada(instruccion.parametros.SELECT.tabla)){
//				t_paquete_insert* paquete = crear_paquete_insert(instruccion);
//				agregar_instruccion_bloqueada(crear_instruccion_insert_bloqueada(paquete, NULL), instruccion.parametros.INSERT.tabla);
//				eliminar_paquete_insert(paquete);
//			}else{
				status = resolver_insert(logger_consola,instruccion.parametros.INSERT.tabla,
																	instruccion.parametros.INSERT.key,
																	instruccion.parametros.INSERT.value,
																	instruccion.parametros.INSERT.timestamp);
				eliminar_paquete_status(status);
			//}
			break;
		case CREATE:
			log_info(logger_consola, "Se solicitó CREATE por consola");
			status = resolver_create(logger_consola, instruccion.parametros.CREATE.tabla,
					instruccion.parametros.CREATE.consistencia,
					instruccion.parametros.CREATE.num_particiones,
					instruccion.parametros.CREATE.compactacion_time);
			log_info(logger_consola, status->mensaje->palabra);
			eliminar_paquete_status(status);
			//aca debería enviarse el mensaje a LFS con CREATE
			break;
		case DESCRIBE:
			log_info(logger_consola, "Se solicitó DESCRIBE por consola");
			resolver_describe_consola(instruccion.parametros.DESCRIBE.tabla);

			//aca debería enviarse el mensaje a LFS con DESCRIBE
			break;
		case DROP:
			log_info(logger, "Se solicitó DROP por consola");
			status = resolver_drop(logger_consola, instruccion.parametros.DROP.tabla);
			eliminar_paquete_status(status);
			//aca debería enviarse el mensaje a LFS con DROP
			break;
		case EXIT:
			log_info(logger_consola, "Finalizando LFS..");
			pthread_cancel(hilo_consola);
			break;

		default:
			log_warning(logger_consola, "Operacion desconocida.");
			return EXIT_FAILURE;
		}
	return EXIT_SUCCESS;
}

void resolver_describe_consola(char* nombre_tabla){
	log_info(logger_consola, "Se realiza DESCRIBE");
	if(string_is_empty(nombre_tabla)){
		informar_todas_tablas();
	}else{
		mostrar_metadata_tabla(nombre_tabla);
	}
}

void informar_todas_tablas(){
	char* path_tablas = string_from_format("%s/Tables", path_montaje);
	DIR * dir = opendir(path_tablas);
	struct dirent * entry = readdir(dir);
	while(entry != NULL){
		if (entry->d_type == DT_DIR &&  ( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 )){
			char* nombre_tabla = entry->d_name;
			mostrar_metadata_tabla(nombre_tabla);
		}
		entry = readdir(dir);
	}
	closedir(dir);
	free(path_tablas);
}

void mostrar_metadata_tabla(char* nombre_tabla){
	log_info(logger_consola, "Tabla: %s", nombre_tabla);
	char* dir_tabla = string_from_format("%s/Tables/%s", path_montaje, nombre_tabla);
	t_metadata* metadata_tabla = obtener_info_metadata_tabla(dir_tabla, nombre_tabla);
	char*consistencia = consistencia_to_string(metadata_tabla->consistencia);
	log_info(logger_consola, "Metadata tabla: %s", metadata_tabla->nombre_tabla->palabra);
	log_info(logger_consola, "Consistencia: %s", consistencia);
	log_info(logger_consola, "Numero de particiones: %d", metadata_tabla->n_particiones);
	log_info(logger_consola, "Tiempo de compactacion: %d", metadata_tabla->tiempo_compactacion);
	eliminar_metadata(metadata_tabla);
	free(consistencia);
	free(dir_tabla);

}

void resolver_select_consola (char* nombre_tabla, uint16_t key){

	t_list* registros_encontrados = list_create();

	log_info(logger_consola, "Se realiza SELECT");
	log_info(logger_consola, "Consulta en la tabla: %s", nombre_tabla);
	log_info(logger_consola, "Consulta por key: %d", key);

	if (existe_tabla_fisica(nombre_tabla)){

		t_tabla_logica* tabla = buscar_tabla_logica_con_nombre(nombre_tabla);
		pthread_mutex_lock(&tabla->mutex_compac_select);
		pthread_mutex_lock(&tabla->mutex_select_drop);
		t_list* registros_memtable = buscar_registros_memtable(nombre_tabla, key);
		t_list* registros_temporales = buscar_registros_temporales(nombre_tabla, key);
		t_list* registros_particion = buscar_registros_en_particion(nombre_tabla, key);

		list_add_all(registros_encontrados, registros_memtable);
		list_add_all(registros_encontrados, registros_temporales);
		list_add_all(registros_encontrados, registros_particion);
		t_registro* registro_buscado = buscar_registro_actual(registros_encontrados);

		if(registro_buscado !=NULL){
			char* resultado = generar_registro_string(registro_buscado->timestamp, registro_buscado->key, registro_buscado->value);
			log_info(logger_consola, "Resultado: %s", resultado);
			free(resultado);

		}else{
			char * mje_error = string_from_format("No se encontró registro con key: %d en la tabla %s", key, nombre_tabla);
			log_error(logger_consola, mje_error);
			free(mje_error);
		}
		list_destroy(registros_memtable);
		list_destroy(registros_temporales);
		list_destroy(registros_particion);
		pthread_mutex_unlock(&tabla->mutex_select_drop);
		pthread_mutex_unlock(&tabla->mutex_compac_select);
	}else{
		char * mje_error = string_from_format("La tabla %s no existe", nombre_tabla);
		log_error(logger_consola, mje_error);
		free(mje_error);
	}
	list_destroy(registros_encontrados);
}


