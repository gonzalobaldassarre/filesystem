/*
 * protocolos.c
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */


#include "protocolos.h"

int get_tamanio_paquete_status(t_status_solicitud* paquete){
	return sizeof(bool)+ paquete->mensaje->size+ sizeof(int);
}

int get_tamanio_paquete_select(t_paquete_select* paquete_select){
	return paquete_select->nombre_tabla->size + sizeof(uint16_t)+ sizeof(t_operacion)+sizeof(int);
}

int get_tamanio_paquete_create(t_paquete_create* paquete_create){
	return paquete_create->nombre_tabla->size + sizeof(uint16_t)+ sizeof(t_operacion)+sizeof(int)+sizeof(t_consistencia)+sizeof(long);
}

int get_tamanio_paquete_insert(t_paquete_insert* paquete_insert){
	return paquete_insert->nombre_tabla->size+ paquete_insert->valor->size+ 2*sizeof(int)+ sizeof(t_operacion)+sizeof(uint16_t)+sizeof(long);
}

int get_tamanio_paquete_add(t_paquete_add* paquete_add){
	return sizeof(t_operacion) +sizeof(uint16_t) + sizeof(t_consistencia);
}

int get_tamanio_paquete_describe_drop(t_paquete_drop_describe* paquete_drop_describe){
	return sizeof(t_operacion) + sizeof(int) + paquete_drop_describe->nombre_tabla->size;
}

int get_tamanio_paquete_metadata(t_metadata* paquete_metadata) {
	return paquete_metadata->nombre_tabla->size + sizeof(t_consistencia) + 2*sizeof(int)+sizeof(long);
}

//Creacion de paquetes
t_paquete_add* crear_paquete_add(t_instruccion_lql instruccion){
	t_paquete_add* paquete_add = malloc(sizeof(t_paquete_add));
	paquete_add->codigo_operacion = instruccion.operacion;
	paquete_add->num_memoria = instruccion.parametros.ADD.numero_memoria;
	paquete_add->consistencia = instruccion.parametros.ADD.consistencia;
	return paquete_add;
}

t_paquete_select* crear_paquete_select(t_instruccion_lql instruccion){
	t_paquete_select* paquete_select = malloc(sizeof(t_paquete_select));
	paquete_select->codigo_operacion = instruccion.operacion;
	paquete_select->key = instruccion.parametros.SELECT.key;
	char* nombre_tabla = instruccion.parametros.SELECT.tabla;
	paquete_select->nombre_tabla = malloc(sizeof(t_buffer));
	paquete_select->nombre_tabla->size = string_size(nombre_tabla);
	paquete_select->nombre_tabla->palabra = malloc(paquete_select->nombre_tabla->size);
	memcpy(paquete_select->nombre_tabla->palabra, nombre_tabla, paquete_select->nombre_tabla->size);


	return paquete_select;
}

t_paquete_create* crear_paquete_create(t_instruccion_lql instruccion) {
	t_paquete_create* paquete_create = malloc(sizeof(t_paquete_create));
	paquete_create->codigo_operacion = instruccion.operacion;
	paquete_create->consistencia = instruccion.parametros.CREATE.consistencia;
	paquete_create->num_particiones = instruccion.parametros.CREATE.num_particiones;
	paquete_create->tiempo_compac = instruccion.parametros.CREATE.compactacion_time;
	char* nombre_tabla = instruccion.parametros.CREATE.tabla;
	paquete_create->nombre_tabla = malloc(sizeof(t_buffer));
	paquete_create->nombre_tabla->size = string_size(nombre_tabla);
	paquete_create->nombre_tabla->palabra = malloc(paquete_create->nombre_tabla->size);
	memcpy(paquete_create->nombre_tabla->palabra, nombre_tabla, paquete_create->nombre_tabla->size);
	return paquete_create;
}

t_paquete_insert* crear_paquete_insert(t_instruccion_lql instruccion){
	t_paquete_insert* paquete_insert = malloc(sizeof(t_paquete_insert));
	paquete_insert->codigo_operacion = instruccion.operacion;

	paquete_insert->nombre_tabla = malloc(sizeof(t_buffer));
	char* nombre_tabla = instruccion.parametros.INSERT.tabla;
	paquete_insert->nombre_tabla->size = string_size(nombre_tabla);
	paquete_insert->nombre_tabla->palabra = malloc(paquete_insert->nombre_tabla->size);
	memcpy(paquete_insert->nombre_tabla->palabra, nombre_tabla, paquete_insert->nombre_tabla->size);

	paquete_insert->key = instruccion.parametros.INSERT.key;

	paquete_insert->valor = malloc(sizeof(t_buffer));
	char* valor_ingresar = instruccion.parametros.INSERT.value;
	paquete_insert->valor->size = string_size(valor_ingresar);
	paquete_insert->valor->palabra = malloc(paquete_insert->valor->size);
	memcpy(paquete_insert->valor->palabra, valor_ingresar, paquete_insert->valor->size);

	paquete_insert->timestamp=instruccion.parametros.INSERT.timestamp;
	return paquete_insert;
}

t_paquete_drop_describe* crear_paquete_drop_describe(t_instruccion_lql instruccion){
	t_paquete_drop_describe* paquete = malloc(sizeof(t_paquete_drop_describe));
	paquete->codigo_operacion = instruccion.operacion;
	paquete->nombre_tabla = malloc(sizeof(t_buffer));
	char* nombre_tabla = instruccion.parametros.DROP.tabla;
	paquete->nombre_tabla->size = string_size(nombre_tabla);
	paquete->nombre_tabla->palabra = malloc(paquete->nombre_tabla->size);
	memcpy(paquete->nombre_tabla->palabra, nombre_tabla, paquete->nombre_tabla->size);

	return paquete;
}

t_status_solicitud* crear_paquete_status(bool es_valido, char* mensaje ){
	t_status_solicitud* paquete = malloc(sizeof(t_status_solicitud));
	paquete->es_valido=es_valido;
	paquete->mensaje = malloc(sizeof(t_buffer));
	paquete->mensaje->size = string_size(mensaje);
	paquete->mensaje->palabra = malloc(paquete->mensaje->size);
	memcpy(paquete->mensaje->palabra, mensaje, paquete->mensaje->size);

	return paquete;
}

t_metadata* crear_paquete_metadata(char*nombre_tabla, t_consistencia consistencia, int particiones, long compactacion){
	t_metadata* metadata = malloc(sizeof(t_metadata));
	metadata->consistencia = consistencia;
	metadata->n_particiones= particiones;
	metadata->tiempo_compactacion=compactacion;
	metadata->nombre_tabla = malloc(sizeof(t_buffer));
	metadata->nombre_tabla->size = string_size(nombre_tabla);
	metadata->nombre_tabla->palabra = malloc(metadata->nombre_tabla->size);
	memcpy(metadata->nombre_tabla->palabra, nombre_tabla, metadata->nombre_tabla->size);
	return metadata;
}


//Serializacion de paquetes
char* serializar_paquete_select(t_paquete_select* paquete_select, int bytes)
{
	char * serializado = malloc(bytes);
	int desplazamiento = 0;

	memcpy(serializado + desplazamiento, &(paquete_select->codigo_operacion), sizeof(t_operacion));
	desplazamiento+= sizeof(t_operacion);
	memcpy(serializado + desplazamiento, &(paquete_select->nombre_tabla->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete_select->nombre_tabla->palabra, paquete_select->nombre_tabla->size);
	desplazamiento+= paquete_select->nombre_tabla->size;
	memcpy(serializado + desplazamiento, &(paquete_select->key), sizeof(uint16_t));
		desplazamiento+= sizeof(uint16_t);
	return serializado;
}

char* serializar_paquete_create(t_paquete_create* paquete_create, int bytes){
	char * serializado = malloc(bytes);
	int desplazamiento = 0;

	memcpy(serializado + desplazamiento, &(paquete_create->codigo_operacion), sizeof(t_operacion));
	desplazamiento+= sizeof(t_operacion);
	memcpy(serializado + desplazamiento, &(paquete_create->nombre_tabla->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete_create->nombre_tabla->palabra, paquete_create->nombre_tabla->size);
	desplazamiento+= paquete_create->nombre_tabla->size;
	memcpy(serializado + desplazamiento, &(paquete_create->consistencia), sizeof(t_consistencia));
	desplazamiento+= sizeof(t_consistencia);
	memcpy(serializado + desplazamiento, &(paquete_create->num_particiones), sizeof(uint16_t));
	desplazamiento+= sizeof(uint16_t);
	memcpy(serializado + desplazamiento, &(paquete_create->tiempo_compac), sizeof(long));
	desplazamiento+= sizeof(long);
	return serializado;
}

char* serializar_paquete_insert(t_paquete_insert* paquete_insert, int bytes){
	char* serializado = malloc(bytes);
	int desplazamiento = 0;
	memcpy(serializado + desplazamiento, &(paquete_insert->codigo_operacion), sizeof(t_operacion));
	desplazamiento+= sizeof(t_operacion);
	memcpy(serializado + desplazamiento, &(paquete_insert->nombre_tabla->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete_insert->nombre_tabla->palabra, paquete_insert->nombre_tabla->size);
	desplazamiento+= paquete_insert->nombre_tabla->size;
	memcpy(serializado + desplazamiento, &(paquete_insert->key), sizeof(uint16_t));
	desplazamiento+= sizeof(uint16_t);
	memcpy(serializado + desplazamiento, &(paquete_insert->valor->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete_insert->valor->palabra, paquete_insert->valor->size);
	desplazamiento+= paquete_insert->valor->size;
	memcpy(serializado + desplazamiento, &(paquete_insert->timestamp), sizeof(long));
	desplazamiento+= sizeof(long);

	return serializado;
}

char* serializar_paquete_add(t_paquete_add* paquete_add, int bytes){
	char* serializado = malloc(bytes);
	int desplazamiento = 0;
	memcpy(serializado + desplazamiento, &(paquete_add->codigo_operacion), sizeof(t_operacion));
	desplazamiento+= sizeof(t_operacion);
	memcpy(serializado + desplazamiento, &(paquete_add->num_memoria), sizeof(uint16_t));
	desplazamiento+= sizeof(uint16_t);
	memcpy(serializado + desplazamiento, &(paquete_add->consistencia), sizeof(t_consistencia));
	desplazamiento+= sizeof(t_consistencia);
	return serializado;
}

char* serialiazar_paquete_drop_describe(t_paquete_drop_describe* paquete, int bytes){
	char* serializado = malloc(bytes);
	int desplazamiento = 0;
	memcpy(serializado + desplazamiento, &(paquete->codigo_operacion), sizeof(t_operacion));
	desplazamiento+= sizeof(t_operacion);
	memcpy(serializado + desplazamiento, &(paquete->nombre_tabla->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete->nombre_tabla->palabra, paquete->nombre_tabla->size);
	desplazamiento+= paquete->nombre_tabla->size;
	return serializado;
}

char* serializar_status_solicitud(t_status_solicitud* paquete, int bytes){

	char* serializado = malloc(bytes);
	int desplazamiento = 0;
	memcpy(serializado + desplazamiento, &(paquete->es_valido), sizeof(bool));
	desplazamiento+= sizeof(bool);
	memcpy(serializado + desplazamiento, &(paquete->mensaje->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(serializado + desplazamiento, paquete->mensaje->palabra, paquete->mensaje->size);
	desplazamiento+= paquete->mensaje->size;
	return serializado;
}

char* serializar_metadata(t_metadata* metadata, int bytes){

	char* serializado = malloc(bytes);
	int desplazamiento = 0;
	memcpy(serializado + desplazamiento, &(metadata->consistencia), sizeof(t_consistencia));
	desplazamiento += sizeof(t_consistencia);
	memcpy(serializado + desplazamiento, &(metadata->n_particiones), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(serializado + desplazamiento, &(metadata->tiempo_compactacion), sizeof(long));
	desplazamiento += sizeof(long);
	memcpy(serializado + desplazamiento, &(metadata->nombre_tabla->size), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(serializado + desplazamiento, metadata->nombre_tabla->palabra, metadata->nombre_tabla->size);
	desplazamiento += metadata->nombre_tabla->size;

	return serializado;
}
char* serializar_tabla_gossiping(t_list* tablag){
	char* tablaSerializada=malloc((sizeof(int)*2)*tablag->elements_count);
	int desplazamiento=0;
	for(int i=0;i<tablag->elements_count;i++){
		t_gossip* unelem = list_get(tablag,i);
		memcpy(tablaSerializada,&(unelem->ip_memoria),sizeof(int));
		desplazamiento+=sizeof(int);
		memcpy(tablaSerializada+desplazamiento,&(unelem->puerto_memoria),sizeof(int));
	}
	return tablaSerializada;
}
char* generar_registro_string(long timestamp, uint16_t key, char* value){

	char* time_to_string=string_itoa(timestamp);
	char* key_to_string= string_itoa(key);
	char *resultado = string_new();
	string_append(&resultado, time_to_string);
	string_append(&resultado, ";");
	string_append(&resultado, key_to_string);
	string_append(&resultado, ";");
	string_append(&resultado, value);
	free(time_to_string);
	free(key_to_string);
	return resultado;
}

t_registro* obtener_registro(char* registro_serealizado){
	char** split = string_split(registro_serealizado,";");
	t_registro* registro = malloc(sizeof(t_registro));
	registro->timestamp =(long) atol(split[0]);
	registro->key =(uint16_t) atol(split[1]);
	registro->value = malloc(string_size(split[2]));
	strcpy(registro->value ,split[2]);//, string_size(&(registro->value)));
	free(split[0]);
	free(split[1]);
	free(split[2]);
	free(split);
	return registro;
}

//Desearializacion de paquetes

t_paquete_select* deserializar_select (int socket_cliente){
	t_paquete_select* consulta_select = malloc(sizeof(t_paquete_select));
	consulta_select->codigo_operacion = SELECT;
	consulta_select->nombre_tabla = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(consulta_select->nombre_tabla->size), sizeof(int), MSG_WAITALL);
	int size_nombre_tabla = consulta_select->nombre_tabla->size;
	char * nombre_tabla = malloc(size_nombre_tabla);
	consulta_select->nombre_tabla->palabra = malloc(consulta_select->nombre_tabla->size);
	recv(socket_cliente, nombre_tabla, size_nombre_tabla, MSG_WAITALL);
	memcpy(consulta_select->nombre_tabla->palabra ,nombre_tabla, size_nombre_tabla );
	recv(socket_cliente,  &(consulta_select->key), sizeof(uint16_t), MSG_WAITALL);
	free(nombre_tabla);
	return consulta_select;//Acordarse de hacer un free despues de usarse
}

t_paquete_create* deserializar_create (int socket_cliente){
	t_paquete_create* consulta_create = malloc(sizeof(t_paquete_create));
	consulta_create->codigo_operacion=CREATE;
	consulta_create->nombre_tabla = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(consulta_create->nombre_tabla->size), sizeof(int), MSG_WAITALL);
	int size_nombre_tabla = consulta_create->nombre_tabla->size;
	char * nombre_tabla = malloc(size_nombre_tabla);
	consulta_create->nombre_tabla->palabra = malloc(consulta_create->nombre_tabla->size);
	recv(socket_cliente, nombre_tabla, size_nombre_tabla, MSG_WAITALL);
	memcpy(consulta_create->nombre_tabla->palabra ,nombre_tabla, size_nombre_tabla );
	recv(socket_cliente,  &(consulta_create->consistencia), sizeof(t_consistencia), MSG_WAITALL);
	recv(socket_cliente,  &(consulta_create->num_particiones), sizeof(uint16_t), MSG_WAITALL);
	recv(socket_cliente,  &(consulta_create->tiempo_compac), sizeof(long), MSG_WAITALL);
	free(nombre_tabla);

	return consulta_create;//Acordarse de hacer un free despues de usarse
}

t_paquete_insert* deserealizar_insert(int socket_cliente) {
	t_paquete_insert* consulta_insert = malloc(sizeof(t_paquete_insert));
	consulta_insert->codigo_operacion = INSERT;

	consulta_insert->nombre_tabla = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(consulta_insert->nombre_tabla->size), sizeof(int), MSG_WAITALL);
	int size_nombre_tabla = consulta_insert->nombre_tabla->size;
	char * nombre_tabla = malloc(size_nombre_tabla);
	consulta_insert->nombre_tabla->palabra = malloc(consulta_insert->nombre_tabla->size);
	recv(socket_cliente, nombre_tabla, size_nombre_tabla, MSG_WAITALL);
	memcpy(consulta_insert->nombre_tabla->palabra ,nombre_tabla, size_nombre_tabla );

	recv(socket_cliente, &(consulta_insert->key), sizeof(uint16_t), MSG_WAITALL);

	consulta_insert->valor = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(consulta_insert->valor->size), sizeof(int), MSG_WAITALL);
	int size_valor = consulta_insert->valor->size;
	char * valor = malloc(size_valor);
	consulta_insert->valor->palabra = malloc(consulta_insert->valor->size);
	recv(socket_cliente, valor, size_valor, MSG_WAITALL);
	memcpy(consulta_insert->valor->palabra ,valor, size_valor );

	recv(socket_cliente,  &(consulta_insert->timestamp), sizeof(long), MSG_WAITALL);
	free(nombre_tabla);
	free(valor);
	return consulta_insert;//Acordarse de hacer un free despues de usarse
}

t_paquete_add* desearilizar_add(int socket_cliente){
	t_paquete_add* consulta_add = malloc(sizeof(t_paquete_add));
	consulta_add->codigo_operacion = ADD;
	recv(socket_cliente,  &(consulta_add->num_memoria), sizeof(uint16_t), MSG_WAITALL);
	recv(socket_cliente,  &(consulta_add->consistencia), sizeof(t_consistencia), MSG_WAITALL);

	return consulta_add;//Acordarse de hacer un free despues de usarse
}

t_status_solicitud* desearilizar_status_solicitud(int socket_cliente){
	t_status_solicitud* status = malloc(sizeof(t_status_solicitud));
	recv(socket_cliente,  &(status->es_valido), sizeof(bool), MSG_WAITALL);
	status->mensaje=malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(status->mensaje->size), sizeof(int), MSG_WAITALL);
	int size_mensaje = status->mensaje->size;
	char * mensaje = malloc(size_mensaje);
	status->mensaje->palabra = malloc(status->mensaje->size);
	recv(socket_cliente, mensaje, size_mensaje, MSG_WAITALL);
	memcpy(status->mensaje->palabra ,mensaje, size_mensaje );
	free(mensaje);
	return status;//Acordarse de hacer un free despues de usarse
}


t_paquete_drop_describe* deserealizar_drop_describe(int socket_cliente) {
	t_paquete_drop_describe* consulta = malloc(sizeof(t_paquete_drop_describe));
	consulta->nombre_tabla = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(consulta->nombre_tabla->size), sizeof(int), MSG_WAITALL);
	int size_nombre_tabla = consulta->nombre_tabla->size;
	char * nombre_tabla = malloc(size_nombre_tabla);
	consulta->nombre_tabla->palabra = malloc(consulta->nombre_tabla->size);
	recv(socket_cliente, nombre_tabla, size_nombre_tabla, MSG_WAITALL);
	memcpy(consulta->nombre_tabla->palabra ,nombre_tabla, size_nombre_tabla );
	free(nombre_tabla);
	return consulta;//Acordarse de hacer un free despues de usarse
}

t_metadata* deserealizar_metadata(int socket_cliente){
	t_metadata* metadata = malloc(sizeof(t_metadata));
	recv(socket_cliente, &(metadata->consistencia), sizeof(t_consistencia), MSG_WAITALL);
	recv(socket_cliente, &(metadata->n_particiones), sizeof(int), MSG_WAITALL);
	recv(socket_cliente, &(metadata->tiempo_compactacion), sizeof(long), MSG_WAITALL);
	metadata->nombre_tabla = malloc(sizeof(t_buffer));
	recv(socket_cliente,  &(metadata->nombre_tabla->size), sizeof(int), MSG_WAITALL);
	int size_nombre_tabla = metadata->nombre_tabla->size;
	char * nombre_tabla = malloc(size_nombre_tabla);
	metadata->nombre_tabla->palabra = malloc(metadata->nombre_tabla->size);
	recv(socket_cliente, nombre_tabla, size_nombre_tabla, MSG_WAITALL);
	memcpy(metadata->nombre_tabla->palabra ,nombre_tabla, size_nombre_tabla );
	free(nombre_tabla);
	return metadata;
}

t_consistencia get_valor_consistencia(char* consistencia_ingresada){

	t_consistencia result;
	//if(string_equals_ignore_case(consistencia_ingresada,STRONG_HASH_TEXT)){
	if(strcmp(consistencia_ingresada, STRONG_HASH_TEXT) == 0){
		result= STRONG_HASH;
	}
	if(strcmp(consistencia_ingresada, STRONG_TEXT) == 0){
		result = STRONG;
	}
	if(strcmp(consistencia_ingresada, EVENTUAL_TEXT) == 0){
		result= EVENTUAL;
	}
	return result;
}


/*struct tablaMemoriaGossip crearTablaGossip(){
	struct tablaMemoriaGossip* elementoCreado = malloc(sizeof(struct tablaMemoriaGossip));
	//-- CREAR PRIMER ELEMENTO(MEMORIA A, LA PRIEMRA QUE SE LEVANTA, HAY QUE PASAR POR PARAMETROS LOS DATOS DE LA MISMA--//
	return *elementoCreado;
}*/




int recibir_numero_de_tablas(int socket){
	int cant_tablas = 0;
	recv(socket, &cant_tablas, sizeof(int), MSG_WAITALL);
	return cant_tablas;
}

void enviar_numero_de_tablas(int socket, int cant_tablas){
	send(socket, &cant_tablas, sizeof(int), MSG_WAITALL);
}

