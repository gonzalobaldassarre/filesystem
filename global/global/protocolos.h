/*
 * protocolos.h
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

#ifndef GLOBAL_PROTOCOLOS_H_
#define GLOBAL_PROTOCOLOS_H_
#include <commons/collections/list.h>
#include<stdint.h>
#include<sys/socket.h>
#include<stdio.h>
#include<stdlib.h>
#include<signal.h>
#include<unistd.h>
#include <string.h>
#include <stdbool.h>
#include "strings.h"


#define STRONG_TEXT "SC"
#define STRONG_HASH_TEXT "SHC"
#define EVENTUAL_TEXT "EC"

typedef struct{
	char* nombre;
	t_list* registros;
}t_cache_tabla;
typedef struct
{
	int size;
	char* palabra;
} t_buffer;

typedef enum consistencias{
	STRONG, STRONG_HASH, EVENTUAL
}t_consistencia;

typedef struct{
	t_buffer* nombre_tabla;
	t_consistencia consistencia;
	int n_particiones;
	long tiempo_compactacion;
}t_metadata;


typedef struct {
	long timestamp;
	char* value;
	uint16_t key;
}t_registro;

typedef struct {
	char* memoria;
	t_list* tabla;
} datosSelect ;
typedef enum estado {
	CONECTADA, DESCONECTADA
}t_estado;


t_list asdasd;

typedef enum operaciones {
	INSERT,SELECT,CREATE,DESCRIBE,DROP, JOURNAL,ADD,METRICS,RUN, HANDSHAKE,GOSSPING,SOLICITUD_TABLA_GOSSIPING,EXIT
}t_operacion;


typedef struct
{
	t_operacion codigo_operacion;
	t_buffer* nombre_tabla;
	uint16_t key;
	t_buffer* valor;
	long timestamp;

} t_paquete_insert;

typedef struct
{
	char* ip_memoria;
	char* puerto_memoria;
	char* nombre_memoria;
}t_gossip;

typedef struct
{
	t_operacion codigo_operacion;
	t_buffer* nombre_tabla;
	t_consistencia consistencia;
	uint16_t num_particiones;
	long tiempo_compac;

} t_paquete_create;

typedef struct
{
	t_operacion codigo_operacion;
	uint16_t num_memoria;
	t_consistencia consistencia;

} t_paquete_add;

typedef struct
{
	t_operacion codigo_operacion;
	t_buffer* nombre_tabla;
	uint16_t key;

} t_paquete_select;



typedef struct
{
	t_operacion codigo_operacion;
	t_buffer* nombre_tabla;

} t_paquete_drop_describe;

typedef struct {
	bool es_valido;
	t_buffer* mensaje;
}t_status_solicitud;

typedef struct {
	bool valido;
	t_operacion operacion;
	union {
		struct {
			char* tabla;
			long timestamp;
			uint16_t key;
			char* value;
			char** split_campos;
			char** split_value;
		} INSERT;
		struct {
			char* tabla;
			uint16_t key;

		} SELECT;
		struct {
			char* tabla;
			t_consistencia consistencia;
			uint16_t num_particiones;
			long compactacion_time;
		} CREATE;
		struct {
			char* tabla;
		} DESCRIBE;
		struct {
			char* tabla;
		} DROP;
		struct {
			uint16_t numero_memoria;
			t_consistencia consistencia;
		} ADD;
		struct {
			char * path_script;
		} RUN;
		struct {
			;
		} METRICS;

	} parametros;
	char** _raw; //Para uso de la liberación
} t_instruccion_lql;

typedef struct {
	t_list* paginas;
	char* nombreTabla;
} segmento;

typedef struct {
	unsigned int posicionEnMemoria;
	int modificado;
	long ultimaLectura;
} pagina; // para la lista

typedef struct {
	uint16_t key;
	long timestamp;
	char* value;
} pagina_concreta; // para la memoria

int get_tamanio_paquete_select(t_paquete_select* paquete_select);
int get_tamanio_paquete_create(t_paquete_create* paquete_create);
int get_tamanio_paquete_insert(t_paquete_insert* paquete_insert);
int get_tamanio_paquete_add(t_paquete_add* paquete_add);
int get_tamanio_paquete_describe_drop(t_paquete_drop_describe* paquete_drop_describe);
int get_tamanio_paquete_status(t_status_solicitud* paquete);
int get_tamanio_paquete_metadata(t_metadata* paquete_metadata);

t_paquete_add* crear_paquete_add(t_instruccion_lql instruccion);
t_paquete_select* crear_paquete_select(t_instruccion_lql instruccion);
t_paquete_create* create_paquete_create(t_instruccion_lql instruccion);
t_paquete_insert* crear_paquete_insert(t_instruccion_lql instruccion);
t_paquete_drop_describe* crear_paquete_drop_describe(t_instruccion_lql instruccion);
t_status_solicitud* crear_paquete_status(bool es_valido, char* mensaje );
t_paquete_create* crear_paquete_create(t_instruccion_lql instruccion);

char* serializar_paquete_select(t_paquete_select* paquete_select, int bytes);
char* serializar_paquete_create(t_paquete_create* paquete_create, int bytes);
char* serializar_paquete_insert(t_paquete_insert* paquete_insert, int bytes);
char* serializar_paquete_add(t_paquete_add* paquete_add, int bytes);
char* serialiazar_paquete_drop_describe(t_paquete_drop_describe* paquete, int bytes);
char* serializar_status_solicitud(t_status_solicitud* paquete, int bytes);
char* serializar_metadata(t_metadata* metadata, int bytes);

t_paquete_select* deserializar_select (int socket_cliente);
t_paquete_create* deserializar_create (int socket_cliente);
t_paquete_insert* deserealizar_insert(int socket_cliente);
t_paquete_add* desearilizar_add(int socket_cliente);
t_paquete_drop_describe* deserealizar_drop_describe(int socket_cliente);
t_registro* obtener_registro(char* registro_serealizado);
char* generar_registro_string(long timestamp, uint16_t key, char* value);
t_status_solicitud* desearilizar_status_solicitud(int socket_cliente);
t_metadata* deserealizar_metadata(int socket_cliente);

t_registro* obtener_registro(char* registro_serealizado);
char* generar_registro_string(long timestamp, uint16_t key, char* value);
t_metadata* crear_paquete_metadata(char*nombre_tabla, t_consistencia consistencia, int particiones, long compactacion);

int recibir_numero_de_tablas (int socket);
void enviar_numero_de_tablas(int socket, int cant_tablas);

t_consistencia get_valor_consistencia(char* consistencia_ingresada);

#endif /* GLOBAL_PROTOCOLOS_H_ */
