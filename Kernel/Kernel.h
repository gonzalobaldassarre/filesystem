/*
 * Kernel.c
 *
 *  Created on: 7 abr. 2019
 *      Author: LOS DINOS
 */

#ifndef KERNEL_H_
#define KERNEL_H_

#include <pthread.h>
#include <readline/readline.h>
#include <global/parser.h>
#include <global/utils.h>
#include <global/protocolos.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/collections/queue.h>
#include <stdint.h>
#include <time.h>
#include <semaphore.h>
#include <math.h>
#include <sys/inotify.h>
							// ******* TIPOS NECESARIOS ******* //
t_log* logger;
t_log* log_metrics;
t_log* log_plani;
t_log* log_consultas;
t_config* archivoconfig;
t_list* tablaGossiping;
int retardo_gossiping;
int CANT_EXEC;
int CANT_METRICS;
pthread_mutex_t memorias_disponibles_mutex;
pthread_mutex_t strong_consistency_mutex;
pthread_mutex_t strong_hash_consistency_mutex;
pthread_mutex_t eventual_consistency_mutex;
pthread_mutex_t exec_queue_mutex;
pthread_mutex_t ready_queue_mutex;
sem_t exec_queue_consumer;
sem_t new_queue_consumer;
sem_t ready_queue_consumer;
char* ARCHIVO_CONFIG;
char* IP_MEMORIA;
char* PUERTO_MEMORIA;


typedef char* t_valor;					// VALOR QUE DEVUELVE EL SELECT(TODAVIA NO SABEMOS QUE ALMACENA EN TABLAS?)



typedef struct memoria{
	uint16_t numero_memoria;
	char* ip;
	char* puerto;
} t_memoria ;

typedef struct script{
	int id;
	char* path;
	long int offset;
	int error;
} t_script ;

typedef struct consistencia_tabla{
	char* nombre_tabla;
	t_consistencia consistencia;
}t_consistencia_tabla;

t_list* memorias_disponibles;

t_list* strong_consistency;
t_list* eventual_consistency;
t_list* strong_hash_consistency;
t_list* tablas_con_consistencias;

t_list* metricas;

int SLEEP_EJECUCION;




						// ******* FIN VARIABLES NECESARIAS ******* //


							// ******* API KERNEL ******* //
void resolver_add (t_instruccion_lql instruccion);
void resolver_run(t_instruccion_lql instruccion);
void resolver_create(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void resolver_describe(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void resolver_describe_drop(t_instruccion_lql instruccion, t_operacion operacion, t_script* script_en_ejecucion);
void resolver_insert(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void resolver_select(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void resolver_metrics();
void resolver_journal();

void resolver_create_script(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void resolver_insert_script(t_instruccion_lql instruccion, t_script* script_en_ejecucion);

int insert(char* tabla, uint16_t key, long timestamp); 	// INSERT PROTOTIPO (1)
t_valor select_(char* tabla, uint16_t key); 			// SELECT PROTOTIPO (2)
int drop(char* tabla);									// DROP PROTOTIPO	(3)
int create(char* tabla, t_consistencia consistencia, int maximo_particiones, long tiempo_compactacion); // CREATE PROTOTIPO (4)
t_metadata describe(char* tabla); 						// DESCRIBE PROTOTIPO (5)
int journal(void);										// JOURNAL PROTOTIPO(6)
int run(FILE* archivo);									// RUN PROTOTIPO	(7)
int add(int memoria, t_consistencia consistencia);		// ADD PROTOTIPO	(8)

							// ******* FIN API KERNEL ******* //



							// ******* DEFINICION DE FUNCIONES A UTILIZAR ******* //
void chequearSocket(int socketin);
void iniciar_logger(void);
t_log* crear_log(char* path, int debe_ser_activo_en_consola);
void leer_config(void);
void leer_atributos_config();
void terminar_programa(int conexion);
int generarID();
int asignar_consistencia(t_memoria* memoria, t_consistencia consistencia);
char* tipo_consistencia(t_consistencia consistencia);
char leer_archivo(FILE* archivo, t_script* script_en_ejecucion);
void ejecutar_instruccion(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void ejecutar_instruccion_script(t_instruccion_lql instruccion, t_script* script_en_ejecucion);
void parsear_y_ejecutar(char* linea, int flag_de_consola);
void parsear_y_ejecutar_script(char* linea, int flag_de_consola, t_script* script_en_ejecucion);
void* iniciar_peticion_tablas(void* tablaGossiping);
void iniciarHiloGossiping(t_list* tablaGossiping);
void ejecutar_script(t_script* script_a_ejecutar);
t_memoria* conseguir_memoria(char* nombre_tabla, uint16_t key);
void recibir_tabla_de_gossiping(int socket);
void eliminar_t_memoria(t_memoria* memoria);
void guardar_consistencia_tabla(char* nombre_tabla, t_consistencia consistencia);
t_consistencia_tabla* conseguir_tabla(char* nombre_tabla);
t_memoria* obtener_memoria_segun_consistencia(t_consistencia consistencia, uint16_t key);
int funcion_hash_magica(uint16_t ki);
int get_random(int maximo);
uint16_t convertir_string_a_int(char* string);

void iniciar_hilo_inotify();
void iniciar_inotify();
void cambiar_nodos_viejos_por_nuevos();
void revisar_y_cambiar_en(t_list* lista);
void revisa_y_cambia_si_encuentra(t_memoria* nodo_viejo, t_list* lista, int indice);
t_memoria* crear_nuevo_nodo_memoria(t_memoria* memoria);
void destruir_elementos(t_list* lista);
int socket_memoria_principal();


//				***** REVISAR COMO CREAR LAS CONEXIONES *****					//


					// ******* FIN DEFINICION DE FUNCIONES A UTILIZAR ******* //

#endif /* TP0_H_ */

