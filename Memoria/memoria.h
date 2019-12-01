/*
 * memoria.h
 *
 *  Created on: 7 abr. 2019
 *      Author: utnso
 */
#ifndef MEMORIA_H_
#define MEMORIA_H_
#include <pthread.h>
#include <readline/readline.h>
#include <global/parser.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <stdint.h>
#include <time.h>
#include <global/utils.h>
#include <global/protocolos.h>
#include <sys/inotify.h>


							// ******* TIPOS NECESARIOS ******* //
// prueba select //
	 fd_set master;   // conjunto maestro de descriptores de fichero
	 fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
	 int fdmax,fdmin;        // número máximo de descriptores de fichero
	 //server_memoria  // descriptor de socket a la escucha
	 int memoriaNuevaAceptada;        // descriptor de socket de nueva conexión aceptada
//
pthread_mutex_t mutexMemoria;
void *iniciar_select(void* dato);
void iniciarHiloKernel(datosSelect* dato);
void *journaling_automatico(void* dato);
void iniciarHiloJournaling(char* memo, t_list* tablas);
int socket_memoria;
pthread_t consola;
long retardo_journaling;
long retardo_gossiping;
t_list* tablas;
char* serializar_pagina(pagina_concreta* pagina);
pagina_concreta* deserializar_pagina(char* paginac);
segmento* encontrarSegmento(char* nombredTabla, t_list* tablas);
pagina* *encontrarPagina(segmento* unSegmento, uint16_t key);
void cambiarBitModificado(char* tabla, uint16_t key,t_list* tablas, char* memoria_principal);
void modificarRegistro(uint16_t key,char* dato,long tss,int posicion, char* memoria_principal, t_list* tablas);
//void resolver_insert2(int socket_kernel_fd, int socket_conexion_lfs);
pagina_concreta* traerPaginaDeMemoria(unsigned int posicion,char* memoria_principal);
int existeTabla(char* tabla);
void agregar_pagina_a_tabla(pagina* pagina,char* nombreTabla);
pagina* crearPagina();
pagina* crearPaginaInsert();
void eliminarPagina(int posicionDondePonerElDatoEnMemoria, char* memoria_princial, t_list* tablas);
int lru(char* memoria_principal, t_list* tablas);
void journaling(char* memoria_principal,t_list* tablas);
void agregarEnMemoriaElRegistro(char* key,char* value,long timestamp);
int buscarRegistroEnTabla(char* tabla, uint16_t key, char* memoria_principal,t_list* tablas);
void traerPaginaDeMemoria2(unsigned int posicion,char* memoria_principal,pagina_concreta* pagina);
void resolver_create_consola(int scoket_conexion_lfs, t_instruccion_lql instruccion_create);
char* memoria_principal;
int nbytes;
int primeraVuelta = 0;
pthread_t thread_memoria;
t_list* tablaGossiping;
t_log* logger;
t_log* logger_mostrado;
t_log* logger_g;
t_config* archivoconfig;
int socket_conexion_lfs;
unsigned int cantidad_paginas;
size_t tamanio_pagina=0;
unsigned int posicionProximaLibre=0;
int max_value=20;
char* ip_memoria;
char* puerto_memoria;
char* nombre_memoria;
size_t tamanio_memoria;
int socketMemoriaSeed;
void iniciarHiloGossiping(t_list* tablaGossiping);
char* ip__lfs;
char* path;
char* pathlog;
char* puerto__lfs;
char** puertosSeeds;
char** seeds;
int server_memoria;
int socket_kernel_conexion_entrante;
typedef char* t_valor;	//valor que devuelve el select
char** levantarSeeds();
pagina_concreta* esperarRegistroYPocesarlo();
char** levantarPuertosSeeds();
void select_esperar_conexiones_o_peticiones(char* memoria_principal,t_list* tablas);
t_gossip* encontrarMemoria(char* nombredMemoria, t_list* memorias);
void iniciarHiloConsola();
void *iniciar_consola(void* dato);
void parsear_y_ejecutar(char* linea, int flag_de_consola, char* memoria, t_list* tablas);
void ejecutar_API_desde_consola(t_instruccion_lql instruccion,char* memoria,t_list* tablas);
int resolver_select_para_consola(t_instruccion_lql select,char* memoria_principal, t_list* tablas);
int resolver_insert_para_consola(t_instruccion_lql insert,char* memoria_principal, t_list* tablas);
void seedsCargadas();
void logearSeeds();
void levantar_datos_memoria();
void levantar_datos_lfs();
int esperar_operaciones();
void iniciarTablaDeGossiping();
void gossiping_consola();
void ejecutar_API_desde_Kernel(int socket_memoria, t_operacion cod_op,char* memoria_principal, t_list* tablas);
void leer_config(char* path);
void iniciar_logger(char* pathlog);
void iniciar_logger_gossip(char* pathlog);
void iniciar_servidor_memoria_y_esperar_conexiones_kernel();
void intentar_handshake_a_lfs(int alguien);
int insert(char* tabla, uint16_t key, long timestamp);
void enviar_paquete_select(int socket_envio, t_paquete_select* consulta_select);
void recibir_datos(t_log* logger,int socket_fd);
void recibir_max_value(t_log* logger, int socket_cliente);
pagina_concreta* traerRegistroDeMemoria(int posicion);
/*segmento**/segmento* crearSegmento(char* nombreTabla);
t_valor select_(char* tabla, uint16_t key);
void paginaNueva(uint16_t key, char* value, long ts, char* tabla, char* memoria,t_list* tablas);
void paginaNuevaInsert(uint16_t key, char* value, long ts, char* tabla, char* memoria,t_list* tablas);
void agregarPaginaASegmento(char* tabla, pagina* pagina, t_list* tablas);
int drop(char* tabla);
void resolver_describe_consola(t_instruccion_lql instruccion);
void iniciar_hilo_inotify_memoria(char **argv);

int create(char* tabla, t_consistencia consistencia, int maximo_particiones, long tiempo_compactacion);

t_metadata describe(char* tabla);

int journal(void);

/*OPERACIONES*/
int resolver_select_para_kernel (int socket_kernel_fd, int socket_conexion_lfs,char* memoria_principal, t_list* tablas);
t_paquete_select* consulta_select_a_usar;
t_paquete_insert* consulta_insert_a_usar;
void resolver_despues_de_journaling (int socket_kernel_fd, t_paquete_select* consulta_select ,int socket_conexion_lfs,char* memoria_principal, t_list* tablas);
void resolver_insert_despues_de_journaling(t_paquete_insert* consulta_insert, int socket_conexion_lfs,char* memoria_principal, t_list* tablas);
void resolver_describe_para_kernel(int socket_kernel_fd, int socket_conexion_lfs, char* operacion);
void resolver_drop_consola(t_instruccion_lql instruccion);
#endif /* MEMORIA_H_ */
