#include "Kernel.h"

//Variables

t_queue* new_queue;
t_queue* ready_queue;
t_queue* exec_queue;
t_queue* exit_queue;

//Procedimientos

void iniciar_hilo_planificacion();
void iniciar_hilo_ready();
void iniciar_hilo_ejecucion();
void* planificador();
void revisa_new_queue();
void revisa_ready_queue();
void revisa_exit_queue();
int hay_algo();
int puedo_ejecutar();
void revisa_exec_queue();


