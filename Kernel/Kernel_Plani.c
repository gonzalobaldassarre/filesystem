#include "Kernel_Plani.h"



//Creación de Hilos

void iniciar_hilo_planificacion(){
	pthread_t hilo_planificacion;

	if(pthread_create(&hilo_planificacion, 0, planificador, NULL) !=0){
		log_error(logger, "Error al crear el hilo de Planificación");
	}
	if(pthread_detach(hilo_planificacion) != 0){
		log_error(logger, "Error al crear el hilo de Planificación");
	}
}

void iniciar_hilo_ready(){
	pthread_t hilo_ready;

	if(pthread_create(&hilo_ready, 0, revisa_ready_queue, NULL) !=0){
		log_error(logger, "Error al crear el hilo de Ready Queue");
	}
	if(pthread_detach(hilo_ready) != 0){
		log_error(logger, "Error al crear el hilo de Ready Queue");
	}
}

void iniciar_hilo_ejecucion(){
	pthread_t hilo_ejecucion;

	if(pthread_create(&hilo_ejecucion, 0, revisa_exec_queue, NULL) !=0){
		log_error(logger, "Error al crear el hilo de Exec Queue");
	}
	if(pthread_detach(hilo_ejecucion) != 0){
		log_error(logger, "Error al crear el hilo de Exec Queue");
	}
}


void* planificador(){
	log_plani = crear_log("/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/Kernel/Kernel_Plani.log", 0);

	pthread_mutex_init(&exec_queue_mutex, NULL);
	pthread_mutex_init(&ready_queue_mutex, NULL);
	sem_init(&exec_queue_consumer, 1, 0);
	sem_init(&ready_queue_consumer, 1, 0);

	iniciar_hilo_ready();

	for(int cant = 0; cant < CANT_EXEC; cant++){
		iniciar_hilo_ejecucion();
	}

	while(1){
		revisa_new_queue();
	}

	pthread_mutex_destroy(&exec_queue_mutex);
	pthread_mutex_destroy(&ready_queue_mutex);
	sem_destroy(&exec_queue_consumer);
	sem_destroy(&ready_queue_consumer);
}

void revisa_new_queue(){
	sem_wait(&new_queue_consumer);
	while(queue_size(new_queue)>0){
		char* new_path=queue_pop(new_queue);

		t_script* nuevo_script = malloc(sizeof(t_script));
		nuevo_script->id=generarID();
		nuevo_script->path = malloc(string_size(new_path));
		memcpy(nuevo_script->path,new_path, string_size(new_path));
		nuevo_script->offset=0;
		nuevo_script->error=0;

		pthread_mutex_lock(&ready_queue_mutex);
		queue_push(ready_queue, nuevo_script);
		pthread_mutex_unlock(&ready_queue_mutex);
		log_info(log_plani, "El archivo %s pasa a estado READY", new_path);
		sem_post(&ready_queue_consumer);
	}
}

void revisa_ready_queue(){
	while(1){
		sem_wait(&ready_queue_consumer);
		if(puedo_ejecutar() && hay_algo()) {
			pthread_mutex_lock(&ready_queue_mutex);
			t_script* script=queue_pop(ready_queue);
			pthread_mutex_unlock(&ready_queue_mutex);

			pthread_mutex_lock(&exec_queue_mutex);
			queue_push(exec_queue, script);
			pthread_mutex_unlock(&exec_queue_mutex);
			log_info(log_plani, "El archivo %s pasa a estado EXEC", script->path);
			sem_post(&exec_queue_consumer);
		}
	}
}

int hay_algo(){
	pthread_mutex_lock(&ready_queue_mutex);
	int size = queue_size(ready_queue);
	pthread_mutex_unlock(&ready_queue_mutex);

	return size > 0;
}

int puedo_ejecutar(){
	pthread_mutex_lock(&exec_queue_mutex);
	int size = queue_size(exec_queue);
	pthread_mutex_unlock(&exec_queue_mutex);

	return size<CANT_EXEC;
}

void revisa_exec_queue(){
	while(1){
		sem_wait(&exec_queue_consumer);

		pthread_mutex_lock(&exec_queue_mutex);
		t_script* script_a_ejecutar = queue_pop(exec_queue);
		pthread_mutex_unlock(&exec_queue_mutex);

		ejecutar_script(script_a_ejecutar);
		sleep(SLEEP_EJECUCION);

		if (script_a_ejecutar->offset==NULL) {
			queue_push(exit_queue, script_a_ejecutar);
			log_info(log_plani, "El archivo %s pasa a estado EXIT", script_a_ejecutar->path);
			sem_post(&ready_queue_consumer);
		} else {
			pthread_mutex_lock(&ready_queue_mutex);
			queue_push(ready_queue, script_a_ejecutar);
			pthread_mutex_unlock(&ready_queue_mutex);
			log_info(log_plani, "El archivo %s pasa a estado READY", script_a_ejecutar->path);
			sem_post(&ready_queue_consumer);
		}
	}
}

void revisa_exit_queue(){
	while(1){
		t_script* script=queue_pop(exit_queue);
		fclose(script);

	}
}


