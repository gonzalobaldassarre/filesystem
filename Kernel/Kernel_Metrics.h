/*
 * Kernel_Metrics.h
 *
 *  Created on: 18 jun. 2019
 *      Author: utnso
 */
#ifndef KERNEL_METRICS_H_
#define KERNEL_METRICS_H_

#include <math.h>
#include "Kernel_Plani.h"


typedef struct t_metrics {
	int nombre_memoria;
	int read_latency;
	int write_latency;
	int cant_reads;
	int cant_writes;
	int memory_load;
	int cant_operaciones_totales;

} t_metrics;


t_log* log_metrics;

void iniciar_hilo_metrics();
void loguear_y_borrar();
void loguear();
void borrar();
t_metrics* obtener_nodo_metricas(int nombre_memoria);
void registrar_metricas(int nombre_memoria, int diferencia_timestamp);
void registrar_metricas_insert(int nombre_memoria, int diferencia_timestamp);

#endif /* KERNEL_METRICS_H_ */
