/*
 * parser.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */
#include "parser.h"

#define EXIT_KEY "EXIT"
#define INSERT_KEY "INSERT"
#define SELECT_KEY "SELECT"
#define CREATE_KEY "CREATE"
#define DESCRIBE_KEY "DESCRIBE"
#define DROP_KEY "DROP"
#define ADD_KEY "ADD"
#define RUN_KEY "RUN"
#define JOURNAL_KEY "JOURNAL"
#define GOSSIPING_KEY "GOSSIPING"
#define METRICS_KEY "METRICS"
#define TABLA_KEY "SOLICITUD_TABLA_GOSSIPING"

t_instruccion_lql parsear_linea(char* line){

	if(line == NULL || string_equals_ignore_case(line, "")){
		return lanzar_error("No se pudo interpretar una linea vacia\n");
	}

	t_instruccion_lql ret = {
		.valido = true
	};

	char* auxLine = string_duplicate(line);
	string_trim(&auxLine);
	char** split = string_split(auxLine," ");

	char* keyword = split[0];

	ret._raw = split;

	if(string_equals_ignore_case(keyword, INSERT_KEY)){
		ret.operacion = INSERT;
		if (split[1] == NULL || split[2]== NULL || split[3] == NULL ){
			free(split);
			return lanzar_error("Formato incorrecto. INSERT TABLA KEY VALUE TIMESTAMP - El ultimo valor es opcional\n");
		}
		char **insert_split = string_n_split(auxLine, 5, " ");
		if (insert_split[1] == NULL || insert_split[2]== NULL || insert_split[3] == NULL || string_is_empty(insert_split[3])){
			free(insert_split);
			free(split);
			return lanzar_error("Formato incorrecto. INSERT TABLA KEY VALUE TIMESTAMP - El ultimo valor es opcional\n");
		}
		char* insert_value = string_new();
		string_append(&insert_value, insert_split[3]);
		char **value_ts = string_split( insert_value, "\"");
		string_to_upper(insert_split[1]);

		ret.parametros.INSERT.tabla = insert_split[1];
		ret.parametros.INSERT.key= (uint16_t)atol(insert_split[2]);
		if(insert_split[4] == NULL){

			ret.parametros.INSERT.timestamp= get_timestamp();//(unsigned long)time(NULL);
			//printf("\n\n TIMESTAMP: %lu as\n\n",(unsigned long)get_timestamp());
		} else{
			ret.parametros.INSERT.timestamp=(long)atol(insert_split[4]);
		}

		ret.parametros.INSERT.value = value_ts[0];
		ret.parametros.INSERT.split_campos=insert_split;
		ret.parametros.INSERT.split_value=value_ts;
		free(insert_value);

	} else if(string_equals_ignore_case(keyword, SELECT_KEY)){
		ret.operacion=SELECT;
		if (split[1] == NULL || split[2]== NULL){
			free(split);
			return lanzar_error("Formato incorrecto. SELECT TABLA KEY\n");
		}
		string_to_upper(split[1]);
		ret.parametros.SELECT.tabla = split[1];
		ret.parametros.SELECT.key= (uint16_t)atol(split[2]);
	} else if(string_equals_ignore_case(keyword, CREATE_KEY)){
		ret.operacion=CREATE;
		if (split[1] == NULL || split[2]== NULL || split[3] == NULL || split[4] == NULL){
			free(split);
			return lanzar_error("Formato incorrecto. CREATE TABLE SC NumPart COMPACTACION\n");
		}
		string_to_upper(split[1]);
		ret.parametros.CREATE.tabla = split[1];
		string_to_upper(split[2]);
		if (!check_consistencia(split[2])){
			free(split);
			return lanzar_error("Valor consistencia inválido. Debe ser STRONG, STRONG_HASH, EVENTUAL\n");
		}
		ret.parametros.CREATE.consistencia = get_valor_consistencia(split[2]);
		ret.parametros.CREATE.num_particiones=atoi(split[3]);
		ret.parametros.CREATE.compactacion_time=(long)atol(split[4]);
	} else if(string_equals_ignore_case(keyword, DESCRIBE_KEY)){
		ret.operacion=DESCRIBE;
		if(split[1] !=NULL){
			string_to_upper(split[1]);
			ret.parametros.DESCRIBE.tabla = split[1];
		}else{
			ret.parametros.DESCRIBE.tabla=string_new();
		}
	} else if(string_equals_ignore_case(keyword, DROP_KEY)){
		ret.operacion=DROP;
		if (split[1] == NULL || split[2]!= NULL){
			free(split);
			return lanzar_error("Formato incorrecto. DROPE TABLE\n");
		}
		string_to_upper(split[1]);
		ret.parametros.DROP.tabla = split[1];
	} else if(string_equals_ignore_case(keyword, RUN_KEY)){
		ret.operacion=RUN;
		if (split[1] == NULL || split[2]!= NULL){
			free(split);
			return lanzar_error("Formato incorrecto. RUN path\n");
		}
		ret.parametros.RUN.path_script = split[1];
	} else if(string_equals_ignore_case(keyword, JOURNAL_KEY)){
		ret.operacion=JOURNAL;
		if (split[1] != NULL){
			free(split);
			return lanzar_error("Formato incorrecto. JOURNAL\n");
		}
	} else if(string_equals_ignore_case(keyword, GOSSIPING_KEY)){
			ret.operacion=GOSSPING;
			if (split[1] != NULL){
				free(split);
				return lanzar_error("Formato incorrecto. GOSSIPING\n");
			}
	} else if(string_equals_ignore_case(keyword, TABLA_KEY)){
				ret.operacion=SOLICITUD_TABLA_GOSSIPING;
				if (split[1] != NULL){
					free(split);
					return lanzar_error("Formato incorrecto. GOSSIPING\n");
				}
	} else if(string_equals_ignore_case(keyword, ADD_KEY)){
		ret.operacion=ADD;
		if (!string_equals_ignore_case(split[1],"MEMORY") || split[2]== NULL || !string_equals_ignore_case(split[3],"TO") || split[4]==NULL){
			free(split);
			return lanzar_error("Formato incorrecto. ADD MEMORY TO CONSISTENCY\n");
		}
		ret.parametros.ADD.numero_memoria=atoi(split[2]);
		ret.parametros.ADD.consistencia= get_valor_consistencia(split[4]);

	} else if(string_equals_ignore_case(keyword, METRICS_KEY)){
		ret.operacion=METRICS;
		if(split[1] != NULL){
			free(split);
			return lanzar_error("Formato incorrecto. METRICS\n");
		}
	} else if(string_equals_ignore_case(keyword, EXIT_KEY)){
			ret.operacion=EXIT;
	}else {
		free(split);
		return lanzar_error("Operacion no contemplada.\n");
	}

	free(auxLine);
	return ret;
}

int check_consistencia(char* consistencia_ingresada){
	if ( !string_equals_ignore_case(STRONG_TEXT,consistencia_ingresada) && !string_equals_ignore_case(STRONG_HASH_TEXT,consistencia_ingresada) && !string_equals_ignore_case(EVENTUAL_TEXT,consistencia_ingresada)){
		return false;
	}
	return true;
}

t_instruccion_lql lanzar_error(char* mensaje){
	fprintf(stderr,mensaje );
	t_instruccion_lql error;
	error.valido=false;
	return error;
}
long long get_timestamp() {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    long long ms = spec.tv_nsec / (long) 1000000 + (spec.tv_sec * (long) 1000);
    return ms;
}
