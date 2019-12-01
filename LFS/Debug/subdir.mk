################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../LFS.c \
../LFS_Compactacion.c \
../LFS_Consola.c \
../LFS_Dump.c \
../LFS_Server.c 

OBJS += \
./LFS.o \
./LFS_Compactacion.o \
./LFS_Consola.o \
./LFS_Dump.o \
./LFS_Server.o 

C_DEPS += \
./LFS.d \
./LFS_Compactacion.d \
./LFS_Consola.d \
./LFS_Dump.d \
./LFS_Server.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/global" -O0 -g -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


