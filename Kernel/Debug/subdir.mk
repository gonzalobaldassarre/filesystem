################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../Kernel.c \
../Kernel_Metrics.c \
../Kernel_Plani.c 

OBJS += \
./Kernel.o \
./Kernel_Metrics.o \
./Kernel_Plani.o 

C_DEPS += \
./Kernel.d \
./Kernel_Metrics.d \
./Kernel_Plani.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/tp-2019-1c-Los-Dinosaurios-Del-Libro/global" -O0 -g -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


