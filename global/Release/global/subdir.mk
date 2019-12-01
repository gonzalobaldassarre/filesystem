################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../global/parser.c \
../global/protocolos.c \
../global/strings.c \
../global/utils.c 

OBJS += \
./global/parser.o \
./global/protocolos.o \
./global/strings.o \
./global/utils.o 

C_DEPS += \
./global/parser.d \
./global/protocolos.d \
./global/strings.d \
./global/utils.d 


# Each subdirectory must supply rules for building sources it contributes
global/%.o: ../global/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


