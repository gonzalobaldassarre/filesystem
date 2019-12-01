RM=rm -rf
CC=gcc

C_SRCS=$(shell find . -iname "*.c" | tr '\n' ' ')
H_SRCS=$(shell find . -iname "*.h" | tr '\n' ' ')
OBJS=$(C_SRCS:./src/%.c=src/%.o)

# Clean and compile .so
all:
	 
	libglobal.so

libglobal.so: 
	@echo 'Pase por aca libglobal.so'
	$(CC) -shared -o "libglobal.so" $(OBJS)

src/utils.o: src/%.c
	@echo 'Pase por aca algo con .o'
	$(CC) -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"

# Add debug parameters and compile
debug: CC += -DDEBUG -g
debug: all

install: all
	cp -u libglobal.so /usr/lib
	cp --parents -u $(H_SRCS) /usr/include

uninstall:
	rm -f /usr/lib/libglobal.so
	rm -rf /usr/include/global

.PHONY: all create-dirs clean install uninstall
