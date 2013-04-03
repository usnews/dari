CFLAGS = -std=c99 -O2 -W -Wall -pedantic -pthread

all: cache-invalidation.so

cache-invalidation.so: cache-invalidation.c
	g++ -O3 -Wall -DMYSQL_DYNAMIC_PLUGIN -lzmq -lmysqlservices -L/usr/local/mysql/lib -I/usr/local/include -I/usr/local/mysql/include -I. -shared cache-invalidation.c -o cache-invalidation.so

install: all
	cp cache-invalidation.so /usr/local/mysql/lib/plugin

clean: 
	rm -rf cache-invalidation.so cache-invalidation.so.dSYM
