CFLAGS = -O2 -W -Wall -pedantic -pthread

MYSQL_SRC ?= /Users/jcollins/src/mysql-5.6.10

all: cache-invalidation.so

cache-invalidation.so: cache-invalidation.cc
	g++ -O3 -Wall -DMYSQL_DYNAMIC_PLUGIN -lzmq -lmysqlservices \
	-L/usr/local/mysql/lib -I/usr/local/include -I. \
	-I${MYSQL_SRC}/sql \
	-I${MYSQL_SRC}/include \
	-I${MYSQL_SRC}/regex \
	-I/usr/local/mysql/include \
	-shared \
	cache-invalidation.cc -o cache-invalidation.so


install: all
	cp cache-invalidation.so /usr/local/mysql/lib/plugin
	-echo "UNINSTALL PLUGIN CACHE_INVALIDATION;" | mysql -u root -p
	-echo "INSTALL PLUGIN CACHE_INVALIDATION SONAME 'cache-invalidation.so';" | mysql -u root -p

clean: 
	rm -rf cache-invalidation.so cache-invalidation.so.dSYM
