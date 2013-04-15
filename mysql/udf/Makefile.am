lib_LTLIBRARIES = lib_mysqludf_dari.la
lib_mysqludf_dari_la_SOURCES = lib_mysqludf_dari.c
lib_mysqludf_dari_la_CFLAGS = -DSTANDARD -DMYSQL_SERVER @MYSQL_CFLAGS@
lib_mysqludf_dari_la_CXXFLAGS = -DSTANDARD -DMYSQL_SERVER @MYSQL_CFLAGS@
lib_mysqludf_dari_la_LDFLAGS = -module -avoid-version -no-undefined -rpath @MYSQL_PLUGINDIR@ -lsnappy -ljansson

EXTRA_DIST = *.h *.sql

mrproper:
	make clean
	make maintainer-clean
	rm -rf config.guess config.h.* config.status configure config.sub ltmain.sh depcomp aclocal.m4 install.sh config.log compile missing Makefile.in doc/html *.lo mkinstalldirs install-sh *~ config/ltmain.sh config/libtool.m4 config/ltoptions.m4 config/ltversion.m4 config/lt~obsolete.m4

.PHONY : mrproper
