AC_PREREQ(2.59)

AC_INIT(lib_mysqludf_dari,0.0.1,jeremy@perfectsensedigital.com)

AC_CHECK_LIB([jansson],[json_dumps], , AC_ERROR(jansson not found))
AC_CHECK_LIB([snappy], [snappy_compress], [], [AC_MSG_ERROR([No snappy library is not found.])])

AM_INIT_AUTOMAKE(foreign)

AM_PROG_LIBTOOL
AC_PROG_CC

m4_include(config/ax_lib_mysql.m4)
AX_LIB_MYSQL(,,AC_MSG_ERROR("Can't find mysql library" ))

if test -n "$MYSQL_PLUGINDIR" && test "$libdir" == '${exec_prefix}/lib' ; then
  AC_MSG_NOTICE(setting libdir to mysql plugin dir $MYSQL_PLUGINDIR)
  libdir=$MYSQL_PLUGINDIR
  AC_SUBST(libdir)
fi

m4_include(config/ax_mysql_bin.m4)
AX_MYSQL_BIN

AC_C_CONST
AC_TYPE_SIZE_T
AC_CHECK_HEADERS(limits.h syslimits.h)

AC_OUTPUT([ Makefile ])
