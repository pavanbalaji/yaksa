##
## Copyright (C) by Argonne National Laboratory
##     See COPYRIGHT in top-level directory
##

AM_CPPFLAGS += -I$(top_srcdir)/src/frontend/context

libyaksa_la_SOURCES += \
	src/frontend/context/yaksa_context.c
