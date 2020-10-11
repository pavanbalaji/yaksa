##
## Copyright (C) by Argonne National Laboratory
##     See COPYRIGHT in top-level directory
##

AM_CPPFLAGS += -I$(top_srcdir)/src/frontend/pup

libyaksa_la_SOURCES += \
	src/frontend/pup/yaksa_ipack.c \
	src/frontend/pup/yaksa_iunpack.c \
	src/frontend/pup/yaksa_iacc.c \
	src/frontend/pup/yaksa_request.c \
	src/frontend/pup/yaksi_ipack.c \
	src/frontend/pup/yaksi_ipack_element.c \
	src/frontend/pup/yaksi_ipack_backend.c \
	src/frontend/pup/yaksi_iacc_unpack.c \
	src/frontend/pup/yaksi_iacc_unpack_element.c \
	src/frontend/pup/yaksi_iacc_unpack_backend.c \
	src/frontend/pup/yaksi_request.c
