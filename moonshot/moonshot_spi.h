#ifndef MOONSHOT_SPI_H
#define MOONSHOT_SPI_H

#include "postgres.h"

typedef struct {
    double* arr;
    int size;
} double_array_data;

typedef struct {
    float* arr;
    int size;
} float_array_data;

typedef struct {
    char* arr;
    int size;
} char_array_data;


typedef struct {
    int ncols;
    int proc;
    int pos;
    Datum* data;
} row_cache;

typedef struct Vector
{
	int32		vl_len_;		
	int16		dim;			
	int16		unused;
	float		x[FLEXIBLE_ARRAY_MEMBER];
} Vector;

extern bool activeSPI;

extern int connect_SPI();
extern void disconnect_SPI();
extern int execute(char* query, bool use_cursor);

extern bool fetch_next();

extern double getdouble(int column);
extern float getfloat(int column);
extern int getint(int column);
extern long getlong(int column);
extern double_array_data* getdoublearray(int column);
extern float_array_data* getvector(int column);

/* ? CHECK IF CAN BE DEPRECATED */
extern double_array_data* fetch_next_double_array(int column);

#endif