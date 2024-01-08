#ifndef MOONSHOT_SPI_H
#define MOONSHOT_SPI_H

#include "postgres.h"

typedef struct {
    double* arr;
    int size;
} double_array_data;

typedef struct {
    int ncols;
    int proc;
    int pos;
    Datum* data;
} row_cache;

extern bool activeSPI;

extern int connect_SPI();
extern void disconnect_SPI();
extern int execute(char* query);

extern bool fetch_next();

extern double getdouble(int column);
extern float getfloat(int column);
extern int getint(int column);
extern double_array_data* getdoublearray(int column);

extern double_array_data* fetch_next_double_array(int column);

#endif