#include "int.h"

intermediate** intermediateCreate(int numOfRelations ){

    intermediate **array=malloc(numOfRelations * sizeof(*intermediate));
    for (int i=0 ; i<numOfRelations ; i++)
    {
        array[i]=NULL;
        array->num_cols=numOfRelations;
        array->num_rows=0;

    }

    return array;


}