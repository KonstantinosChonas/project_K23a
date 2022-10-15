#include "int.h"


int main (void){

    int n = 21,i=0,j=1;
    for (i=0;i<3;i++){
        j=j*2;
    }
    j--;

    printf("%d \n",n & j);


}