#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
   Joiner joiner;
   // Read join relations
   string line;
   while (getline(cin, line)) {
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }

    relationInfo *relIn=NULL;

    relIn = (relationInfo*) malloc(joiner.relations.size()*sizeof(relationInfo));
    // relIn=new relationInfo[joiner.relations.size()];

    for (int i=0 ; i<joiner.relations.size() ; i++)
        relIn[i].columns=NULL;


    // relIn->columns=NULL;

    cerr << "hello" << endl;

    for (int i=0 ; i < joiner.relations.size() ; i++){

        //relationInfo r=relIn[i];

        relIn[i].num_tuples=joiner.relations[i].size;

        relIn[i].num_cols=joiner.relations[i].columns.size();

        // r.columns=(int **)malloc(joiner.relations[i].size*sizeof(int*));

        relIn[i].columns=new int * [joiner.relations[i].size];

        for (int j=0 ; j<joiner.relations[i].size ; j++){
            // r.columns[j]=(int *)malloc(joiner.relations[i].columns.size()*sizeof(int));
            // r.columns[j]=&j;

             relIn[i].columns[j]=new int[joiner.relations[i].columns.size()];
        }
        //cout << joiner.relations[i].size << "*" << joiner.relations[i].columns.size() << " ";
        for (int j=0 ; j<joiner.relations[i].size ; j++){

            for ( int k=0 ; k<joiner.relations[i].columns.size() ; k++){



                relIn[i].columns[j][k]=joiner.relations[i].columns[k][j];

            }

        }


    }

    cerr << "relIn[0].columns[5][3]" << endl;

    cerr << relIn[2].columns[5][3] << endl;

   // Preparation phase (not timed)
   // Build histograms, indexes,...
   //
   QueryInfo i;
   while (getline(cin, line)) {
       if (line == "F") continue; // End of a batch
       i.parseQuery(line);
       //cerr << line << endl;
       //cerr << i.relationIds[1] << endl;
       cout << joiner.join(i);
   }



   return 0;
}
