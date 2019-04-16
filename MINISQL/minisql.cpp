#include "minisql.h"
#include "api\api.hpp"
#include "bufferManager\bufferManager.hpp"
#include "interpreterManager\interpreterManager.hpp"
#include "recordManager\recordManager.hpp"
#include "catalogManager\catalogManager.hpp"
#include "indexManager\indexManager.hpp"



int main()
{
	helloWorld();
	char command[COMMENDLENGTH]="";
	char input[INPUTLENGTH]="";
    bool judge=false;

    while (1)
    {
        cout<<">>";
    	strcpy(command,"");
    	judge=false;
    	while (!judge)
    	{
    		gets_s(input);
    		//gets(input);
			if (judgeEnd(input))
    			judge=true;
    		strcat(command,input);
    		addEnd(command);
    	}
    	interpreterManager.parseCommand(command);
        if (interpreterManager.operation==EXEFILE)
        {
            string fileName=interpreterManager.exeFileName;
			string fileCommand;
			fstream myfile(fileName.c_str(),ios::in);
            //ifstream myfile(fileName);
			if (!myfile.is_open())
			{
				cout << "Cannot open the file." << endl;
			}
            while (getline(myfile,fileCommand))
            {
				char filecommand[COMMENDLENGTH] = "";
				for (int i = 0; i < fileCommand.length(); i++) filecommand[i] = fileCommand[i];
				judgeEnd(filecommand);
                addEnd(filecommand);
                interpreterManager.parseCommand(filecommand);
                sqlLoop();
            }
        }
    	else sqlLoop();
    }
    getchar();
    return 0;
}
