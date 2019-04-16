#ifndef _INTERPRETER_H_
#define _INTERPRETER_H_
#include "..\minisql.h"
#include "..\catalogManager\catalogManager.hpp"

class interpreterManager
{
public:
	int operation;
	vector<Attribute> oAttributeList;
	vector<Condition> oConditionList;
	Tuple oTuple;
	Table oTable;
	Index oIndex;
	int primaryKeyIndex;
	int uniqueKeyIndex;
	vector<Condition> uniqueCondition;
	string oTableName;
	string oIndexName;
	string oAttributeName;
	string exeFileName;

	interpreterManager();
	bool getToken(string & command, string& result);
	void parseCommand(char* command);

private:
	void initialize();
	bool getString(string& input, string& output);
	bool isInt(const char* input);
	bool isFloat(const char* input);
}interpreterManager;

#endif