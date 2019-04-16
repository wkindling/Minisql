#ifndef _RECORD_H_
#define _RECORD_H_
#include "..\minisql.h"
#include "..\bufferManager\bufferManager.hpp"

class recordManager
{
public:
	TupleSet select(Table& table,vector<Condition> conditionList);
	void createTable(Table& table);
	void dropTable(Table& table);
	void insertTuple(Table& table,Tuple& tuple);
	int deleteTuple(Table& table,vector<Condition> conditionList);
	bool compare(Table& table,Tuple& tuple,vector<Condition> conditionList);

private:
	Tuple transferToTuple(Table& table,string stringTuple);
	string transferToString(Table& table,Tuple tuple);
}recordManager;



#endif