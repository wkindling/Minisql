#ifndef _INDEX_H_
#define _INDEX_H_
#include "..\minisql.h"
#include "bPlusTree.h"

class indexManager
{
public:
	void createIndex(Table& table, Index& index);
	void dropIndex(Index& index);
	void deleteTuple(Index& index,Table& table, string key);
	void insertTuple(Index& index,Table& table, string key);
	Tuple equalSelect(Table& table, Index& index, string searchKey, int blockIndex=0);

private:
	Tuple transferToTuple(Table& table,string stringTuple);
	string getAttributeValue(Table& table, Index& index,string tuple);
	
	IndexLeaf findDropLeafNode(Table& table, Index& index,string key,int blockIndex=0);
	IndexLeaf findInsertLeafNode(Table& table,Index& index,string key);
	IndexNoneL insertNode(Index& index, IndexLeaf node,int blockIndex=0);
	void deleteNode(Index& index, IndexLeaf node, int blockIndex = 0);
	
}indexManager;


#endif