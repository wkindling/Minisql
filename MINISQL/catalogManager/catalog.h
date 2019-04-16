#ifndef _CATALOG_H_
#define _CATALOG_H_
#include "..\minisql.h"
#include "..\bufferManager\bufferManager.hpp"

class catalogManager
{
public:
	catalogManager();
	~catalogManager();

	void createTable(Table& table);
	void dropTable(string tableName);
    void updateTable(Table& table);
    bool tableCheck(string tableName);
    Table getTable(string tableName);


	void createIndex(Index& Index);
    void dropIndex(string indexName);
    void updateIndex(Index& index);
    bool indexCheck(string indexName);
    Index getIndex(string indexName);
    Index getIndexFromTable(string tableName,int attributeIndex);
    int getAttributeIndex(Table& table, string attributeName);


	vector<Table> tableList;
	vector<Index> indexList;
	int tableNum;
	int indexNum;

	void openTableCatalog();
    void openIndexCatalog();
    void saveTableCatalog();
    void saveIndexCatalog();

}catalogManager;
#endif
