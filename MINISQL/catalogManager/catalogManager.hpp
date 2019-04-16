#ifndef _CATALOGMANAGER_H_
#define _CATALOGMANAGER_H_
#include "catalog.h"

catalogManager::catalogManager()
{
    openTableCatalog();
    openIndexCatalog();
}

catalogManager::~catalogManager()
{
	saveTableCatalog();
	saveIndexCatalog();
}

void catalogManager::createTable(Table& table)
{
	tableNum++;
	for (int i=0;i<table.attributeList.size();i++) 
		table.tupleLength+=table.attributeList[i].attributeLength;
	tableList.push_back(table);
}

void catalogManager::createIndex(Index& index)
{
    indexNum++;
    indexList.push_back(index);
}

void catalogManager::dropTable(string tableName)
{
	for (int i=0;i<tableNum;i++)
	{
		if (tableList[i].tableName==tableName)
		{
			tableList.erase(tableList.begin()+i);
			tableNum--;
			break;
		}
	}
	for (int i=0;i<indexNum;i++)
	{
		if (indexList[i].tableName==tableName)
		{
			indexList.erase(indexList.begin()+i);
			indexNum--;
			break;
		}
	}
}

void catalogManager::dropIndex(string indexName)
{
	for (int i = 0; i < indexList.size(); i++)
	{
		if (indexList[i].indexName == indexName)
		{
			indexList.erase(indexList.begin() + i);
			i--;
			indexNum--;
		}
	}
}

void catalogManager::updateTable(Table& table)
{
	for (int i=0;i<tableNum;i++)
	{
		if (tableList[i].tableName==table.tableName)
		{
           tableList[i]=table;
           break;
		}
	}
}

void catalogManager::updateIndex(Index& index)
{
	for (int i=0;i<indexNum;i++)
	{
		if (indexList[i].indexName==index.indexName)
		{
			indexList[i]=index;
			break;
		}
	}
}

bool catalogManager::tableCheck(string tableName)
{
	for (int i=0;i<tableNum;i++)
	{
		 if (tableList[i].tableName==tableName)
		 	return true;
	}
	return false;
}

bool catalogManager::indexCheck(string indexName)
{
	for (int i=0;i<indexNum;i++)
	{
		 if (indexList[i].indexName==indexName)
		 	return true;
	}
	return false;
}

Table catalogManager::getTable(string tableName)
{
	Table emptyTable;
	for (int i=0;i<tableNum;i++)
	{
		if (tableList[i].tableName==tableName)
		{
			return tableList[i];
		}
	}
    return emptyTable;
}

Index catalogManager::getIndex(string indexName)
{
	Index emptyIndex;
	for (int i=0;i<indexNum;i++)
	{
		if (indexList[i].indexName==indexName)
		{
			return indexList[i];
		}
	}
    return emptyIndex;
}

Index catalogManager::getIndexFromTable(string tableName,int attributeIndex)
{
	int i;
//	Index emptyIndex;
//	emptyIndex.indexName = "";
	for (i=0;i<indexList.size();i++)
	{
		if (indexList[i].tableName==tableName&&indexList[i].attributeIndex==attributeIndex)
		{
			break;
			//return indexList[i];
		}
	}
	if (i >= indexNum)
	{
		Index temp;
		return temp;
	}
	return indexList[i];
	//return emptyIndex;

}

int catalogManager::getAttributeIndex(Table& table, string attributeName)
{
	for (int i=0;i<table.attributeNum;i++)
	{
		if (table.attributeList[i].attributeName==attributeName)
		{
			return i;
		}
	}
	return -1;
}

void catalogManager::openTableCatalog()
{
	const string fileName="table.catalog";
	fstream fin(fileName.c_str(),ios::in);
	fin>>tableNum;

	for (int i=0;i<tableNum;i++)
	{
		Table currentTable;
		fin>>currentTable.tableName;
		fin>>currentTable.attributeNum;
		fin>>currentTable.tableBlockNum;

		for (int j=0;j<currentTable.attributeNum;j++)
		{
			Attribute currentAttribute;
			fin>>currentAttribute.attributeName;
			fin>>currentAttribute.attributeType;
			fin>>currentAttribute.attributeLength;
			fin>>currentAttribute.isUnique;
			fin>>currentAttribute.isPrimaryKey;
			currentTable.tupleLength += currentAttribute.attributeLength;
			currentTable.attributeList.push_back(currentAttribute);
		}

		tableList.push_back(currentTable);
	}
	fin.close();
}

void catalogManager::openIndexCatalog()
{
	const string fileName="index.catalog";
	fstream fin(fileName.c_str(),ios::in);
	fin>>indexNum;

	for (int i=0;i<indexNum;i++)
	{
		Index currentIndex;
		fin>>currentIndex.indexName;
		fin>>currentIndex.tableName;
		fin>>currentIndex.attributeIndex;
		fin>>currentIndex.attributeLength;
		fin>>currentIndex.indexBlockNum;
		indexList.push_back(currentIndex);
	}
	fin.close();
}

void catalogManager::saveTableCatalog()
{
	const string fileName="table.catalog";
	fstream fout(fileName.c_str(),ios::out);
	fout<<tableNum<<endl;

	for (int i=0;i<tableNum;i++)
	{
		fout<<tableList[i].tableName<<" ";
		fout<<tableList[i].attributeNum<<" ";
		fout<<tableList[i].tableBlockNum<<endl;

		for (int j=0;j<tableList[i].attributeNum;j++)
		{
			fout<<tableList[i].attributeList[j].attributeName<<" ";
			fout<<tableList[i].attributeList[j].attributeType<<" ";
			fout<<tableList[i].attributeList[j].attributeLength<<" ";
			fout<<tableList[i].attributeList[j].isUnique<<" ";
			fout<<tableList[i].attributeList[j].isPrimaryKey<<endl;
		}
	}
	fout.close();
}

void catalogManager::saveIndexCatalog()
{
	const string fileName="index.catalog";
	fstream fout(fileName.c_str(),ios::out);
	fout<<indexNum<<endl;

	for (int i=0;i<indexNum;i++)
	{
		fout<<indexList[i].indexName<<" ";
		fout<<indexList[i].tableName<<" ";
		fout<<indexList[i].attributeIndex<<" ";
		fout<<indexList[i].attributeLength<<" ";
		fout<<indexList[i].indexBlockNum<<endl;
	}
	fout.close();
}

#endif