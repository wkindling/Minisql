#ifndef _API_H_
#define _API_H_
#include "..\minisql.h"
#include "..\interpreterManager\interpreterManager.hpp"
#include "..\recordManager\recordManager.hpp"
#include "..\indexManager\indexManager.hpp"

void printResult(TupleSet data, Table table, vector<Attribute> column);
void helloWorld();
void sqlLoop();
void addEnd(char *command);
bool judgeEnd(char *input);

void helloWorld()
{
	cout<<"Welcome to MINISQL created by Wen Jiahao."<<endl;
	cout<<"It is a Final Project for Database System Concepts."<<endl;
	cout<<"It implements some basic functions of Mysql."<<endl;
	cout<<"There may exist some bugs. Nevertheless, enjoy yourself:)"<<endl;
	cout << "----------------------------------------------------------------------" << endl;
	cout << endl;
}

void sqlLoop()
{
	Table table;
	Index index;
	string tempKey;
	string fileName;
	int tempPrimaryKeyIndex=-1;
	int tupleCount=0;
	TupleSet tupleset;

	switch(interpreterManager.operation)
	{
		case CREATETABLE:
			interpreterManager.oTable.attributeNum=interpreterManager.oTable.attributeList.size();
			catalogManager.createTable(interpreterManager.oTable);
			recordManager.createTable(interpreterManager.oTable);
			cout<<"Table "<<interpreterManager.oTable.tableName<<" has been created successfully."<<endl;
			break;
		case TABLEEXISTED:
			cout<<"The table has been created, please check the catalog."<<endl;
			break;
		case DROPTABLE:
			recordManager.dropTable(interpreterManager.oTable);
			for (int i=0;i<interpreterManager.oTable.attributeNum;i++)
			{
				index=catalogManager.getIndexFromTable(interpreterManager.oTable.tableName,i);
				if (index.indexName != "")
				{   
					indexManager.dropIndex(index);
				}
			}
			catalogManager.dropTable(interpreterManager.oTable.tableName);
			cout<<"Table "<<interpreterManager.oTable.tableName<<" has been dropped successfully."<<endl;
			break;
		case INSERT:
			table=interpreterManager.oTable;
			if (interpreterManager.primaryKeyIndex==-1&&interpreterManager.uniqueKeyIndex==-1)
			{
				recordManager.insertTuple(table,interpreterManager.oTuple);
				catalogManager.updateTable(table);
				cout<<"One tuple has been inserted successfully."<<endl;
				break;
			}
			if (interpreterManager.primaryKeyIndex!=-1)
			{
				tupleset=recordManager.select(table,interpreterManager.oConditionList);
				if (tupleset.tuples.size()>0)
				{
					cout<<"Primary key repeated, insertion failed."<<endl;
					break;
				}
			}
			if (interpreterManager.uniqueKeyIndex!=-1)
			{
				tupleset=recordManager.select(table,interpreterManager.oConditionList);
				if (tupleset.tuples.size()>0)
				{
					cout<<"Unique key repeated, insertion failed."<<endl;
					break;
				}
			}
			recordManager.insertTuple(table,interpreterManager.oTuple);
			catalogManager.updateTable(table);
			
			for (int i = 0; i < table.attributeNum; i++)
			{
				Index tempIndex = catalogManager.getIndex(table.attributeList[i].attributeName);
				if (tempIndex.indexName != "")
				{
					indexManager.insertTuple(tempIndex, table, interpreterManager.oTuple.attributeValueList[i]);
					catalogManager.updateIndex(tempIndex);
				}
			}
			
			cout<<"One tuple has been inserted successfully."<<endl;
			break;
		case INSERTERROR:
			cout<<"Your insertion grammar is wrong."<<endl;
			break;
		case SELECTALL:
		case SELECTCONDITION:
			table=interpreterManager.oTable;
			tupleset=recordManager.select(table,interpreterManager.oConditionList);
			if (tupleset.tuples.size()!=0) printResult(tupleset,table,interpreterManager.oAttributeList);
			else cout<<"No tuples can be found."<<endl;
			break;
		case DELETE:
		case DELETEALL:
			tupleCount=recordManager.deleteTuple(interpreterManager.oTable,interpreterManager.oConditionList);
			for (int i = 0; i < table.attributeNum; i++)
			{
				Index tempIndex = catalogManager.getIndex(table.attributeList[i].attributeName);
				if (tempIndex.indexName != "")
				{
					indexManager.deleteTuple(tempIndex, table, interpreterManager.oTuple.attributeValueList[i]);
					catalogManager.updateIndex(tempIndex);
				}
			}
			cout<<tupleCount<<" rows has been deleted successfully."<<endl;
			break;
		case CREATEINDEX:
			table=interpreterManager.oTable;
			index=interpreterManager.oIndex;
			if (!table.attributeList[index.attributeIndex].isPrimaryKey&&!table.attributeList[index.attributeIndex].isUnique)
			{
				cout<<"Attribute "<<table.attributeList[index.attributeIndex].attributeName<<" is not unique."<<endl;
				break;
			}
			catalogManager.createIndex(index);
			indexManager.createIndex(table,index);
			catalogManager.updateIndex(index);
			cout<<"The index "<<index.indexName<<" has been created successfully."<<endl;
			break;
		case CREATEINDEXERROR:
			cout<<"Your index create grammar is wrong."<<endl;
			break;
		case INDEXEXISTED:
			cout<<"The index has existed."<<endl;
			break;
		case DROPINDEX:
			index=catalogManager.getIndex(interpreterManager.oIndexName);
			if (index.indexName=="")
			{
				cout<<"Index"<<interpreterManager.oIndexName<<" does not exist."<<endl;
				break;
			}
     		indexManager.dropIndex(index);
			catalogManager.dropIndex(interpreterManager.oIndexName);
			cout<<"The index has been dropped successfully."<<endl;
			break;
		case QUIT:
			cout<<"Thank you for use:)"<<endl;
			getchar();
			exit(0);
			break;
		case INDEXNOTEXIST:
			cout<<"The index does not exist."<<endl;
			break;
		case DROPINDEXERROR:
			cout<<"Your drop index grammar is wrong."<<endl;
			break;
		case DROPTABLEERROR:
			cout<<"Your drop table grammar is wrong."<<endl;
			break;
		case INSERTNUMBERERROR:
			cout<<"The number of attribute is wrong."<<endl;
			break;
		case TABLENOTEXIST:
			cout<<"The table does not exist."<<endl;
			break;
		case DELETEERROR:
			cout<<"Your delete grammar is wrong."<<endl;
			break;
		case UNVALIDUNIQUE:
			cout<<"The unique key is unvalid."<<endl;
			break;
		case UNVALIDPRIMARYKEY:
			cout<<"The primary key is unvalid."<<endl;
			break;
		case CHARLENGTHERROR:
			cout<<"The char length is not valid."<<endl;
			break;
		case CREATETABLEERROR:
			cout<<"Your create table grammar is wrong."<<endl;
			break;
		case ATTRIBUTEERROR:
			cout<<"The attribute is wrong."<<endl;
			break;
		case SELECTERROR:
			cout<<"Your select grammar is wrong."<<endl;
			break;
	}
}

void printResult(TupleSet data, Table table, vector<Attribute> showAttributeList)
{
	if(showAttributeList[0].attributeName == "*")
	{
		cout << endl <<"+";
		for(int i = 0; i < table.attributeNum; i++)
		{
			for(int j = 0; j < table.attributeList[i].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;
		cout << "| ";
		for(int i = 0; i < table.attributeNum; i++)
		{
			cout << table.attributeList[i].attributeName;
			int lengthLeft = table.attributeList[i].attributeLength - table.attributeList[i].attributeName.length();
			for(int j = 0; j < lengthLeft; j++)
			{
				cout << ' ';
			}
			cout << "| ";
		}
		cout << endl;
		cout << "+";
		for(int i = 0; i < table.attributeNum; i++)
		{
			for(int j = 0; j < table.attributeList[i].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;

		for(int i = 0; i < data.tuples.size(); i++)
		{
			cout << "| ";
			for(int j = 0; j < table.attributeNum; j++)
			{
				int lengthLeft = table.attributeList[j].attributeLength;
				for(int k =0; k < data.tuples[i].attributeValueList[j].length(); k++)
				{
					if(data.tuples[i].attributeValueList[j].c_str()[k] == EMPTY) 
						break;
					else
					{
						cout << data.tuples[i].attributeValueList[j].c_str()[k];
						lengthLeft--;
					}
				}
				for(int k = 0; k < lengthLeft; k++) cout << " ";
				cout << "| ";
			}
			cout << endl;
		}

		cout << "+";
		for(int i = 0; i < table.attributeNum; i++)
		{
			for(int j = 0; j < table.attributeList[i].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;
	}
	else
	{
		cout << endl <<"+";
		for(int i = 0; i < showAttributeList.size(); i++)
		{
			int col;
			for(col = 0; col < table.attributeList.size(); col++)
			{
				if(table.attributeList[col].attributeName == showAttributeList[i].attributeName) 
					break;
			}
			for(int j = 0; j < table.attributeList[col].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;
		cout << "| ";
		for(int i = 0; i < showAttributeList.size(); i++)
		{
			int col;
			for(col = 0; col < table.attributeList.size(); col++)
			{
				if(table.attributeList[col].attributeName == showAttributeList[i].attributeName) 
					break;
			}
			cout << table.attributeList[col].attributeName;
			int lengthLeft = table.attributeList[col].attributeLength - table.attributeList[col].attributeName.length();
			for(int j = 0; j < lengthLeft; j++)
			{
				cout << ' ';
			}
			cout << "| ";
		}
		cout << endl;
		cout << "+";
		for(int i = 0; i < showAttributeList.size(); i++)
		{
			int col;
			for(col = 0; col < table.attributeList.size(); col++)
			{
				if(table.attributeList[col].attributeName == showAttributeList[i].attributeName) 
					break;
			}
			for(int j = 0; j < table.attributeList[col].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;

		for(int i = 0; i < data.tuples.size(); i++)
		{
			cout << "| ";
			for(int j = 0; j < showAttributeList.size(); j++)
			{
				int col;
				for(col = 0; col < table.attributeList.size(); col++)
				{
					if(table.attributeList[col].attributeName == showAttributeList[j].attributeName) 
					break;
				}
				int lengthLeft = table.attributeList[col].attributeLength;
				for(int k =0; k < data.tuples[i].attributeValueList[col].length(); k++)
				{
					if(data.tuples[i].attributeValueList[col].c_str()[k] == EMPTY) 
						break;
					else
					{
						cout << data.tuples[i].attributeValueList[col].c_str()[k];
						lengthLeft--;
					}
				}
				for(int k = 0; k < lengthLeft; k++) cout << " ";
				cout << "| ";
			}
			cout << endl;
		}

		cout << "+";
		for(int i = 0; i < showAttributeList.size(); i++)
		{
			int col;
			for(col = 0; col < table.attributeList.size(); col++)
			{
				if(table.attributeList[col].attributeName == showAttributeList[i].attributeName) 
					break;
			}
			for(int j = 0; j < table.attributeList[col].attributeLength + 1; j++)
			{
				cout << "-";
			}
			cout << "+";
		}
		cout << endl;
	}
	cout << data.tuples.size() << " rows have been found."<< endl;
}

void addEnd(char *command)
{
	unsigned len = strlen(command);
	command[len] = ' ';
	command[len + 1] = '\0';
}

bool judgeEnd(char *input)
{
	unsigned int next = strlen(input) - 1;
	char prev = ' ';
	while(next >= 0 && (prev == '\t' || prev ==' '))
	{
		prev = input[next];
		next --;
	}
	if(prev == ';')
	{
		input[next + 1] ='\0';
		return 1;
	}
	return 0;
}

#endif

