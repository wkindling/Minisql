//
//Created by Wen Jiahao on 10/06/2018.
//
#ifndef _MINISQL_H_
#define _MINISQL_H_

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <list>

/*Const List*/
#define UNIT_BLOCKSIZE 4096 //4KB
#define BUFFER_BLOCK_NUM 1000 
#define POINTERLENGTH 5  
#define COMMENDLENGTH 200 
#define INPUTLENGTH 200
#define UNDEFINED 1
#define INTLENGTH 20
#define FLOATLENGTH 20
#define CHARTYPE 14
#define EMPTY '~'
#define NOTEMPTY '1'
#define DELETED '~'
#define INTTYPE 100
#define CHARTYPE 101
#define FLOATTYPE 102
/*Operation List*/
#define QUIT 2
#define SELECTALL 5
#define SELECTCONDITION 7
#define CREATETABLE 18
#define CREATEINDEX 21
#define DELETEALL 23
#define DELETE 24
#define INSERT 27
#define DROPTABLE 29
#define DROPINDEX 32
#define EXEFILE 34
enum compareOperation{lessThan, lessEqual, equalTo, largerEqual, largerThan, notEqual};
/*Error List*/
#define SELECTERROR 3
#define TABLENOTEXIST 4
#define ATTRIBUTEERROR 6
#define CREATETABLEERROR 8
#define TABLEEXISTED 9
#define CHARLENGTHERROR 15
#define UNVALIDPRIMARYKEY 16
#define UNVALIDUNIQUE 17
#define CREATEINDEXERROR 19
#define INDEXEXISTED 20
#define DELETEERROR 22
#define INSERTERROR 25
#define INSERTNUMBERERROR 26
#define DROPTABLEERROR 28
#define DROPINDEXERROR 30
#define INDEXNOTEXIST 31
#define EXEERROR 33

using namespace std;


class block
{
public:
  string fileName;
  bool isChanged;
  bool isEmpty;
  int  blockIndex;
  int  LRU;
  char data[UNIT_BLOCKSIZE+1];

    block()
    {
      emptyBlock();
    }
    
    void emptyBlock()
    {
      fileName="unknown";
      isChanged=false;
      isEmpty=true; // is empty
      blockIndex=0;
      LRU=0;
      for (int i=0;i<UNIT_BLOCKSIZE;i++) data[i]=EMPTY;
      data[UNIT_BLOCKSIZE]='\0';
    }

    string readBlockSeg(int start,int end)
    {
      string content="";
      for (int i=start;i<end;i++)
      {
        content+=data[i];
      }
      return content;
    }

    char readBlockDot(int pos)
    {
      return data[pos];
    }
};

class Attribute
{
public:
   string attributeName;
   int attributeType;
   int attributeLength;
   bool isPrimaryKey;
   bool isUnique;

   Attribute(string n,int t,int l, bool p,bool u)
   {
   	  attributeName=n;
   	  attributeType=t;
   	  attributeLength=l;
   	  isPrimaryKey=p;
   	  isUnique=u;
   }
   Attribute()
   {
      isPrimaryKey=false;
      isUnique=false;
   }
};

class Tuple
{
public:
  vector<string> attributeValueList;
};

class TupleSet
{
public:
  vector<Tuple> tuples;
};

class Table
{
public:
	string tableName;
	int attributeNum;
	int tupleLength;
	int tableBlockNum;
	vector<Attribute> attributeList;

	Table()
	{
		attributeNum=0;
		tupleLength=0;
		tableBlockNum=0;
	}
	
  void operator=(const Table& T)
  {
    tableName=T.tableName;
    attributeNum=T.attributeNum;
    tupleLength=T.tupleLength;
    tableBlockNum=T.tableBlockNum;
    attributeList=T.attributeList;
  }   
};

class Index
{
public:
	string indexName;
    string tableName;
    int attributeIndex;
    int attributeLength;
    int indexBlockNum;

    Index()
    {
    	attributeIndex=0;
    	attributeLength=0;
    	indexBlockNum=0;
    }
    
    void operator=(const Index& I)
    {
        indexName=I.indexName;
        tableName=I.tableName;
        attributeIndex=I.attributeIndex;
        attributeLength=I.attributeLength;
        indexBlockNum=I.indexBlockNum;
    }
};

class Condition
{
public:
  string conditionValue;
  string tableName;
  int attributeIndex;
  compareOperation compareOp;
};

#endif