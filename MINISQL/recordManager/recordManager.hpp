#ifndef _RECORDMANAGER_H_
#define _RECORDMANAGER_H_
#include "record.h"

void recordManager::createTable(Table& table)
{
	const string fileName=table.tableName+".table";
	fstream fout(fileName.c_str(),ios::out);
	fout.close();
}

void recordManager::dropTable(Table& table)
{
	const string fileName=table.tableName+".table";
	if (remove(fileName.c_str()))
	{
		perror("File deletion failed!");
	}
	else 
	{
		bufferManager.deleteFile(fileName);
	}
}

void recordManager::insertTuple(Table& table,Tuple& tuple)
{
	string stringTuple=transferToString(table,tuple);
	//getInsertPos(table);
	int insertBufferIndex=-1;
	int insertDataPosition=-1;
	if (table.tableBlockNum==0)
	{
		insertBufferIndex=bufferManager.mallocBlockInTableFile(table);
		insertDataPosition=0;
	}
	else 
	{
		string fileName=table.tableName+".table";
		int tupleLength=table.tupleLength+1;
		int blockIndex=table.tableBlockNum-1;
		//cout <<"tupleLength:"<<tupleLength<<endl;
		int bufferIndex=bufferManager.checkInBuffer(fileName,blockIndex);
		if (bufferIndex==-1)
		{
			bufferIndex=bufferManager.getEmptyBuffer();
			bufferManager.cacheBlock(fileName,blockIndex,bufferIndex);
		}
		int maxTupleNum=UNIT_BLOCKSIZE/tupleLength;
		for (int i=0;i<maxTupleNum;i++)
		{
			int dataPosition=i*tupleLength;
			char data=bufferManager.bufferCache[bufferIndex].data[dataPosition];
			//cout << data << " ";
			if (data==EMPTY)
			{
				insertBufferIndex=bufferIndex;
				insertDataPosition=dataPosition;
				break;
			}
		}
		if (insertDataPosition==-1&&insertBufferIndex==-1)
		{
			insertBufferIndex=bufferManager.mallocBlockInTableFile(table);
			insertDataPosition=0;
		}
	}
	bufferManager.bufferCache[insertBufferIndex].data[insertDataPosition]=NOTEMPTY;
	for (int i=0;i<table.tupleLength;i++)
	{
		bufferManager.bufferCache[insertBufferIndex].data[insertDataPosition+i+1]=stringTuple.c_str()[i];
	}
	bufferManager.bufferCache[insertBufferIndex].isChanged=true;
	bufferManager.recordBuffer(insertBufferIndex);
}

bool recordManager::compare(Table& table, Tuple& tuple, vector<Condition> conditionList)
{
	for (unsigned int i = 0; i<conditionList.size(); i++)
	{
		int attributeIndex = conditionList[i].attributeIndex;
		string tupleValue = "";
		string conditionValue = conditionList[i].conditionValue;
		int tupleLength = 0;
		int conditionLength = conditionValue.length();
		for (int k = 0; k < tuple.attributeValueList[attributeIndex].length(); k++)
		{
			if (tuple.attributeValueList[attributeIndex].c_str()[k] == EMPTY)
			{
				tupleLength = k;
				break;
			}
			tupleValue += tuple.attributeValueList[attributeIndex].c_str()[k];
		}

		double tupleFloat = atof(tupleValue.c_str());
		double conditionFloat = atof(conditionValue.c_str());
		

		switch (table.attributeList[attributeIndex].attributeType)
		{
		case CHARTYPE:
			switch (conditionList[i].compareOp)
			{
			case lessThan:
				if (tupleValue >= conditionValue) return false;
				break;
			case lessEqual:
				if (tupleValue>conditionValue) return false;
				break;
			case equalTo:
				if (tupleValue != conditionValue) return false;
				break;
			case largerThan:
				if (tupleValue <= conditionValue) return false;
				break;
			case largerEqual:
				if (tupleValue<conditionValue) return false;
			case notEqual:
				if (tupleValue == conditionValue) return false;
				break;
			}
			break;
		case INTTYPE:
			switch (conditionList[i].compareOp)
			{
			case lessThan:
				if (tupleLength>conditionLength) return false;
				else if (tupleLength < conditionLength) break;
				else if (tupleValue >= conditionValue) return false;
				break;
			case lessEqual:
				if (tupleLength>conditionLength) return false;
				else if (tupleLength < conditionLength) break;
				else if (tupleValue > conditionValue) return false;
				break;
			case equalTo:
				if (tupleLength != conditionLength) return false;
				else if (tupleValue != conditionValue) return false;
				break;
			case largerThan:
				if (tupleLength<conditionLength) return false;
				else if (tupleLength > conditionLength) break;
				else if (tupleValue <= conditionValue) return false;
				break;
			case largerEqual:
				if (tupleLength<conditionLength) return false;
				else if (tupleLength > conditionLength) break;
				else if (tupleValue < conditionValue) return false;
				break;
			case notEqual:
				if (tupleLength != conditionLength) break;
				else if (tupleValue == conditionValue) return false;
				break;
			}
			break;
		case FLOATTYPE:
			switch (conditionList[i].compareOp)
			{
			case lessThan:
				if (tupleFloat >= conditionFloat) return false;
				break;
			case lessEqual:
				if (tupleFloat > conditionFloat) return false;
				break;
			case equalTo:
				if (tupleFloat != conditionFloat) return false;
				break;
			case largerThan:
				if (tupleFloat <= conditionFloat) return false;
				break;
			case largerEqual:
				if (tupleFloat < conditionFloat) return false;
				break;
			case notEqual:
				if (tupleFloat == conditionFloat) return false;
				break;
			}
			break;
		}
	}
	return true;
}

TupleSet recordManager::select(Table& table, vector<Condition>conditionList)
{
	TupleSet selectResult;
	string fileName=table.tableName+".table";
	int tupleLength=table.tupleLength+1;
	int maxTupleNum=UNIT_BLOCKSIZE/tupleLength;
	for (int blockIndex=0;blockIndex<table.tableBlockNum;blockIndex++)
	{
		int bufferIndex=bufferManager.checkInBuffer(fileName,blockIndex);
		if (bufferIndex==-1)
		{
			bufferIndex=bufferManager.getEmptyBuffer();
			bufferManager.cacheBlock(fileName,blockIndex,bufferIndex);
		}
		for (int i=0;i<maxTupleNum;i++)
		{
			int dataPosition=i*tupleLength;
			string stringTuple=bufferManager.bufferCache[bufferIndex].readBlockSeg(dataPosition,dataPosition+tupleLength);
			if (stringTuple.c_str()[0]!=EMPTY)
            {
            	stringTuple.erase(stringTuple.begin());
				Tuple tuple=transferToTuple(table,stringTuple);

		     if (conditionList.size()==0)
				{
					selectResult.tuples.push_back(tuple);
				}
				else
				{
					if (compare(table,tuple,conditionList))
					{
						selectResult.tuples.push_back(tuple);
					}
				}
            }
		}
	}
	return selectResult;
}

int recordManager::deleteTuple(Table& table, vector<Condition> conditionList)
{
	int deleteNum=0;
	string fileName=table.tableName+".table";
	int tupleLength=table.tupleLength+1;
	int maxTupleNum=UNIT_BLOCKSIZE/tupleLength;

	for (int blockIndex=0;blockIndex<table.tableBlockNum;blockIndex++)
	{
		int bufferIndex=bufferManager.checkInBuffer(fileName,blockIndex);
		if (bufferIndex==-1)
		{
			bufferIndex=bufferManager.getEmptyBuffer();
			bufferManager.cacheBlock(fileName,blockIndex,bufferIndex);
		}
		for (int i=0;i<maxTupleNum;i++)
		{
			int dataPosition=i*tupleLength;
			string stringTuple=bufferManager.bufferCache[bufferIndex].readBlockSeg(dataPosition,dataPosition+tupleLength);
			if (stringTuple.c_str()[0]!=EMPTY)
			{
				stringTuple.erase(stringTuple.begin());
				Tuple tuple=transferToTuple(table,stringTuple);
				if (conditionList.size()==0)
				{
					bufferManager.bufferCache[bufferIndex].data[dataPosition]=DELETED;
				    deleteNum++;
				}
				else 
				{
					if (compare(table,tuple,conditionList))
						{
							bufferManager.bufferCache[bufferIndex].data[dataPosition]=DELETED;
						    deleteNum++;
						}
				}
		    }
		}
        bufferManager.bufferCache[bufferIndex].isChanged=true;
	}
	return deleteNum;
}

Tuple recordManager::transferToTuple(Table& table,string stringTuple)
{
	Tuple tuple;
	int start=0,end=0;
	string attributeValue;

	for (int i=0;i<table.attributeNum;i++)
	{
		start=end;
		attributeValue="";
		end=start+table.attributeList[i].attributeLength;
		for (int j=start;j<end;j++) attributeValue+=stringTuple.c_str()[j];
		tuple.attributeValueList.push_back(attributeValue);
	}
	return tuple;
}

string recordManager::transferToString(Table& table,Tuple tuple)
{
	string stringTuple;
	for (unsigned int i=0;i<tuple.attributeValueList.size();i++)
	{
		string attributeValue=tuple.attributeValueList[i];
		while (attributeValue.length()<(unsigned int)table.attributeList[i].attributeLength) attributeValue+=EMPTY;
		stringTuple+=attributeValue;
	}
    return stringTuple;
}

#endif