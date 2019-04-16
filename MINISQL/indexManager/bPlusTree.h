#ifndef _BPLUSTREE_H_
#define _BPLUSTREE_H_
#include "..\minisql.h"
#include "..\bufferManager\bufferManager.hpp"

class IndexLeaf
{
public:
	string key;
	int blockIndex;
	int dataPosition;

	IndexLeaf()
	{
		key="";
		blockIndex=0;
		dataPosition=0;
	}
	IndexLeaf(string k,int b, int d)
	{
		key=k;
		blockIndex=b;
		dataPosition=d;
	}
};

class IndexNoneL
{
public:
	string key;
	int childPtr;
	IndexNoneL()
	{
		key = "";
		childPtr = 0;
	}
	IndexNoneL(string k, int c)
	{
		key = k;
		childPtr = c;
	}
};



class bPlusTree
{
public:
	bool isRoot;
	int bufferIndex;
	int fatherPtr;
	int tupleNum;
	int attributeLength;

    bPlusTree(){}
	bPlusTree(int index)
	{
		bufferIndex=index;
		tupleNum=0;
	}

	int getPtr(int position)
	{
		int ptr=0;
		for (int i=position;i<position+POINTERLENGTH;i++)
		{
			ptr=ptr*10+bufferManager.bufferCache[bufferIndex].data[i]-'0';
		}
		return ptr;
	}

	int calcTupleNum()
	{
		int tupleNum=0;
		for (int i=2;i<6;i++)
		{
			if (bufferManager.bufferCache[bufferIndex].data[i]==EMPTY) break;
			tupleNum=tupleNum*10+bufferManager.bufferCache[bufferIndex].data[i]-'0';
		}
		return tupleNum;
	}
};


class bPlusBranch:public bPlusTree
{
public:
	list<IndexNoneL> nodeList;
	bPlusBranch(){}
	bPlusBranch(int index)
	{
		bufferIndex=index;
	}
	bPlusBranch(int bindex, Index& index)
	{
		bufferIndex=bindex;
		if (bufferManager.bufferCache[bufferIndex].data[0]=='R') isRoot=true;
		else isRoot=false;

		int tupleCount=calcTupleNum();
		tupleNum=0;
		fatherPtr=getPtr(6);
		attributeLength=index.attributeLength;
		int position=6+POINTERLENGTH;
		for (int i=0;i<tupleCount;i++)
		{
			string value="";
			for (int i=position;i<position+attributeLength;i++)
			{
				if (bufferManager.bufferCache[bufferIndex].data[i]==EMPTY) break;
				value+=bufferManager.bufferCache[bufferIndex].data[i];
			}
			position+=attributeLength;
			int ptrChild=getPtr(position);
			position+=POINTERLENGTH;
			IndexNoneL node(value,ptrChild);
			insert(node);
		}
	}

	~bPlusBranch()
	{
		if (isRoot) bufferManager.bufferCache[bufferIndex].data[0]='R';
		else bufferManager.bufferCache[bufferIndex].data[0]='_';
		bufferManager.bufferCache[bufferIndex].data[1]='_';

		char temp[5];
		int position=2;
		_itoa(tupleNum,temp,10);
		string tupleNumString=temp;
		while (tupleNumString.length()<4) tupleNumString=')'+tupleNumString;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,tupleNumString.c_str(),4);
        position+=4;

		_itoa(fatherPtr,temp,10);
		string fatherPtrString=temp;
		while (fatherPtrString.length()<POINTERLENGTH) fatherPtrString='0'+fatherPtrString;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,fatherPtrString.c_str(),POINTERLENGTH);
		position=6+POINTERLENGTH;

		list<IndexNoneL>::iterator i;
		for (i=nodeList.begin();i!=nodeList.end();i++)
		{
			string value=(*i).key;
			while (value.length()<attributeLength) value+=EMPTY;
			strncpy(bufferManager.bufferCache[bufferIndex].data+position,value.c_str(),attributeLength);
			position+=attributeLength;

			_itoa((*i).childPtr,temp,10);
			string ptrChild=temp;
			while (ptrChild.length()<POINTERLENGTH) ptrChild="0"+ptrChild;
			strncpy(bufferManager.bufferCache[bufferIndex].data+position,ptrChild.c_str(),POINTERLENGTH);
			position+=POINTERLENGTH;
		}
	}

	int insert(IndexNoneL node)
	{
		int count=0;
		tupleNum++;
		list<IndexNoneL>::iterator i=nodeList.begin();
		if (nodeList.size()==0)
		{
			nodeList.insert(i,node);
			return 0;
		}
		else
		{
			for (i=nodeList.begin();i!=nodeList.end();i++,count++)
			{
				if ((*i).key>node.key) break;
			}
			nodeList.insert(i,node);
			return count;
		}
	}

	int deleteNode(IndexNoneL node)
	{
		tupleNum--;
		int count=0;
		list<IndexNoneL>::iterator i=nodeList.begin();
		for (i=nodeList.begin();i!=nodeList.end();i++,count++)
		{
			if ((*i).key==node.key) break;
		}
		nodeList.erase(i);
		return count;
	}

	IndexNoneL getLast()
	{
		tupleNum--;
		IndexNoneL temp=nodeList.back();
		nodeList.pop_back();
		return temp;
	}

	IndexNoneL getFirst()
	{
		return nodeList.front();
	}
};


class bPlusLeaf:public bPlusTree
{
public:
	int nextSibling;
	int lastSibling;
	list<IndexLeaf> nodeList;

	bPlusLeaf(int index)
	{
		bufferIndex=index;
		tupleNum=0;
		nextSibling=lastSibling=0;
	}
	bPlusLeaf(int bindex,Index& index)
	{
		bufferIndex=bindex;
		if (bufferManager.bufferCache[bufferIndex].data[0]=='R') isRoot=true;
		else isRoot=false;
		
		int tupleCount=calcTupleNum();
		tupleNum=0;
		fatherPtr=getPtr(6);				
		lastSibling=getPtr(6+POINTERLENGTH);
		nextSibling=getPtr(6+2*POINTERLENGTH);
		attributeLength=index.attributeLength;

		int position=6+3*POINTERLENGTH;
		for (int i=0;i<tupleCount;i++)
		{
			string value="";
			for (int i=position;i<position+attributeLength;i++)
			{
				if (bufferManager.bufferCache[bufferIndex].data[i]==EMPTY) break;
				value+=bufferManager.bufferCache[bufferIndex].data[i];
			}
			position+=attributeLength;
			int blockIndex=getPtr(position);
			position+=POINTERLENGTH;
			int dataPosition=getPtr(position);
			position+=POINTERLENGTH;
			IndexLeaf node(value,blockIndex,dataPosition);
			insert(node);
		}
	}

	~bPlusLeaf()
	{
		if (isRoot) bufferManager.bufferCache[bufferIndex].data[0]='R';
		else bufferManager.bufferCache[bufferIndex].data[0]='_';
		bufferManager.bufferCache[bufferIndex].data[1]='L';

		char temp[5];
		_itoa(tupleNum,temp,10);
		string tupleNumString=temp;
		while (tupleNumString.length()<4) tupleNumString='0'+tupleNumString;

		int position=2;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,tupleNumString.c_str(),4);
		
		position+=4;
		_itoa(fatherPtr,temp,10);
		string fatherPtrString=temp;
		while (fatherPtrString.length()<POINTERLENGTH) fatherPtrString='0'+fatherPtrString;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,fatherPtrString.c_str(),POINTERLENGTH);
		position+=POINTERLENGTH;

		_itoa(lastSibling,temp,10);
		string lastSiblingString=temp;
		while (lastSiblingString.length()<POINTERLENGTH) lastSiblingString='0'+lastSiblingString;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,lastSiblingString.c_str(),POINTERLENGTH);
		position+=POINTERLENGTH;

		_itoa(nextSibling,temp,10);
		string nextSiblingString=temp;
		while (nextSiblingString.length()<POINTERLENGTH) nextSiblingString='0'+nextSiblingString;
		strncpy(bufferManager.bufferCache[bufferIndex].data+position,nextSiblingString.c_str(),POINTERLENGTH);
		position+=POINTERLENGTH;		

		list<IndexLeaf>::iterator i;
		for (i=nodeList.begin();i!=nodeList.end();i++)
		{
			string value=(*i).key;
			while (value.length()<attributeLength) value+=EMPTY;
			strncpy(bufferManager.bufferCache[bufferIndex].data+position,value.c_str(),attributeLength);
			position+=attributeLength;

			_itoa((*i).blockIndex,temp,10);
			string blockIndexString=temp;
			while (blockIndexString.length()<POINTERLENGTH) blockIndexString='0'+blockIndexString;
			strncpy(bufferManager.bufferCache[bufferIndex].data+position,blockIndexString.c_str(),POINTERLENGTH);
			position+=POINTERLENGTH;

			_itoa((*i).dataPosition,temp,10);
			string dataPositionString=temp;
			while (dataPositionString.length()<POINTERLENGTH) dataPositionString='0'+dataPositionString;
			strncpy(bufferManager.bufferCache[bufferIndex].data+position,dataPositionString.c_str(),POINTERLENGTH);
			position+=POINTERLENGTH;
		}
	}

	int insert(IndexLeaf node)
	{
		int count=0;
		tupleNum++;
		list<IndexLeaf>::iterator i=nodeList.begin();
		if (nodeList.size()==0)
		{
			nodeList.insert(i,node);
			return 0;
		}
		else
		{
			for (i=nodeList.begin();i!=nodeList.end();i++,count++)
			{
				if ((*i).key>node.key) break;
			}
			nodeList.insert(i,node);
		    return count;
		}
	}

	int deleteNode(IndexLeaf node)
	{
		tupleNum--;
		int count=0;
		list<IndexLeaf>::iterator i=nodeList.begin();
		for (i=nodeList.begin();i!=nodeList.end();i++,count++)
		{
			if ((*i).key==node.key) break;
		}
		nodeList.erase(i);
		return count;
	}


	IndexLeaf getLast()
	{
		tupleNum--;
		IndexLeaf temp=nodeList.back();
		nodeList.pop_back();
		return temp;
	}	

	IndexLeaf getFirst()
	{
		return nodeList.front();
	}	
};

#endif