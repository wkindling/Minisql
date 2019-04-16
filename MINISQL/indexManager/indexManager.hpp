#ifndef _INDEXMANAGER_H_
#define _INDEXMANAGER_H_
#include "index.h"

/*------------insert--------------*/
void indexManager::insertTuple(Index& index,Table& table, string key)
{
	IndexLeaf node=findInsertLeafNode(table,index,key);
	insertNode(index,node);
}

IndexLeaf indexManager::findInsertLeafNode(Table& table,Index& index,string key)
{
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
    		int position=i*tupleLength;
    		string tupleString=bufferManager.bufferCache[bufferIndex].readBlockSeg(position,position+tupleLength);
    		if (tupleString.c_str()[0]==EMPTY) continue;
    		string value=getAttributeValue(table,index,tupleString);
  			if (key==value)
    		{
    			IndexLeaf node(value,blockIndex,i);
    			return node;
    		}
    	}
    }
}

IndexNoneL indexManager::insertNode(Index& index, IndexLeaf node, int blockIndex)//blockIndex belongs to index
{
	IndexNoneL indexBranch;
	string fileName = index.indexName + ".index";
	int bufferIndex = bufferManager.getBufferIndex(fileName, blockIndex);
	bool isLeaf = (bufferManager.bufferCache[bufferIndex].data[1] == 'L');
	if (isLeaf)
	{
		bPlusLeaf leaf(bufferIndex, index);
		leaf.insert(node);
		int tupleLength = index.attributeLength + POINTERLENGTH * 2;
		int maxTupleNum = (UNIT_BLOCKSIZE - 6 - 3 * POINTERLENGTH) / tupleLength;

		if (leaf.tupleNum>maxTupleNum)
		{
			if (leaf.isRoot)
			{
				int rBufferIndex = leaf.bufferIndex;
				leaf.bufferIndex = bufferManager.mallocBlockInIndexFile(index);
				int sBufferIndex = bufferManager.mallocBlockInIndexFile(index);
				bPlusBranch newBranchRoot(rBufferIndex);
				bPlusLeaf newLeaf(sBufferIndex);

				newBranchRoot.isRoot = true;
				newLeaf.isRoot = false;
				leaf.isRoot = false;

				newBranchRoot.fatherPtr = newLeaf.fatherPtr = leaf.fatherPtr = 0;
				newBranchRoot.attributeLength = newLeaf.attributeLength = leaf.attributeLength;
				newLeaf.lastSibling = bufferManager.bufferCache[leaf.bufferIndex].blockIndex;
				leaf.nextSibling = bufferManager.bufferCache[newLeaf.bufferIndex].blockIndex;
				while (newLeaf.nodeList.size()<leaf.nodeList.size())
				{
					IndexLeaf tempNode = leaf.getLast();
					newLeaf.insert(tempNode);
				}

				IndexNoneL rootNode;
				rootNode.key = newLeaf.getFirst().key;
				rootNode.childPtr = bufferManager.bufferCache[newLeaf.bufferIndex].blockIndex;
				newBranchRoot.insert(rootNode);
				rootNode.key = leaf.getFirst().key;
				rootNode.childPtr = bufferManager.bufferCache[leaf.bufferIndex].blockIndex;
				newBranchRoot.insert(rootNode);
				return indexBranch;
			}
			else
			{
				bufferIndex = bufferManager.mallocBlockInIndexFile(index);
				bPlusLeaf newLeaf(bufferIndex);
				newLeaf.isRoot = false;
				newLeaf.fatherPtr = leaf.fatherPtr;
				newLeaf.attributeLength = leaf.attributeLength;

				newLeaf.nextSibling = leaf.nextSibling;
				newLeaf.lastSibling = bufferManager.bufferCache[leaf.bufferIndex].blockIndex;
				leaf.nextSibling = bufferManager.bufferCache[newLeaf.bufferIndex].blockIndex;
				if (newLeaf.nextSibling != 0)
				{
					bufferIndex = bufferManager.getBufferIndex(fileName, newLeaf.nextSibling);
					bPlusLeaf nextNode(bufferIndex, index);
					nextNode.lastSibling = bufferManager.bufferCache[newLeaf.bufferIndex].blockIndex;
				}

				while (newLeaf.nodeList.size()<leaf.nodeList.size())
				{
					IndexLeaf tempNode = leaf.getLast();
					newLeaf.insert(tempNode);
				}
				indexBranch.key = newLeaf.getFirst().key;
				indexBranch.childPtr = leaf.nextSibling;
				return indexBranch;
			}
		}
		else  return indexBranch;
	}
	else
	{
		bPlusBranch thisBranch(bufferIndex, index);
		list<IndexNoneL>::iterator i = thisBranch.nodeList.begin();
		if ((*i).key>node.key) (*i).key = node.key;
		else
		{
			for (i = thisBranch.nodeList.begin(); i != thisBranch.nodeList.end(); i++)
				if ((*i).key>node.key) break;
			i--;
		}
		IndexNoneL bNode = insertNode(index, node, (*i).childPtr);  //插入他的儿子，直到插到叶子，再递归回来
		if (bNode.key == "") return indexBranch;
		else
		{
			thisBranch.insert(bNode);
			int tupleLength = index.attributeLength + POINTERLENGTH;
			int maxTupleNum = (UNIT_BLOCKSIZE - 6 - POINTERLENGTH) / tupleLength;
			if (thisBranch.tupleNum>maxTupleNum)
			{
				if (thisBranch.isRoot)
				{
					int rBufferIndex = thisBranch.bufferIndex;
					thisBranch.bufferIndex = bufferManager.mallocBlockInIndexFile(index);
					int sBufferIndex = bufferManager.mallocBlockInIndexFile(index);
					bPlusBranch newBranchRoot(rBufferIndex);
					bPlusBranch newBranch(sBufferIndex);

					newBranchRoot.isRoot = true;
					newBranch.isRoot = false;
					thisBranch.isRoot = false;

					newBranchRoot.fatherPtr = newBranch.fatherPtr = thisBranch.fatherPtr = 0;
					newBranchRoot.attributeLength = newBranch.attributeLength = thisBranch.attributeLength;

					while (newBranch.nodeList.size()<thisBranch.nodeList.size())
					{
						IndexNoneL tempNode = thisBranch.getLast();
						newBranch.insert(tempNode);
					}

					IndexNoneL rootNode;
					rootNode.key = newBranch.getFirst().key;
					rootNode.childPtr = bufferManager.bufferCache[newBranch.bufferIndex].blockIndex;
					newBranchRoot.insert(rootNode);
					rootNode.key = thisBranch.getFirst().key;
					rootNode.childPtr = bufferManager.bufferCache[thisBranch.bufferIndex].blockIndex;
					newBranchRoot.insert(rootNode);
					return indexBranch;
				}
				else
				{
					bufferIndex = bufferManager.mallocBlockInIndexFile(index);
					bPlusBranch newBranch(bufferIndex);
					newBranch.isRoot = false;
					newBranch.fatherPtr = thisBranch.fatherPtr;
					newBranch.attributeLength = thisBranch.attributeLength;

					while (newBranch.nodeList.size()<thisBranch.nodeList.size())
					{
						IndexNoneL tempNode = thisBranch.getLast();
						newBranch.insert(tempNode);
					}
					indexBranch.key = newBranch.getFirst().key;
					indexBranch.childPtr = bufferManager.bufferCache[bufferIndex].blockIndex;
					return indexBranch;
				}
			}
			else
			{
				return indexBranch;
			}
		}
	}
	return indexBranch;
}

/*------------delete--------------*/
void indexManager::deleteTuple(Index& index,Table& table, string key)
{
	IndexLeaf node=findDropLeafNode(table,index,key);
	deleteNode(index, node);
}

IndexLeaf indexManager::findDropLeafNode(Table& table, Index& index,string key,int blockIndex)
{
	string fileName=index.indexName=".index";
	int bufferIndex=bufferManager.getBufferIndex(fileName,blockIndex);
	bool isLeaf=(bufferManager.bufferCache[bufferIndex].data[1]=='L');
	if (isLeaf)
	{
		bPlusLeaf leaf(bufferIndex,index);
		list<IndexLeaf>::iterator i=leaf.nodeList.begin();
		for (i=leaf.nodeList.begin();i!=leaf.nodeList.end();i++)
		{
			if ((*i).key==key)
			{
				return *i;
			}
		}
	}
	else 
	{
		bPlusBranch branch(bufferIndex,index);
        list<IndexNoneL>::iterator i=branch.nodeList.begin();
        for (i=branch.nodeList.begin();i!=branch.nodeList.end();i++) //如果不是叶子节点，先找branch的nodelist，找到位置后插入下一个childPtr
        {
            if ((*i).key>key)
            {
                i--;
                break;
            }
        }
        if (i==branch.nodeList.end()) i--;
        return findDropLeafNode(table,index,key,(*i).childPtr);
	}
}

void indexManager::deleteNode(Index& index, IndexLeaf node, int blockIndex)
{
	string fileName=index.indexName+".index";
	int bufferIndex=bufferManager.getBufferIndex(fileName,blockIndex);
	bool isLeaf=(bufferManager.bufferCache[bufferIndex].data[1]=='L');
	if (isLeaf)
	{
		bPlusLeaf leaf(bufferIndex,index);
		int nodeIndex=leaf.deleteNode(node);
		int tupleLength=index.attributeLength+POINTERLENGTH*2;
		int maxTupleNum=(UNIT_BLOCKSIZE-6-3*POINTERLENGTH)/tupleLength;

		if (leaf.tupleNum>maxTupleNum/2)//不用分裂，只用更新节点信息
		{
			if (leaf.isRoot) //是根
			{
				return;
			}
			else 	//不是根
			{
				if (nodeIndex==0)
				{
					bufferIndex=bufferManager.getBufferIndex(fileName,leaf.fatherPtr);
					bPlusBranch fatherBranch(bufferIndex,index);
					IndexNoneL tempnode = fatherBranch.getFirst();
					tempnode.key=node.key;
					tempnode.childPtr=bufferManager.bufferCache[leaf.bufferIndex].blockIndex;
					return;
				}
				else return;
			}
		}
		else if (leaf.tupleNum<maxTupleNum/2)//需要分裂
		{
			if (leaf.isRoot)//如果是根节点
			{
				return;
			}
			else 
			{
				bufferIndex=bufferManager.getBufferIndex(fileName,leaf.lastSibling);
				bPlusLeaf lastNode(bufferIndex,index);
				bufferIndex=bufferManager.getBufferIndex(fileName,leaf.nextSibling);
				bPlusLeaf nextNode(bufferIndex,index);
				if (lastNode.tupleNum>maxTupleNum/2)
				{
					IndexLeaf tempNode=lastNode.getLast();
					int tempIndex=leaf.insert(tempNode);
					if (tempIndex!=0) return;
					else 
					{
						bufferIndex=bufferManager.getBufferIndex(fileName,leaf.fatherPtr);
						bPlusBranch fatherBranch(bufferIndex,index);
						fatherBranch.getFirst().key=leaf.getFirst().key;
						return;
					}
				}
				else if(nextNode.tupleNum>maxTupleNum/2)
				{
					IndexLeaf tempNode=nextNode.getFirst();
					nextNode.deleteNode(tempNode);
					leaf.insert(tempNode);
					return;
				}
				else //左右邻居都借不了节点 
				{
					if (leaf.tupleNum+lastNode.tupleNum<=maxTupleNum)
					{
						while (leaf.nodeList.size()!=0)
						{
							IndexLeaf tempNode=leaf.getLast();
							lastNode.insert(tempNode);
						}
					}
					else if (leaf.tupleNum+nextNode.tupleNum<=maxTupleNum)
					{
						while(leaf.nodeList.size()!=0)
						{
							IndexLeaf tempNode=leaf.getLast();
							nextNode.insert(tempNode);
						}
					}
					bufferIndex=bufferManager.getBufferIndex(fileName,leaf.fatherPtr);
					bPlusBranch branch(bufferIndex,index);
					list<IndexNoneL>::iterator i;
					for (i=branch.nodeList.begin();i!=branch.nodeList.end();i++)
					{
						if (node.key<(*i).key)
						{
							i--;
							break;
						}
					}
					IndexNoneL deletedNode=*i;
					branch.deleteNode(deletedNode);
					if (branch.tupleNum>=maxTupleNum/2) return;
					else 
					{
						while (1)
						{
							bufferIndex=bufferManager.getBufferIndex(fileName,branch.fatherPtr);
							bPlusBranch fatherbranch(bufferIndex,index);
							list<IndexNoneL>::iterator i;
							for (i=fatherbranch.nodeList.begin();i!=fatherbranch.nodeList.end();i++)
							{
								if ((*i).childPtr==bufferManager.bufferCache[branch.bufferIndex].blockIndex) break;
							}
							if (i!=fatherbranch.nodeList.begin())
							{
								i--;
								bufferIndex = bufferManager.getBufferIndex(fileName, (*i).childPtr);
								bPlusBranch lastNode(bufferIndex,index);
								if (lastNode.tupleNum>maxTupleNum/2)
								{
									IndexNoneL tempNode=lastNode.getLast();
									int tempindex=branch.insert(tempNode);
									if (tempindex!=0) return;
									else 
									{
										(*i).key=branch.getFirst().key;
										return;
									}
								}
								else if (lastNode.tupleNum+branch.tupleNum<maxTupleNum)
								{
									while (branch.nodeList.size()>0)
									{
										IndexNoneL tempNode=branch.getLast();
										lastNode.insert(tempNode);
									}
									branch=lastNode;
								}
							}
							else if (i!=fatherbranch.nodeList.end())
							{
								i++;
								bufferIndex = bufferManager.getBufferIndex(fileName, (*i).childPtr);
								bPlusBranch nextNode(bufferIndex,index);
								if (nextNode.tupleNum>maxTupleNum/2)
								{
									IndexNoneL tempNode=nextNode.getLast();
									int tempindex=branch.insert(tempNode);
									return;
								}
								else if (nextNode.tupleNum+branch.tupleNum<maxTupleNum)
								{
									while (branch.nodeList.size()>0)
									{
										IndexNoneL tempNode=branch.getLast();
										nextNode.insert(tempNode);
									}
									branch=nextNode;
								}
							}
						}
					}
				}
			}
		}
	}
	else 
	{
		bPlusBranch branch(bufferIndex,index);
		list<IndexNoneL>::iterator i=branch.nodeList.begin();
		if (node.key<(*i).key) deleteNode(index,node,(*i).childPtr);
		else 
		{
			for (i=branch.nodeList.begin();i!=branch.nodeList.end();i++)
			{
				if (node.key<(*i).key) 
				{
					i--;
					break;
				}
			}
			deleteNode(index,node,(*i).childPtr);
		}
	}
}
/*------------create--------------*/
void indexManager::createIndex(Table& table, Index& index)
{
	string fileName=index.indexName+".index";
	fstream fout(fileName.c_str(),ios::out);
	fout.close();

	int bufferIndex= bufferManager.getEmptyBuffer();
	bufferManager.bufferCache[bufferIndex].fileName=fileName;
	bufferManager.bufferCache[bufferIndex].blockIndex=0;
	bufferManager.bufferCache[bufferIndex].isChanged=true;
	bufferManager.bufferCache[bufferIndex].isEmpty=false;
	bufferManager.bufferCache[bufferIndex].data[0]='R';
	bufferManager.bufferCache[bufferIndex].data[1]='L';
	for (int i=2;i<(2+4+POINTERLENGTH*3);i++) bufferManager.bufferCache[bufferIndex].data[i]='0';
	index.indexBlockNum++;
    
    fileName=table.tableName+".table";
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
    		int position=i*tupleLength;
    		string tupleString=bufferManager.bufferCache[bufferIndex].readBlockSeg(position,position+tupleLength);
    		if (tupleString.c_str()[0]==EMPTY) continue;
    		tupleString.erase(tupleString.begin());
    		string value=getAttributeValue(table,index,tupleString);
    		IndexLeaf node(value,blockIndex,i);
    		insertNode(index,node,0);
    	}
    }
}
/*------------drop--------------*/
void indexManager::dropIndex(Index& index)
{
    string fileName=index.indexName+".index";
	if (remove(fileName.c_str()) != 0)
	{
		cout << fileName;
		perror("File deletion failed.");
	}
    else bufferManager.deleteFile(fileName); 
} 
/*------------equalSelect--------------*/
Tuple indexManager::equalSelect(Table& table, Index& index, string searchKey, int blockIndex)
{
    Tuple result;
    string fileName=index.indexName+".index";
    int bufferIndex=bufferManager.getBufferIndex(fileName,blockIndex);
    bool isLeaf=(bufferManager.bufferCache[bufferIndex].data[1]=='L');
    if (isLeaf)
    {
        bPlusLeaf leaf(bufferIndex,index);
        list<IndexLeaf>::iterator i=leaf.nodeList.begin();
        for (i=leaf.nodeList.begin();i!=leaf.nodeList.end();i++)
        {
            if ((*i).key==searchKey)
            {
                fileName=table.tableName+".table";
                int tupleBufferIndex=bufferManager.getBufferIndex(fileName,(*i).blockIndex);
                int position=(table.tupleLength+1)*((*i).blockIndex);
                string tupleString=bufferManager.bufferCache[tupleBufferIndex].readBlockSeg(position,position+table.tupleLength);
                if (tupleString.c_str()[0]!=EMPTY)
                {
                    tupleString.erase(tupleString.begin());
                    Tuple tuple=transferToTuple(table,tupleString);
                    result=tuple;
                    return result;    //如果是叶节点，先找到等于searchkey的索引，然后确定位置后在table中提取tuple
                }
            }
        }
    }
    else 
    {
        bPlusBranch branch(bufferIndex,index);
        list<IndexNoneL>::iterator i=branch.nodeList.begin();
        for (i=branch.nodeList.begin();i!=branch.nodeList.end();i++) //如果不是叶子节点，先找branch的nodelist，找到位置后插入下一个childPtr
        {
            if ((*i).key>searchKey)
            {
                i--;
                break;
            }
        }
        if (i==branch.nodeList.end()) i--;
        result=equalSelect(table,index,searchKey,(*i).childPtr);
    }
    return result;
}


string indexManager::getAttributeValue(Table& table, Index& index,string tuple)
{
    string attributeValue="";
    int start=0,end=0;
    for (int i=0;i<=index.attributeIndex;i++)
    {
        start=end;
        end+=table.attributeList[i].attributeLength;
    }
    for (int j=start;j<end && tuple[j]!=EMPTY;j++) attributeValue+=tuple[j];
    return attributeValue;
}

Tuple indexManager::transferToTuple(Table& table, string stringTuple)
{
	Tuple tuple;
	int start = 0, end = 0;
	string attributeValue;

	for (int i = 0; i<table.attributeNum; i++)
	{
		start = end;
		attributeValue = "";
		end = start + table.attributeList[i].attributeLength;
		for (int j = start; j<end; j++) attributeValue += stringTuple.c_str()[j];
		tuple.attributeValueList.push_back(attributeValue);
	}
	return tuple;
}

#endif