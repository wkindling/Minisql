#ifndef _BUFFERMANAGER_H_
#define _BUFFERMANAGER_H_
#include "buffer.h"

bufferManager::bufferManager()
{
	for (int bufferIndex=0;bufferIndex<BUFFER_BLOCK_NUM;bufferIndex++)
	{
		bufferCache[bufferIndex].emptyBlock();
	}
}

bufferManager::~bufferManager()
{
	for (int bufferIndex=0;bufferIndex<BUFFER_BLOCK_NUM;bufferIndex++)
	{
		writeBack(bufferIndex);
	}
}

void bufferManager::writeBack(int bufferIndex) 
{
	if (!bufferCache[bufferIndex].isChanged) return;

	const string fileName=bufferCache[bufferIndex].fileName;
	fstream fout(fileName.c_str(),ios::in|ios::out);
	fout.seekp(UNIT_BLOCKSIZE*bufferCache[bufferIndex].blockIndex,fout.beg);
	fout.write(bufferCache[bufferIndex].data,UNIT_BLOCKSIZE);
	bufferCache[bufferIndex].emptyBlock();
	fout.close();
}

int bufferManager::checkInBuffer(string fileName,int blockIndex)
{
	for (int bufferIndex=0;bufferIndex<BUFFER_BLOCK_NUM;bufferIndex++)
	{
		if (bufferCache[bufferIndex].fileName==fileName&&bufferCache[bufferIndex].blockIndex==blockIndex)
		{
			return bufferIndex;
		}
	}
	return -1;
}

int bufferManager::getEmptyBuffer()
{
	int bufferIndex=0;
	int maxLRU=-1;

	for (int i=0;i<BUFFER_BLOCK_NUM;i++)
	{
		if (bufferCache[i].isEmpty)
		{
			bufferCache[i].emptyBlock();
			bufferCache[i].isEmpty=false; // not empty
			bufferIndex=i;
		    return  bufferIndex;
		}
		else 
		{
			if (maxLRU<bufferCache[i].LRU)
			{
				maxLRU=bufferCache[i].LRU;
				bufferIndex=i;
			}
		}
	}
	writeBack(bufferIndex);
	bufferCache[bufferIndex].isEmpty=false;
	recordBuffer(bufferIndex);
	return bufferIndex;
}

void bufferManager::cacheBlock(string fileName,int blockIndex,int bufferIndex)
{
	bufferCache[bufferIndex].fileName=fileName;
	bufferCache[bufferIndex].blockIndex=blockIndex;
	bufferCache[bufferIndex].isEmpty=false;
	bufferCache[bufferIndex].isChanged=true;

	fstream fin(fileName.c_str(),ios::in);
	fin.seekp(UNIT_BLOCKSIZE*blockIndex,fin.beg);
	fin.read(bufferCache[bufferIndex].data,UNIT_BLOCKSIZE);
	fin.close();
    recordBuffer(bufferIndex);
}


int bufferManager::getBufferIndex(string fileName, int blockIndex)
{
	int bufferIndex=checkInBuffer(fileName,blockIndex);
	if (bufferIndex==-1)
	{
		bufferIndex=getEmptyBuffer();
		cacheBlock(fileName,blockIndex,bufferIndex);
	}
	return bufferIndex;
}

void bufferManager::recordBuffer(int bufferIndex)
{
	for (int i=0;i<BUFFER_BLOCK_NUM;i++)
	{
		if (i!=bufferIndex)
		{
			bufferCache[i].LRU++;
		}
		else 
		{
			bufferCache[i].LRU=0;
		}
	}
}

int bufferManager::mallocBlockInTableFile(Table& table)
{
	int bufferIndex=getEmptyBuffer();
	bufferCache[bufferIndex].isChanged=true;
	bufferCache[bufferIndex].isEmpty=false;
	bufferCache[bufferIndex].fileName=table.tableName+".table";
	bufferCache[bufferIndex].blockIndex=table.tableBlockNum++;
	recordBuffer(bufferIndex);
	return bufferIndex;
}

int bufferManager::mallocBlockInIndexFile(Index& index)
{
	int bufferIndex=getEmptyBuffer();
	bufferCache[bufferIndex].isChanged=true;
	bufferCache[bufferIndex].isEmpty=false;
	bufferCache[bufferIndex].fileName=index.indexName+".index";
	bufferCache[bufferIndex].blockIndex=index.indexBlockNum++;
	recordBuffer(bufferIndex);
	return bufferIndex;
}

void bufferManager::deleteFile(string fileName)
{
	for (int i=0;i<BUFFER_BLOCK_NUM;i++)
	{
		if (bufferCache[i].fileName==fileName)
		{
			 bufferCache[i].isEmpty=true;
			 bufferCache[i].isChanged=false;
		}
	}
}

#endif