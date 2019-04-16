#ifndef _BUFFER_H_
#define _BUFFER_H_
#include "..\minisql.h"

class bufferManager
{
public:
	block bufferCache[BUFFER_BLOCK_NUM];

	bufferManager();
	~bufferManager();
	void writeBack(int bufferIndex);
	int getBufferIndex(string fileName, int blockIndex);
	void cacheBlock(string fileName,int blockIndex,int bufferIndex);
	void recordBuffer(int bufferIndex);
	int getEmptyBuffer();
	int mallocBlockInTableFile(Table& table);
	int mallocBlockInIndexFile(Index& index);
	int checkInBuffer(string fileName,int blockIndex);
	void deleteFile(string fileName);
}bufferManager;

#endif