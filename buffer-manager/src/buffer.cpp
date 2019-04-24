/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include <src/exceptions/file_not_found_exception.h>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "bufHashTbl.h"
#include "types.h"


namespace badgerdb {


BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/* 
 Write all the modified pages to disk and free the memory
 allocated for buf description and the hashtable.
*/
BufMgr::~BufMgr() {
  for(FrameId i =0; i<numBufs;i++){
    if(bufDescTable[i].dirty == true)
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  free(bufDescTable);
  delete[] bufPool;
  free(hashTable);
}

/* 
 Increment the clockhand within the circular buffer pool .
*/
void BufMgr::advanceClock() {
    bufPool->next_page_number();

}

/*
 This function allocates a new frame in the buffer pool 
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
void BufMgr::allocBuf(FrameId & frame) {
        bufDescTable[frame].frameNo = frame;
        bufDescTable[frame].valid = true;
        bufDescTable[frame].refbit = true;

}

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page. 
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
            hashTable->lookup(file,pageNo,bufDescTable->frameNo);
            if (file->exists(file->filename())){
                allocBuf(bufDescTable->frameNo);
                file->readPage(pageNo);
                hashTable->insert(file,pageNo,bufDescTable->frameNo);
                bufDescTable->Set(file,pageNo);
            }else{
                bufDescTable->refbit=false;
                bufDescTable->pinCnt++;
            }

}

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
    bufDescTable->pinCnt--;
        if(dirty==true){
            bufDescTable->dirty=true;
        }
        if (bufDescTable->pinCnt==0){
            throw PageNotPinnedException(file->filename(),pageNo,bufDescTable->frameNo);
        }

}

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
void BufMgr::flushFile(File* file) {
  for (int i=0;i< sizeof(numBufs);i++){
      if (bufDescTable->dirty){
          file->writePage(bufPool[i]);
          bufDescTable[i].dirty=false;
          if (bufDescTable->refbit){
                hashTable->remove(file,bufDescTable[i].pageNo);
          }
          bufDescTable->Clear();
      }
      if (bufDescTable[i].pinCnt>0){
          throw PagePinnedException(file->filename(),bufDescTable[i].pageNo,bufDescTable[i].frameNo);
      }
      if (!bufDescTable[i].valid){
          throw BadBufferException(bufDescTable[i].frameNo,bufDescTable[i].dirty,bufDescTable[i].valid,bufDescTable[i].refbit);
      }
  }
}

/*
 This function allocates a new page and reads it into the buffer pool.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
        file->allocatePage();
        allocBuf(bufDescTable->frameNo);
        hashTable->insert(file,pageNo,bufDescTable->frameNo);
        bufDescTable->Set(file,pageNo);
        bufDescTable->pageNo=pageNo;

}

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
void BufMgr::disposePage(File* file, const PageId PageNo) {
    if(bufDescTable->pageNo==PageNo){
        hashTable->remove(file,PageNo);
    }
    file->deletePage(PageNo);
	
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}



}
