/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

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

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
  
}

/*
 This function allocates a new frame in the buffer pool 
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
void BufMgr::allocBuf(FrameId & frame) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
   
}

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page. 
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
  
}

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
  
}

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
void BufMgr::flushFile(const File* file) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
  
}

/*
 This function allocates a new page and reads it into the buffer pool.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
	
	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */  
}

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
void BufMgr::disposePage(File* file, const PageId PageNo) {
  
	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
	
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