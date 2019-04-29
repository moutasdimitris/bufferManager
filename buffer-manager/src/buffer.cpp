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

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }

/* 
 Write all the modified pages to disk and free the memory
 allocated for buf description and the hashtable.
*/
    BufMgr::~BufMgr() {
        for (FrameId i = 0; i < numBufs; i++) {
            if (bufDescTable[i].dirty == true)
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
        clockHand++;
        if (clockHand == numBufs) {
            clockHand = 0;
        }
    }

/*
 This function allocates a new frame in the buffer pool 
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
    void BufMgr::allocBuf(FrameId &frame) {
        int c = 0; //Counter checks if all pages are pinned
        while (c <= numBufs) {
            if (!bufDescTable[clockHand].valid) {
                frame = clockHand;
                return;
            } else {
                //Victim page (refbit and dirty bit are 0)
                if (bufDescTable[clockHand].pinCnt == 0 && !bufDescTable[clockHand].dirty) {
                    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                    frame = clockHand;
                    return;
                    //
                } else if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty) {
                    flushFile(bufDescTable[clockHand].file);
                    frame = clockHand;
                    return;
                } else if (bufDescTable[clockHand].pinCnt > 0) {
                    c++;
                    advanceClock();
                    //reference bit != 0
                } else if (bufDescTable[clockHand].refbit) {
                    bufDescTable[clockHand].refbit = false;
                    advanceClock();
                }
            }
        }

        // if all pages are pinned, throw BufferExceededException
        throw BufferExceededException();
    }

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page. 
*/
//ok
    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameNo;    // used to get frameNumber
        try {
            // get frameNumber and set the refbit, and then increase pinCount
            // also, return the ptr to the page
            hashTable->lookup(file, pageNo, frameNo);
            bufDescTable[frameNo].refbit = true;
            bufDescTable[frameNo].pinCnt++;
            page = &bufPool[frameNo];
        }
        catch (HashNotFoundException e) {
            // if the page is not yet existed in the hashtable, allocate it and
            // set the bufDescTable
            // also, return the ptr to the page and its pageNo
            FrameId frameFree;
            allocBuf(frameFree);
            bufPool[frameFree] = file->readPage(pageNo);
            hashTable->insert(file, pageNo, frameFree);
            bufDescTable[frameFree].Set(file, pageNo);
            page = &bufPool[frameFree];
        }
    }


/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
        void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
            FrameId tempFrameId;

            try {
                hashTable->lookup(file, pageNo, tempFrameId);

                //if pinCnt is 0 (page is not pinned) it's throws an exception.
                if (bufDescTable[tempFrameId].pinCnt == 0) {
                    throw PageNotPinnedException(file->filename(), bufDescTable[tempFrameId].pageNo, tempFrameId);
                    //When pinCnt is >0 then decrement the pinCnt
                } else if (bufDescTable[tempFrameId].pinCnt > 0) {
                    if (dirty) {
                        bufDescTable[tempFrameId].dirty = true;
                    }
                    bufDescTable[tempFrameId].pinCnt = bufDescTable[tempFrameId].pinCnt - 1;
                }
            } catch (HashNotFoundException e) {}
        }

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
        void BufMgr::flushFile(File *file) {
            // if the frame corresponding to the file is invalid, throw exception
            // if someone is referring to the frame, throw exception
            for (int i = 0; i < numBufs; i++) {
                if (!bufDescTable[i].valid && bufDescTable[i].file == file) {
                    throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                             bufDescTable[i].refbit);
                }
                if (bufDescTable[i].file == file && bufDescTable[i].pinCnt > 0) {
                    throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                }
                // if the frame is valid and dirty, flush it and remove it from hashtable
                // clear the bufDescTable as well
                if (bufDescTable[i].valid && bufDescTable[i].file == file) {
                    if (bufDescTable[i].dirty) {
                        bufDescTable[i].file->writePage(bufPool[i]);
                        bufDescTable[i].dirty = false;
                    }
                    hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
                    bufDescTable[i].Clear();
                }
            }
        }

/*
 This function allocates a new page and reads it into the buffer pool.
*/
        void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
            // allocate new frame by calling allocatePage
            // return the ptr to the page and also value of pageNo
            FrameId frameToAlloc;
            allocBuf(frameToAlloc);
            bufPool[frameToAlloc] = file->allocatePage();
            page = &bufPool[frameToAlloc];
            pageNo = page->page_number();
            hashTable->insert(file, pageNo, frameToAlloc);
            bufDescTable[frameToAlloc].Set(file, pageNo);

        }

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
        void BufMgr::disposePage(File *file, const PageId PageNo) {
            // dispose the page if it is existed
            // throw error if the page if pinned
            FrameId tempFrame;
            try {
                hashTable->lookup(file, PageNo, tempFrame);
                if (bufDescTable[tempFrame].pinCnt != 0) {
                    throw PagePinnedException(bufDescTable[tempFrame].file->filename(), bufDescTable[tempFrame].pageNo,
                                              bufDescTable[tempFrame].frameNo);
                }
                bufDescTable[tempFrame].Clear();
                hashTable->remove(file, PageNo);
                file->deletePage(PageNo);
                return;
            }
            catch (HashNotFoundException e) {
                file->deletePage(PageNo);
            }

            // if the page does not exist in the buffer pool, delete it from it file on disk as well
            // unsure what will happen if the page does not exist even on disk
            // opppss, will throw error without deleting anything
        }

        void BufMgr::printSelf(void) {
            BufDesc *tmpbuf;
            int validFrames = 0;

            for (std::uint32_t i = 0; i < numBufs; i++) {
                tmpbuf = &(bufDescTable[i]);
                std::cout << "FrameNo:" << i << " ";
                tmpbuf->Print();

                if (tmpbuf->valid == true)
                    validFrames++;
            }

            std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
        }


    }
