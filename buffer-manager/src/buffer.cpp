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

        if(clockHand == numBufs-1){
            clockHand = 0;
        }else{
            clockHand++;
        }

    }

/*
 This function allocates a new frame in the buffer pool
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
    void BufMgr::allocBuf(FrameId & frame) {
        int pinCount = 0;
        while(pinCount <= numBufs){
            advanceClock(); //increase the clockHand

            //if the frame isn't valid (written), it's written.
            if (!bufDescTable[clockHand].valid){
                frame = bufDescTable[clockHand].frameNo;
                return;

            //if page had referenced now refbit is cleared
            }if(bufDescTable[clockHand].refbit){
                bufDescTable[clockHand].refbit = false;
                continue;
            //
            }if(bufDescTable[clockHand].pinCnt == 0){
                break;
            }else{
                pinCount++;
            }
        }

        if(pinCount > numBufs){
            throw BufferExceededException();
        }

        // if dirty is true the page is written to the file
        if(bufDescTable[clockHand].dirty){
            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            frame = bufDescTable[clockHand].frameNo;
        }else{
            frame = bufDescTable[clockHand].frameNo;
        }

        //remove the hashtable entry
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

        //clears the buffer
        bufDescTable[clockHand].Clear();
    }

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page.
*/
    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
        FrameId frameToSearch;
        try {

            //Search for the page in the hashTable. if it's not there throws an exception
            hashTable->lookup(file, pageNo, frameToSearch);

            page = &bufPool[frameToSearch];

            //Because the page is referenced refbit and pinCnt have to be increased.
            bufDescTable[frameToSearch].refbit = true;
            bufDescTable[frameToSearch].pinCnt++;
        } catch(HashNotFoundException& e) {
            try {
                allocBuf(frameToSearch);
                Page tempPage = file->readPage(pageNo);
                bufPool[frameToSearch] = tempPage;

                hashTable->insert(file, pageNo, frameToSearch);
                bufDescTable[frameToSearch].Set(file, pageNo);

                //return the page
                page = &bufPool[frameToSearch];
            } catch(BufferExceededException e) {}
        }
    }

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {

        FrameId tempFrameId;

        try {
            hashTable->lookup(file, pageNo, tempFrameId);

            //if pinCnt is 0 (page is not pinned) it's throws an exception.
            if(bufDescTable[tempFrameId].pinCnt == 0){
                throw PageNotPinnedException(file->filename(), bufDescTable[tempFrameId].pageNo, tempFrameId);
                //When pinCnt is >0 then decrement the pinCnt
            }else if(bufDescTable[tempFrameId].pinCnt > 0){
                if(dirty){
                    bufDescTable[tempFrameId].dirty = true;
                }
                bufDescTable[tempFrameId].pinCnt = bufDescTable[tempFrameId].pinCnt - 1;
            }
        }catch(HashNotFoundException e){}
    }

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
    void BufMgr::flushFile(const File* file) {

        for(int i=0; i< sizeof(bufDescTable);i++){
            if(!bufDescTable[i].valid && bufDescTable[i].file == file){
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }

            if(bufDescTable[i].file == file && bufDescTable[i].pinCnt>0){
                throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }

            if(bufDescTable[i].valid && bufDescTable[i].file == file){
                if(bufDescTable[i].dirty){
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
    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {

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
    void BufMgr::disposePage(File* file, const PageId PageNo) {

        FrameId tempFrame;

        try{
            hashTable->lookup(file, PageNo, tempFrame);
            if(bufDescTable[tempFrame].pinCnt > 0){
                throw PagePinnedException(bufDescTable[tempFrame].file->filename(), bufDescTable[tempFrame].pageNo, bufDescTable[tempFrame].frameNo);
            }
            hashTable->remove(file, PageNo);
            bufDescTable[tempFrame].Clear();
            file->deletePage(PageNo);
        }catch (HashNotFoundException e){
            file->deletePage(PageNo);
        }

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
