
## lab1 BUFFER POOL

### 1. LRU REPLACEMENT POLICY

* Pages are stored on disk. 

* Memory region organized as an array of fixed-size pages. An array entry is called a frame.

* When the DBMS requests a page, an exact copy is placed into one of these frames.

由上面的前置知识可知，LRUReplacer实际上管理的是一段用作page缓存的内存。