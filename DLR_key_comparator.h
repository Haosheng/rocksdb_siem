#ifndef STORAGE_ROCKSDB_INCLUDE_DLRCOMPARATOR_
#define STORAGE_ROCKSDB_INCLUDE_DLRCOMPARATOR_

#include <cstring>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/key_op.h"

#define ID_SIZE 26

typedef uint32_t cp_t;
typedef double ts_t;

namespace rocksdb {
	class DLRKeyComparator : public Comparator {
	public:
		int Compare(const Slice& a, const Slice& b) const override {
			cp_t a1, a2;
			ts_t b1, b2;
			char s1[ID_SIZE+1];
			char s2[ID_SIZE+1];
			ko.ParseKey(a, a1, b1, s1);
			ko.ParseKey(b, a2, b2, s2);
			if (a1 < a2){
				return -1;
			} 
			if (a1 > a2){
				return +1;
			} 
			if (b1 < b2) {
				return -1;
			}
			if (b1 > b2) {
				return +1;
			}
			//return 0;
			return strcmp(s1,s2);
    	}

    	virtual const char* Name() const override  { return "DLRKeyComparator"; }
    	void FindShortestSeparator(std::string*, const Slice&) const override{ }
    	void FindShortSuccessor(std::string*) const override{ }
	private:
		Key_Operator ko;
	};
}

#endif
