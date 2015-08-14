#ifndef STORAGE_ROCKSDB_INCLUDE_KEYOP_
#define STORAGE_ROCKSDB_INCLUDE_KEYOP_

#include <cstring>
#include <string>
#include "rocksdb/slice.h"

typedef uint32_t cp_t;
typedef double ts_t;

namespace rocksdb{
	class Key_Operator{
	public:
		Key_Operator(){

		}
		void ParseKey(const Slice& a, cp_t& a1, ts_t& b1,char* s1)const {
		    cp_t* cp_ptr = (cp_t*)a.data();
		    a1=*cp_ptr;
		    cp_ptr++;
		    ts_t* ts_ptr = (ts_t*)cp_ptr;
		    b1 = *ts_ptr;
		    ts_ptr++;
		    
		    char* id_ptr = (char*)ts_ptr;
		    strcpy(s1,id_ptr);
		    
		}

		// //old version, should never be used in new code
		// void GenKey(std::string read, char* key){
		//     std::string delim = "_";
		//     size_t pos = 0;
		//     pos = read.find(delim);
		//     std::string cp_str = read.substr(0,pos);
		//     cp_t cp = std::stoul(cp_str);
		//     cp_t* cp_ptr;
		//     cp_ptr = (cp_t*)key;
		//     *cp_ptr = cp;
		//     cp_ptr++;
		//     read = read.substr(pos+1);
		//     pos = read.find(delim);
		//     std::string ts_str = read.substr(0,pos);
		//     //std::string dot=".";
		//     //size_t ms=ts_str.find(dot);
		//     //std::string sec=ts_str.substr(0,ms);
		//     //std::string msec=ts_str.substr(ms+1);
		//     //ts_t ts= std::stoull(sec)*1000+std::stoull(msec);
		//     ts_t ts = std::stoull(ts_str);
		//     ts_t* ts_ptr;
		//     ts_ptr = (ts_t*)cp_ptr;
		//     *ts_ptr = ts;
		//     ts_ptr++;
		    
		//     read = read.substr(pos+1);
		//     char* id_ptr;
		//     id_ptr = (char*)ts_ptr;
		//     strcpy(id_ptr,read.c_str());
		    
		// }

		void GenerateKey(const cp_t &cp,const ts_t &ts,const std::string &id, char* key){
		    cp_t* cp_ptr;
		    cp_ptr = (cp_t*)key;
		    *cp_ptr = cp;
		    cp_ptr++;

		    ts_t* ts_ptr;
		    ts_ptr = (ts_t*)cp_ptr;
		    *ts_ptr = ts;
		    ts_ptr++;

		    char* id_ptr;
		    id_ptr = (char*)ts_ptr;
		    strcpy(id_ptr,id.c_str());
		}



	
		// void GetSubFields(std::string read, std::string& cpcode, std::string& timestamp, std::string& id){
		// 	std::string delim = "_";
		//     size_t pos = 0;
		//     pos = read.find(delim);
		//     cpcode = read.substr(0,pos);
		//     read = read.substr(pos+1);
		//     pos = read.find(delim);
		//     timestamp = read.substr(0,pos);
		//     id = read.substr(pos+1);
		// }
	};
	
} //end rocksdb
#endif