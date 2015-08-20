#include <iostream>
#include <fstream>
#include <string>
//#include <unordered_map>
#include <vector>
#include <ctime>
//#include <sstream>
//#include <exception>

#include "rocksdb/db.h"
//#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/DLR_key_comparator.h"
#include "rocksdb/key_op.h"

//#include <boost/property_tree/ptree.hpp>
//#include <boost/property_tree/json_parser.hpp>

using namespace rocksdb;

#define ID_SIZE 26
#define KEYSIZE (sizeof(uint32_t)+sizeof(double)+ID_SIZE+1)
// 6 hours time interval
#define TIME_INTERVAL 21600

typedef uint32_t cp_t;
typedef double ts_t;

static std::string kDBdir="/scratch/database/";
static DLRKeyComparator cmp;
static Options options;

uint32_t GetTimeBucket(ts_t timestamp) { 
	std::time_t cur_time = std::time(0);
	uint32_t gap = cur_time - (uint32_t)timestamp;
	if(gap >= TIME_INTERVAL){
		return 38;
	}
	uint32_t bucket_num = gap/600;
	return bucket_num;
}

bool read_db_count(std::vector<std::string> &result, const cp_t cpcode, const ts_t start_time, int count = 100) {
	
	uint32_t column_id = GetTimeBucket(start_time);
	/* verify the request */
	if(column_id > 35){
		std::cerr<<"start time exceed 6 hours, data unavailable."<<std::endl;
		return false;
	}
	/* prepare and open db with corresponding column families */
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.comparator = &cmp;

	std::vector<ColumnFamilyDescriptor> cf_des;
	ColumnFamilyOptions cfoptions;
	cfoptions.comparator = &cmp;

	std::vector<ColumnFamilyHandle*> handles;

	cf_des.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfoptions));
	for(uint32_t i = 0; i <= column_id; ++i) {
		cf_des.push_back(ColumnFamilyDescriptor(std::to_string(i), cfoptions));
	}

	DB* db;
	Status s = db->OpenForReadOnly(options, kDBdir+std::to_string(cpcode), cf_des, &handles, &db);
	if(!s.ok()) {
		std::cerr<<s.ToString()<<std::endl;
		std::cerr<<"open db fail"<<std::endl;
		return false;
	}

	/* generate a key for iterator */
	char key[KEYSIZE];
	Key_Operator ko;

	std::string str_null;

	ko.GenerateKey(cpcode,start_time,str_null,key);
	Slice key_iter(key,KEYSIZE);

	/* create iterators to iterate the db */
	std::vector<Iterator*> iterators;
	s = db->NewIterators(ReadOptions(),handles, &iterators);
	if(!s.ok()) {
		std::cerr<<s.ToString()<<std::endl;
		std::cerr<<"create iterators fail"<<std::endl;
		return false;
	}

	/* iterate through the db to get data */
	int count_got = 0;

	if(count_got < count) {
		for(auto it = iterators.rbegin(); it!= iterators.rend(); ++it) {
			if(count_got >= count) break;
			for((*it)->SeekToFirst(); (*it)->Valid() && count_got < count; (*it)->Next()) {
				/* find valid record */
				if(cmp.Compare((*it)->key(),key_iter) >= 0) {
					cp_t c_p;
					ts_t t_s;
					char id[ID_SIZE+1];
					ko.ParseKey((*it)->key(),c_p,t_s,id);
					std::cerr<<"No. "<<count_got<<" timestamp :"<<std::to_string(t_s)<<" id:"<<id<<std::endl;
					result.push_back((*it)->value().ToString());
					count_got++;
				}
			}
		}
	}

	/* clean up memory */
	for(auto it : iterators){
		delete it;
	}

	for(auto handle : handles){
		delete handle;
	}

	delete db;

	return true;
}

bool read_db_until(std::vector<std::string> &result, const cp_t cpcode, const ts_t start_time, ts_t end_time = -1) {

	/* by default the end time is now */
	if(end_time < 0) {	 
		std::time_t cur_time = std::time(0);
		end_time = (ts_t)cur_time;
	}

	uint32_t column_id_s = GetTimeBucket(start_time);
	uint32_t column_id_e = GetTimeBucket(end_time);
	/* verify the request */
	if(column_id_s > 35){
		std::cerr<<"start time exceed 6 hours, data unavailable."<<std::endl;
		return false;
	}
	/* prepare and open db with corresponding column families */
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.comparator = &cmp;

	std::vector<ColumnFamilyDescriptor> cf_des;
	ColumnFamilyOptions cfoptions;
	cfoptions.comparator = &cmp;

	std::vector<ColumnFamilyHandle*> handles;

	cf_des.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfoptions));
	for(uint32_t i = column_id_e; i <= column_id_s; ++i) {
		cf_des.push_back(ColumnFamilyDescriptor(std::to_string(i), cfoptions));
	}

	DB* db;
	Status s = db->OpenForReadOnly(options, kDBdir+std::to_string(cpcode), cf_des, &handles, &db);
	if(!s.ok()) {
		std::cerr<<s.ToString()<<std::endl;
		std::cerr<<"open db fail"<<std::endl;
		return false;
	}

	/* generate keys for iterator */
	char key_s[KEYSIZE];
	char key_e[KEYSIZE];
	Key_Operator ko;

	std::string str_null;

	ko.GenerateKey(cpcode, start_time, str_null, key_s);
	ko.GenerateKey(cpcode, end_time, str_null, key_e);
	Slice key_iter_s(key_s, KEYSIZE);
	Slice key_iter_e(key_e, KEYSIZE);

	/* create iterators to iterate the db */
	std::vector<Iterator*> iterators;
	s = db->NewIterators(ReadOptions(), handles, &iterators);
	if(!s.ok()) {
		std::cerr<<s.ToString()<<std::endl;
		std::cerr<<"create iterators fail"<<std::endl;
		return false;
	}


	/* iterate through db to get data */
	bool terminate = false;
	for(auto it = iterators.rbegin(); it!= iterators.rend(); ++it) {
		if(terminate) break;
		for((*it)->SeekToFirst(); (*it)->Valid(); (*it)->Next()) {
			/* if exceed end_time then jump out */
			if(cmp.Compare((*it)->key(),key_iter_e)>= 0) {
				terminate = true;
				break;
			}
			/* find valid record */
			if(cmp.Compare((*it)->key(),key_iter_s) >= 0) {
				cp_t c_p;
				ts_t t_s;
				char id[ID_SIZE+1];
				ko.ParseKey((*it)->key(),c_p,t_s,id);
				std::cerr<<" timestamp :"<<std::to_string(t_s)<<" id:"<<id<<std::endl;
				result.push_back((*it)->value().ToString());
			}
		}
	}

	/* clean up memory */
	for(auto it : iterators){
		delete it;
	}

	for(auto handle : handles){
		delete handle;
	}

	delete db;

	return true;

}

int main(int argc, char* argv[]){
	std::string cpcode, sec_back;
	int mode = 0;
	std::cout<<"input mode (0 for read_db_count, 1 for read_db_until): ";
	std::cin>>mode;
	std::cout<<"input cpcode: ";
	std::cin>>cpcode;
	std::cout<<"input sec_back: ";
	std::cin>>sec_back; 
	std::time_t cur_time = std::time(0);
	std::cout<<"curtime: "<<std::to_string((ts_t)cur_time)<<std::endl;
	ts_t start_time = (ts_t)cur_time - std::stoi(sec_back);
	std::cout<<"start_time: "<<std::to_string(start_time)<<std::endl;
	std::vector<std::string> result;
	if(mode){
		if(read_db_until(result, std::stoi(cpcode), start_time)){
			for(auto s: result){
				std::cout<<"got waf: "<<s<<std::endl;
			}
		}
	}
	else {
		if(read_db_count(result, std::stoi(cpcode), start_time, 10)){
			for(auto s: result){
				std::cout<<"got waf: "<<s<<std::endl;
			}
		}
	}

	return 0;

}