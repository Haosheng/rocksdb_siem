#include <iostream>
#include <string>
#include <vector>
#include <ctime>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/DLR_key_comparator.h"
#include "rocksdb/key_op.h"

using namespace rocksdb;

#define ID_SIZE 26
#define KEYSIZE (sizeof(uint32_t)+sizeof(double)+ID_SIZE+1)
// 6 hours time interval
#define TIME_INTERVAL 21600
#define BUCKET_SIZE 600
#define BUCKET_NUM TIME_INTERVAL / BUCKET_SIZE

typedef uint32_t cp_t;
typedef double ts_t;

static std::string kDBdir="/scratch/database/";
static DLRKeyComparator cmp;
static Options options;

uint32_t GetTimeBucket(ts_t timestamp) { 
	std::time_t cur_time = std::time(0);
	uint32_t gap = cur_time - (uint32_t)timestamp;
	if(gap >= TIME_INTERVAL){
		return BUCKET_NUM + 1;
	}
	uint32_t bucket_num = gap / BUCKET_SIZE;
	return bucket_num;
}

int get_count(cp_t cpcode, ts_t start_time, ts_t end_time) {
	/* by default the start time is 6 hours ago */
	if(start_time < 0) {	 
		std::time_t cur_time = std::time(0);
		start_time = (ts_t)cur_time - TIME_INTERVAL + 1;
	}

	/* by default the end time is now */
	if(end_time < 0) {	 
		std::time_t cur_time = std::time(0);
		end_time = (ts_t)cur_time;
	}

	uint32_t column_id_s = GetTimeBucket(start_time);
	uint32_t column_id_e = GetTimeBucket(end_time);
	/* verify the request */
	if(column_id_s > BUCKET_NUM-1 || column_id_e > column_id_s || column_id_e < 0 || column_id_s < 0 ){
		std::cerr<<"Invalid timestamp."<<std::endl;
		return -2;
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
		return -1;
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
		return -5;
	}

	int count = 0;
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
				count++;
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

	return count;
}

uint32_t read_db(std::vector<std::string> &result,  cp_t cpcode, ts_t start_time = -1, ts_t end_time = -1, int32_t count = 0) {
	/* by default the count 0 is 100 records */
	if(count == 0) {
		count = 100;
	}

	/* by default the start time is 6 hours ago */
	if(start_time < 0) {	 
		std::time_t cur_time = std::time(0);
		start_time = (ts_t)cur_time - TIME_INTERVAL + 1;
	}

	/* by default the end time is now */
	if(end_time < 0) {	 
		std::time_t cur_time = std::time(0);
		end_time = (ts_t)cur_time;
	}

	uint32_t column_id_s = GetTimeBucket(start_time);
	uint32_t column_id_e = GetTimeBucket(end_time);
	/* verify the request */
	if(column_id_s > BUCKET_NUM-1 || column_id_e > column_id_s || column_id_e < 0 || column_id_s < 0 ){
		std::cerr<<"Invalid timestamp."<<std::endl;
		return 2;
	}

	if(count < -1) {
		std::cerr<<"Invalid count."<<std::endl;
		return 3;
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
		return 1;
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
		return 5;
	}

	/* iterate through the db to get data */
	int count_got = 0;
	bool terminate = false;
	/* count is specified */
	if(count_got < count) {
		for(auto it = iterators.rbegin(); it!= iterators.rend(); ++it) {
			if(count_got >= count || terminate) break;
			for((*it)->SeekToFirst(); (*it)->Valid() && count_got < count; (*it)->Next()) {
				/* if exceed end_time then jump out */
				if(cmp.Compare((*it)->key(),key_iter_e) >= 0) {
					terminate = true;
					break;
				}
				/* find valid record */
				if(cmp.Compare((*it)->key(),key_iter_s) >= 0) {
					result.push_back((*it)->value().ToString());
					count_got++;
				}
			}
		}
	}
	else // count is -1 which means return all the data
	{
		for(auto it = iterators.rbegin(); it!= iterators.rend(); ++it) {
			if(terminate) break;
			for((*it)->SeekToFirst(); (*it)->Valid(); (*it)->Next()) {
				/* if exceed end_time then jump out */
				if(cmp.Compare((*it)->key(), key_iter_e) >= 0) {
					terminate = true;
					break;
				}
				/* find valid record */
				if(cmp.Compare((*it)->key(), key_iter_s) >= 0) {
					result.push_back((*it)->value().ToString());
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

	return 0;
}


/* main function for testing */
int main(int argc, char* argv[]){
	cp_t cpcode;
	ts_t sec_back_s, sec_back_e;
	ts_t start_time, end_time;
	int count;
	std::cout<<"input cpcode: ";
	std::cin>>cpcode;
	std::cout<<"input sec_back_s: ";
	std::cin>>sec_back_s;
	std::cout<<"input sec_back_e: ";
	std::cin>>sec_back_e;
	std::cout<<"input count: ";
	std::cin>>count;
	std::vector<std::string> result;
	
		std::time_t cur_time = std::time(0);
		std::cout<<"curtime: "<<std::to_string((ts_t)cur_time)<<std::endl;
		if(sec_back_s){
			start_time = (ts_t)cur_time - sec_back_s;		
		}
		else{
			start_time = -1;
		}
		if(sec_back_e){
			end_time = (ts_t)cur_time - sec_back_e;		
		}
		else{
			end_time = -1;
		}
		std::cout<<"start_time: "<<std::to_string(start_time)<<std::endl;
		std::cout<<"end_time: "<<std::to_string(end_time)<<std::endl;

		std::cout<<"records count: "<<get_count(cpcode, start_time, end_time)<<std::endl;

		int status = read_db(result, cpcode, start_time, end_time, count);
		std::cout<<"status: "<<status<<std::endl;
		for(auto s: result){
			std::cout<<s<<std::endl;
		}
	return 0;

}
