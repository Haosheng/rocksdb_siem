#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <ctime>
#include <sstream>
#include <exception>
#include <dirent.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/DLR_key_comparator.h"
#include "rocksdb/key_op.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using namespace rocksdb;

#define ID_SIZE 26
#define KEYSIZE (sizeof(uint32_t)+sizeof(double)+ID_SIZE+1)
/* 6 hours time interval */
#define TIME_INTERVAL 21600
#define BUCKET_SIZE 600
#define BUCKET_NUM TIME_INTERVAL / BUCKET_SIZE

typedef uint32_t cp_t;
typedef double ts_t;

static std::string kDBdir="/scratch/database/";
static std::unordered_map<cp_t, DB*> opening_db;
static std::unordered_map<cp_t, std::vector<ColumnFamilyHandle*>> db_cfhandlers;
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


ts_t GenerateValidTS() { 
	std::time_t cur_time = std::time(0);
	return (ts_t)cur_time - rand()%21600;
}

uint32_t data_ingest(std::string filepath){
	std::ifstream file(filepath);
	if(!file.is_open()){
		std::cerr<<"Error: Open KSD log file failed."<<std::endl;
		return 1;
	}
	std::string read_line;

	//instantiate a key operator
	Key_Operator ko;

	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.create_if_missing = true;
	options.comparator = &cmp;

	while(getline(file,read_line)){
		//get key sub fields
		cp_t cpcode;
		ts_t timestamp;
		std::string msgid;
		std::string waf;

		try{
			std::stringstream ss;
		    ss << read_line;

		    boost::property_tree::ptree pt;
		    boost::property_tree::read_json(ss, pt);
		    ss.flush();

			cpcode = pt.get<cp_t>("cp");

	        timestamp = pt.get<ts_t>("start");

	        msgid = pt.get<std::string>("id");

	        size_t pos = read_line.find("\"waf\":{");
	        waf = read_line.substr(pos);
	        waf.pop_back();
		}
		catch (std::exception const& e)
    	{
        	std::cerr << e.what() << std::endl;
        	return 5;
    	}

    	timestamp = GenerateValidTS();
		//find the column family handler for this log
		uint32_t column_id = GetTimeBucket(timestamp);

		//this log is outdated, skip this one
		if(column_id > BUCKET_NUM-1){
			continue;
		}
		/*get a DB pointer for the cpcode*/

		//create and open a DB for a new cpcode
		if(opening_db.find(cpcode) == opening_db.end()){
			//create a new DB
			  DB* db;
			  Status s = DB::Open(options, kDBdir+std::to_string(cpcode), &db);
			  if(!s.ok()){
			  	std::cout<<s.ToString()<<std::endl;
			  	return 2;
			  } 

			  // create column family
			  std::vector<ColumnFamilyHandle*> cfs(BUCKET_NUM,nullptr);
			  ColumnFamilyOptions cfoptions;
			  cfoptions.comparator = &cmp;

			  for(int i=0;i<BUCKET_NUM;i++){
			    s = db->CreateColumnFamily(cfoptions, std::to_string(i), &cfs[i]);
			    if(!s.ok()){
			    	std::cout<<s.ToString()<<std::endl;
			    	return 3;
			    } 
			  }
			  db_cfhandlers[cpcode] = cfs;
			  opening_db[cpcode]= db;
		} //end of db initialization

		char key[KEYSIZE];
		ko.GenerateKey(cpcode, timestamp, msgid, key);
		Slice sk(key,KEYSIZE);
		Slice sv = waf;
		Status s = opening_db[cpcode]->Put(WriteOptions(),db_cfhandlers[cpcode][column_id], sk, sv);
		if(!s.ok()){
			std::cerr<<"Error: write log to DB failed."<<" cpcode: "<<cpcode<<" timestamp: "<<timestamp<<" msgid: "<<msgid<<" Status: "<<s.ToString()<<std::endl;
			return 4;
		}
		//clean the key buffer
		bzero(key, KEYSIZE);
	 } //end of while loop

	file.close();

	/* close all the opened dbs and their column handlers*/ 
	for(auto it=opening_db.begin();it!=opening_db.end();it++){
		for(auto iter=db_cfhandlers[it->first].begin();iter!=db_cfhandlers[it->first].end();iter++){
			delete *iter;
		}
		delete it->second;
	}

	return 0;
}

void close_all_db() {
	/* close all the opened dbs and their column handlers*/ 
	for(auto it=opening_db.begin();it!=opening_db.end();it++){
		for(auto iter=db_cfhandlers[it->first].begin();iter!=db_cfhandlers[it->first].end();iter++){
			delete *iter;
		}
		delete it->second;
	}
	db_cfhandlers.clear();
	opening_db.clear();
}

void close_db(cp_t cpcode){
	if(opening_db.find(cpcode)==opening_db.end()){
		return;
	}
	for(auto it = db_cfhandlers[cpcode].begin(); it!=db_cfhandlers[cpcode].end();it++) {
		delete *it;
	}
	delete opening_db[cpcode];
	db_cfhandlers.erase(cpcode);
	opening_db.erase(cpcode);
}

void reopen_all_db() {
	close_all_db();
	DIR* dirp;
	dirp = opendir(kDBdir.c_str());
	if (dirp == NULL){
		std::cerr<<"Open database directory failed."<<std::endl;
		return;
	}

	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.create_if_missing = true;
	options.comparator = &cmp;

	dirent* dp;
	while ((dp = readdir(dirp)) != NULL) {
			std::string name(dp->d_name);
			DB* db;
			  Status s = DB::Open(options, kDBdir+name, &db);
			  if(!s.ok()){
			  	std::cout<<s.ToString()<<std::endl;
			  	return;
			  } 

			  // create column family
			  std::vector<ColumnFamilyHandle*> cfs(BUCKET_NUM,nullptr);
			  ColumnFamilyOptions cfoptions;
			  cfoptions.comparator = &cmp;

			  for(int i=0;i<BUCKET_NUM;i++){
			    s = db->CreateColumnFamily(cfoptions, std::to_string(i), &cfs[i]);
			    if(!s.ok()){
			    	std::cout<<s.ToString()<<std::endl;
			    	return;
			    } 
			  }
			  cp_t cp = std::stoi(name);
			  db_cfhandlers[cp] = cfs;
			  opening_db[cp]= db;
	}
	closedir(dirp);
}

int main(int argc, char* argv[]){
	std::srand(time(0));	
	//std::cout << "Please input KSD log file path: ";
	std::string filepath = "/home/rocksdb/scratch/dlrs.txt";
	//std::cin >> filepath;

	int status = data_ingest(filepath);


 	return 0;

}