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
#include "rocksdb/utilities/db_ttl.h"
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

typedef uint32_t cp_t;
typedef double ts_t;

static std::string kDBdir="/scratch/database/";
static std::unordered_map<cp_t, DBWithTTL*> opening_db;
static DLRKeyComparator cmp;
static Options options;


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
		std::time_t cur_time = std::time(0);
		
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

    	/* Outdate data */
    	if(timestamp < cur_time - TIME_INTERVAL) {
    		continue;
    	}

    	if(opening_db.find(cpcode) == opening_db.end()){
	    	DBWithTTL* db;
			Status s = DBWithTTL::Open(options, kDBdir+std::to_string(cpcode),&db, TIME_INTERVAL);
			if(!s.ok()){
				std::cout<<s.ToString()<<std::endl;
			 	return 2;
			} 
			opening_db[cpcode]= db;
		}
		char key[KEYSIZE];
		ko.GenerateKey(cpcode, timestamp, msgid, key);
		Slice sk(key,KEYSIZE);
		Slice sv = waf;
		Status s = opening_db[cpcode]->Put(WriteOptions(), sk, sv);
		if(!s.ok()){
			std::cerr<<"Error: write log to DB failed."<<" cpcode: "<<cpcode<<" timestamp: "<<timestamp<<" msgid: "<<msgid<<" Status: "<<s.ToString()<<std::endl;
			return 4;
		}
		//clean the key buffer
		bzero(key, KEYSIZE);
	 } //end of while loop

	file.close();

	return 0;
}

void close_all_db() {
	/* close all the opened dbs */ 
	for(auto it=opening_db.begin();it!=opening_db.end();it++){
		delete it->second;
	}
	opening_db.clear();
}

void close_db(cp_t cpcode){
	if(opening_db.find(cpcode)==opening_db.end()){
		return;
	}
	delete opening_db[cpcode];
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
		DBWithTTL* db;
		Status s = DBWithTTL::Open(options, kDBdir+name, &db, TIME_INTERVAL);
		if(!s.ok()){
			std::cout<<s.ToString()<<std::endl;
			return;
		} 
		cp_t cp = std::stoi(name);
		opening_db[cp]= db;
	}
	closedir(dirp);
}

int main(int argc, char* argv[]){
	std::srand(time(0));	
	//std::cout << "Please input KSD log file path: ";
	std::string filepath = "/home/rocksdb/dlrs_large.txt";
	//std::cin >> filepath;

	int status = data_ingest(filepath);
	close_all_db();

 	return 0;

}