#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <ctime>
#include <sstream>
#include <exception>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
//#include "rocksdb/table.h"
//#include "rocksdb/cache.h"
#include "rocksdb/DLR_key_comparator.h"
#include "rocksdb/key_op.h"
//#include "rocksdb/statistics.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using namespace rocksdb;

#define ID_SIZE 26
#define KEYSIZE (sizeof(uint32_t)+sizeof(double)+ID_SIZE+1)
// 6 hours time interval
#define TIME_INTERVAL 21600

typedef uint32_t cp_t;
typedef double ts_t;

static std::string kDBdir="/scratch/database/";
static std::unordered_map<cp_t, DB*> opening_db;
static std::unordered_map<cp_t, std::vector<ColumnFamilyHandle*>> db_cfhandlers;
static DLRKeyComparator cmp;
static Options options;

uint32_t GetTimeBucket(ts_t timestamp){
	std::srand(time(NULL));	 
	std::time_t cur_time = std::time(0);
	uint32_t gap = cur_time - (uint32_t)timestamp;
	if(gap >= TIME_INTERVAL){
		return (rand()%36)+1;
	}
	uint32_t bucket_num = (gap/600)+1;
	return bucket_num;
}


int main(int argc, char* argv[]){
	std::cout << "Please input KSD log file path: ";
	std::string filepath = "/home/rocksdb/scratch/dlrs.txt";
	//std::cin >> filepath;

	std::ifstream file(filepath);
	if(!file.is_open()){
		std::cerr<<"Error: Open KSD log file failed."<<std::endl;
		exit(1);
	}

	std::string read_line;

	//instantiate a key operator
	Key_Operator ko;

	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	// options.env->SetBackgroundThreads(2,Env::Priority::LOW);
	// options.write_buffer_size = 10*1024*1024;
	// options.max_write_buffer_number = 5;
	// options.min_write_buffer_number_to_merge = 2;
	// options.level0_file_num_compaction_trigger = 5;
	// options.max_bytes_for_level_base = 100*1024*1024;
	// options.target_file_size_base = 10*1024*1024;
	options.create_if_missing = true;
	options.comparator = &cmp;
	//options.statistics = CreateDBStatistics();

	int count = 0;

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
	        std::string waf = read_line.substr(pos);
	        waf.pop_back();
		}
		catch (std::exception const& e)
    	{
        	std::cerr << e.what() << std::endl;
    	}

		//find the column family handler for this log
		uint32_t column_id = GetTimeBucket(timestamp);

		//this log is outdated, skip this one
		if(column_id > 36){
			continue;
		}
		count++;
		if(count%100==0){
			cpcode+=count/100;
		}
		/*get a DB pointer for the cpcode*/

		//create and open a DB for a new cpcode
		if(opening_db.find(cpcode) == opening_db.end()){
			//create a new DB
			   DB* db;
			  Status s = DB::Open(options, kDBdir+std::to_string(cpcode), &db);
			  if(!s.ok()) std::cout<<s.ToString()<<std::endl;
			  assert(s.ok());

			  // create column family
			  std::vector<ColumnFamilyHandle*> cfs(36,nullptr);
			  ColumnFamilyOptions cfoptions;
			  cfoptions.comparator = &cmp;

			  /* this loop takes 15 secs*/
			  for(int i=1;i<37;i++){
			    s = db->CreateColumnFamily(cfoptions, std::to_string(i), &cfs[i-1]);
			    //assert(s.ok());
			  }

			   // close DB
			   delete db;
			// open DB with 37 column families
			  std::vector<ColumnFamilyDescriptor> column_families;
			  // have to open default column family
			  column_families.push_back(ColumnFamilyDescriptor(
			      kDefaultColumnFamilyName, cfoptions));
			  // open the new one, too
			  for(int i=1;i<37;i++){
			  	column_families.push_back(ColumnFamilyDescriptor(
			      std::to_string(i), cfoptions));
			  }
			  std::vector<ColumnFamilyHandle*> handles;
			  DBOptions db_options(options);
			  s = DB::Open(db_options, kDBdir+std::to_string(cpcode), column_families, &handles, &db);
			  if(!s.ok()) std::cout<<s.ToString()<<std::endl;
			  //assert(s.ok());
			  db_cfhandlers[cpcode] = handles;
			  opening_db[cpcode]= db;
		} //end of db initialization

		char key[KEYSIZE];
		ko.GenerateKey(cpcode, timestamp, msgid, key);
		Slice sk(key,KEYSIZE);
		Slice sv(waf);
		Status s = opening_db[cpcode]->Put(WriteOptions(),db_cfhandlers[cpcode][column_id], sk, sv);
		if(!s.ok()){
			std::cerr<<"Error: write log to DB failed."<<" cpcode: "<<cpcode<<" timestamp: "<<timestamp<<" msgid: "<<msgid<<" Status: "<<s.ToString()<<std::endl;
			//TODO:write to log
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
