/*************************************************************************
    > File Name: TemperatureMaxMinMapReduce.h
    > Author: wangzhicheng
    > Mail: 2363702560@qq.com 
    > Created Time: Sun 08 Oct 2017 05:52:45 PM AWST
 ************************************************************************/

#ifndef TEMPERATURE_MAX_MIN_MAP_REDUCE_H
#define TEMPERATURE_MAX_MIN_MAP_REDUCE_H
#include "MapReduce.h"
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <algorithm>
namespace temperature_max_min_map_reduce {
using namespace map_reduce;
using namespace std;
/*
 * @purpose:describle year and its temperature which draw from very line in the input file
 * */
typedef struct MetaInfo
{
		int year;
		int temperature;
}MetaInfo;
typedef vector<int> TempSet;	// temperature set
class TemperatureMaxMinMapReduce:public MapReduce
{
public:
	/*
	 * @purpose:get data from input file and produce result into output file
	 * @input_path:input file full path
	 * @output_path:output file full path
	 * */
	virtual bool map_reduce(const char *input_path, const char *output_path);
	/*
	 * @purpose:read very line and transform it to MetaInfo struct
	 * */
	bool mapper(const char *input_path, vector<MetaInfo>&vec);
	/*
	 * @purpose:reduce every record of vec and produce result into output file
	 * @mm:reducer result key -- year value -- temperature set
	 * */
	void reducer(const vector<MetaInfo>&vec, map<int, TempSet>&mm);
private:
	/*
	 * @purpose:convert line into metainfo object
	 * */
	inline void generate_metainfo(const string &line, MetaInfo &metainfo)
	{
		string tmp;
		tmp.assign(line, 0, 4);
		metainfo.year = atoi(tmp.c_str());
		tmp.assign(line, 6, 3);
		metainfo.temperature = atoi(tmp.c_str());
	}
	/*
	 * @purpose:generate result set which is held in a map
	 * */
	inline void generate_resultset(const MetaInfo &metainfo, map<int, TempSet>&mm)
	{
		int year = metainfo.year;
		int temperature = metainfo.temperature;
		auto it = mm.find(year);
		if(end(mm) == it)
		{
			TempSet tempset;
			tempset.emplace_back(temperature);
			mm[year] = tempset;
		}
		else
		{
			mm[year].emplace_back(temperature);
		}
	}
	/*
	 * @purpose:produce output file
	 * */
	bool generate_outputfile(const map<int, TempSet>&mm, const char *output_path);
};
}
#endif
