/*************************************************************************
    > File Name: TemperatureMaxMinMapReduce.cpp
    > Author: wangzhicheng
    > Mail: 2363702560@qq.com 
    > Created Time: Sun 08 Oct 2017 05:52:45 PM AWST
 ************************************************************************/

#include "TemperatureMaxMinMapReduce.h"
namespace temperature_max_min_map_reduce {
using namespace temperature_max_min_map_reduce;
/*
 * @purpose:get data from input file and produce result into output file
 * @input_path:input file full path
 * @output_path:output file full path
 * */
bool TemperatureMaxMinMapReduce::map_reduce(const char *input_path, const char *output_path)
{
	vector<MetaInfo>vec;
	if(!mapper(input_path, vec)) return false;
	map<int, TempSet>mm;
	reducer(vec, mm);
	bool succ = generate_outputfile(mm, output_path);

	return succ;
}
/*
 * @purpose:read very line and transform it to MetaInfo struct
 * */
bool TemperatureMaxMinMapReduce::mapper(const char *input_path, vector<MetaInfo>&vec)
{
	ifstream fin(input_path);
	if(!fin) return false;
	string line;
	string tmp;
	MetaInfo metainfo;
	while(!fin.eof())
	{
		fin >> line;
		if(line.empty()) continue;
		generate_metainfo(line, metainfo);
		vec.emplace_back(metainfo);
	}
	fin.close();

	return true;
}
/*
 * @purpose:reduce every record of vec and produce result into output file
 * @mm:reducer result key -- year value -- temperature set
 * */
void TemperatureMaxMinMapReduce::reducer(const vector<MetaInfo>&vec, map<int, TempSet>&mm)
{
	for_each(begin(vec), end(vec), [&mm, this] (const MetaInfo metainfo) {
			generate_resultset(metainfo, mm);
			});
	for_each(begin(mm), end(mm), [] (pair<int, TempSet>elem) {
			TempSet &tempset = elem.second;
			sort(begin(tempset), end(tempset));
			});
}
/*
 * @purpose:produce output file
 * */
bool TemperatureMaxMinMapReduce::generate_outputfile(const map<int, TempSet>&mm, const char *output_path)
{
	ofstream fos(output_path);
	if(!fos) return false;
	const static string TAB = "\t";
	for(auto it = begin(mm);it != end(mm);++it)
	{
		int year = it->first;
		TempSet tempset = it->second;
		fos << year << TAB << *begin(tempset) << TAB << *(end(tempset) - 1) << endl;
	}
	os.close();

	return true;
}
}
