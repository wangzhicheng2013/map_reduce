/*************************************************************************
    > File Name: MapReduce.h
    > Author: wangzhicheng
    > Mail: 2363702560@qq.com 
    > Created Time: Sun 08 Oct 2017 05:52:45 PM AWST
 ************************************************************************/

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H
namespace map_reduce {
class MapReduce
{
public:
	/*
	 * @purpose:get data from input file and produce result into output file
	 * @input_path:input file full path
	 * @output_path:output file full path
	 * */
	virtual bool map_reduce(const char *input_path, const char *output_path) = 0;
};
}
#endif
