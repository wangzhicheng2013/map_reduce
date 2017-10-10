/*************************************************************************
    > File Name: test_unittest.cpp
    > Author: wangzhicheng
    > Mail: 2363702560@qq.com 
    > Created Time: Sun 08 Oct 2017 07:42:01 PM AWST
 ************************************************************************/

#include "TemperatureMaxMinMapReduce.h"
#include "gtest/gtest.h"
using namespace temperature_max_min_map_reduce;
TEST(TEMPERATURE, MAPPER)
{
	TemperatureMaxMinMapReduce mr;
	vector<MetaInfo>vec;
	mr.mapper("./input.data", vec);
	EXPECT_EQ(vec[0].year, 1970);
	EXPECT_EQ(vec[0].temperature, +10);
	EXPECT_EQ(vec[5].year, 1975);
	EXPECT_EQ(vec[5].temperature, 8);
	EXPECT_TRUE(mr.map_reduce("./input.data", "./output.data"));
}
TEST(TEMPERATURE, OUTFILE_SUCC)
{
	TemperatureMaxMinMapReduce mr;
	vector<MetaInfo>vec;
	mr.mapper("./input.data", vec);
	EXPECT_TRUE(mr.map_reduce("./input.data", "./output.data"));
}


