#!/usr/bin/env ruby -w

require "benchmark"
require "fileutils"
require "pp"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "data_space"
require "root_entity"
require "entity"
require "test_vars"

# This scripts shows how to use the data space implementation.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)

MODES = {
  "1. Store Only" => [false, false],
  "2. Inverted Indexes" => [true, false],
  "3. Additional Indexes" => [true, true]
}.sort

BDB_PATH = File.join(File.dirname(__FILE__), "benchmark.bdb")

NUM_NODES = 20
NUM_STR_ATTRIBS = 20

MODES.each { |mode_name, mode_args|
  
  # print mode header
  puts mode_name
  puts "=" * mode_name.length
  
  # create data space
  FileUtils::rm_r BDB_PATH if File.exists? BDB_PATH
  ds = DataSpace.new BDB_PATH, mode_args.shift, mode_args.shift
  
  # fill with dummy data
  # insert nodes
  (1..NUM_NODES).each { |n|
    ds.insert_entity TestVars::ID1 + n.to_s
  }
  # fully connect the nodes
  (1..NUM_NODES).each { |n1|
    (1..NUM_NODES).each { |n2|
      ds.insert_attribute TestVars::ID1 + n1.to_s, TestVars::KEY1,
                           TestVars::ID1 + n2.to_s
    }
  }
  # add string attributes
  (1..NUM_NODES).each { |n|
    (1..NUM_STR_ATTRIBS).each { |s1|
      (1..NUM_STR_ATTRIBS).each { |s2|
        ds.insert_attribute TestVars::ID1 + n.to_s, TestVars::KEY1 + s1.to_s,
                            TestVars::STR1 + s2.to_s + '"'
      }
    }
  }
  
  # test query
  query = []
  (1..NUM_STR_ATTRIBS).each { |s1|
    (1..NUM_STR_ATTRIBS).each { |s2|
      query << Entity.new(TestVars::KEY1 + s1.to_s,
                          TestVars::STR1 + s2.to_s + '"')
    }
  }
  query = Entity.new(Entity::ANY_VALUE, TestVars::VAR1, query)
  NUM_NODES.times {
    query = Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [ query ])
  }
  query = RootEntity.new(TestVars::VAR1, [ query ])
  
  # benchmark
  Benchmark.bmbm { |x|
    # x.report("add entity") { ds.insert_entity TestVars::ID2 }
    # x.report("add str attrib") {
    #   ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::STR1
    # }
    # x.report("add id attrib") {
    #   ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::ID1 + "1"
    # }
    x.report("search") {
      ds.search(query)
    }
    # x.report("search") {
    #   puts ds.search(RootEntity.new(Entity::ANY_VALUE, [
    #     Entity.new(TestVars::KEY1, TestVars::VAR1, [
    #       Entity.new(TestVars::KEY1, TestVars::VAR1),
    #       Entity.new(TestVars::KEY2, TestVars::VAR2, [
    #         Entity.new(Entity::ANY_VALUE, TestVars::VAR3),
    #         Entity.new(TestVars::KEY3, Entity::ANY_VALUE)
    #       ])
    #     ]),
    #     Entity.new(TestVars::KEY2, TestVars::VAR2, [
    #       Entity.new(TestVars::KEY1, TestVars::VAR1),
    #       Entity.new(TestVars::KEY2, TestVars::VAR2)
    #     ])
    #   ])).length
    # }
  }
  
  # close data space
  ds.close
  FileUtils::rm_r BDB_PATH
  
  puts
}