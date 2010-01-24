#!/usr/bin/env ruby -w

require "benchmark"
require "fileutils"
require "pp"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "data_space"
require "root_entity"
require "entity"
require "attributes"
require "bm_vars"

# This scripts benchmarks the data space implementation.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)

def get_test_var(type, num)
  case num
    when 1
      case type
        when "id"
          BmVars::ID1
        when "key"
          BmVars::KEY1
        when "str"
          BmVars::STR1
      end
    when 2
      case type
        when "id"
          BmVars::ID2
        when "key"
          BmVars::KEY2
        when "str"
          BmVars::STR2
      end
    when 3
      case type
        when "id"
          BmVars::ID3
        when "key"
          BmVars::KEY3
        when "str"
          BmVars::STR3
      end
    else
      case type
        when "id"
          BmVars::ID1 + num.to_s
        when "key"
          BmVars::KEY1 + num.to_s
        when "str"
          BmVars::STR1 + num.to_s + '"'
      end
  end
end

SIZES = [50, 100, 200, 400, 800]

MODES = {
  "1. Store Only" => {},
  "2. Inverted Indexes" => {:use_indexes => true},
  "3. All Indexes" => {:use_all_indexes => true}
}.sort

BDB_PATH = File.join(File.dirname(__FILE__), "benchmark.bdb")

NUM_STR_ATTRIBS = 3

d_query = Entity.new(BmVars::KEY1, Entity::ANY_VALUE);
7.times {
  d_query = Entity.new(BmVars::KEY1, Entity::ANY_VALUE, [ d_query ])
}
d_query = RootEntity.new(BmVars::ID1, [ d_query ])
wd_query = []
(1..NUM_STR_ATTRIBS).each { |s1|
  (1..NUM_STR_ATTRIBS).each { |s2|
    wd_query << Entity.new(get_test_var("key", s1), get_test_var("str", s2))
  }
}
wd_query = Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, wd_query)
7.times {
  wd_query = Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [ wd_query ])
}
wd_query = RootEntity.new(Entity::ANY_VALUE, [ wd_query ])
TEST_QUERIES = {
  "1. Specific entity lookups" =>
    RootEntity.new(BmVars::ID1, [
      Entity.new(BmVars::KEY1, BmVars::STR1),
      Entity.new(Entity::ANY_VALUE, BmVars::ID2, [
        Entity.new(BmVars::KEY1, BmVars::ID3, [
          Entity.new(BmVars::KEY3, BmVars::STR2)
        ]),
        Entity.new(BmVars::KEY2, Entity::ANY_VALUE)
      ]) 
    ]),
  "2. Any entity lookups" =>
    RootEntity.new(Entity::ANY_VALUE, [
      Entity.new(BmVars::KEY1, BmVars::STR1),
      Entity.new(BmVars::KEY2, Entity::ANY_VALUE, [
        Entity.new(BmVars::KEY1, Entity::ANY_VALUE, [
          Entity.new(BmVars::KEY3, BmVars::STR2)
        ]),
        Entity.new(BmVars::KEY2, Entity::ANY_VALUE)
      ]) 
    ]),
  "3. Extensive wildcard usage" =>
    RootEntity.new(Entity::ANY_VALUE, [
      Entity.new(Entity::ANY_VALUE, BmVars::STR1),
      Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [
        Entity.new(Entity::ANY_VALUE, BmVars::ID3, [
          Entity.new(Entity::ANY_VALUE, BmVars::STR2)
        ]),
        Entity.new(BmVars::KEY2, Entity::ANY_VALUE)
      ]) 
    ]),
  "4. Variable usage" =>
    RootEntity.new(BmVars::VAR1, [
      # Entity.new(BmVars::KEY1, BmVars::VAR2),
      Entity.new(BmVars::VAR3, Entity::ANY_VALUE, [
        Entity.new(BmVars::VAR3, BmVars::ID3, [
          # Entity.new(Entity::ANY_VALUE, BmVars::VAR2)
        ]),
        Entity.new(BmVars::KEY2, BmVars::VAR1)
      ]) 
    ]),
  "5. Wide query" =>
    RootEntity.new(Entity::ANY_VALUE, [
      Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [
        Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [
          Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE)
        ])
      ]) 
    ]),
  "6. Deep query" => d_query,
  "7. Wide and deep query" => wd_query
}.sort

MAPPING_QUERIES = {
  "8. Mapping query" =>
    RootEntity.new(Entity::ANY_VALUE, [
      Entity.new(BmVars::KEY2 + "m", BmVars::STR2),
      Entity.new(BmVars::KEY3 + "m", BmVars::STR3),
      Entity.new(BmVars::KEY3, BmVars::ID3, [
        Entity.new(BmVars::KEY3, BmVars::ID3, [
          Entity.new(BmVars::KEY1 + "m", BmVars::STR1)
        ]),
        Entity.new(BmVars::KEY2, Entity::ANY_VALUE)
      ]) 
    ])
}

SIZES.each { |num_nodes|
  
  # print size header
  puts "=" * 78
  puts "=== #{num_nodes} NODES ==="
  puts "=" * 78
  
  MODES.each { |mode_name, mode_args|
  
    # print mode header
    puts mode_name
    puts "-" * 78
  
    # create data space
    FileUtils::rm_r BDB_PATH if File.exists? BDB_PATH
    ds = DataSpace.new BDB_PATH, mode_args
  
    # fill with dummy data
    Benchmark.bm(28) { |x|
    
      x.report("0. Insert test data") {
        # insert nodes
        (1..num_nodes).each { |n|
          ds.insert_entity get_test_var("id", n)
        }
        # double-connect the nodes
        (1..num_nodes).each { |n1|
          (1..num_nodes).each { |n2|
            id1, id2 = get_test_var("id", n1), get_test_var("id", n2)
            ds.insert_attribute id1, BmVars::KEY1, id2
            if n2.odd?
              begin
                ds.insert_attribute_mapping(
                  Entity::ANY_VALUE,
                  Attributes.new([BmVars::KEY1, id2]),
                  Attributes.new([BmVars::KEY3, id2])
                )
              rescue DataSpace::MappingExistsError => e
              end
            elsif n1.odd?  
              ds.insert_attribute id1, BmVars::KEY2, id2
            end
          }
        }
        # add string attributes
        (1..num_nodes).each { |n|
          (1..NUM_STR_ATTRIBS).each { |s1|
            (1..NUM_STR_ATTRIBS).each { |s2|
              ds.insert_attribute get_test_var("id", n),
                                  get_test_var("key", s1),
                                  get_test_var("str", s2)
            }
          }
          next if n.odd?
          # add specific mappings
          ds.insert_attribute_mapping(
            get_test_var("id", n),
            Attributes.new([BmVars::KEY1, BmVars::STR1]),
            Attributes.new(
              [BmVars::KEY2 + "m", BmVars::STR2],
              [BmVars::KEY3 + "m", BmVars::STR3]
            )
          )
          ds.insert_attribute_mapping(
            get_test_var("id", n),
            Attributes.new(
              [BmVars::KEY1, BmVars::STR1],
              [BmVars::KEY2, BmVars::STR1],
              [BmVars::KEY3, BmVars::STR1]
            ),
            Attributes.new([BmVars::KEY1 + "m", BmVars::STR1])
          )
        }
      }
    }
    puts
  
    # # test queries for results
    # TEST_QUERIES.each { |name, query|
    #   puts name
    #   puts ds.search(query).size
    # }
    # MAPPING_QUERIES.each { |name, query|
    #   puts name
    #   puts ds.search(query, :use_mappings => true).size
    # }
  
    # excute test queries
    total_query_time = 0
    query_tms = Benchmark.bmbm(28) { |x|
    
      TEST_QUERIES.each { |name, query|
        x.report(name) {
          ds.search query
        }
      }
      
    }.each { |tms|
      total_query_time += tms.real
    }
    puts "Total: #{total_query_time}"
      
    if mode_name =~ /^3/
      puts
      
      # excute test queries with mapping enables
      total_query_time = 0
      query_tms = Benchmark.bmbm(28) { |x|
    
        TEST_QUERIES.each { |name, query|
          x.report(name) {
            ds.search query, :use_mappings => true
          }
        }
      
      }.each { |tms|
        total_query_time += tms.real
      }
      puts "Total (with mapping enabled): #{total_query_time}"
      puts
      
      # execute mapping queries
      total_query_time = 0
      query_tms = Benchmark.bmbm(28) { |x|
    
        MAPPING_QUERIES.each { |name, query|
          x.report(name) {
            ds.search query, :use_mappings => true
          }
        }
      
      }.each { |tms|
        total_query_time += tms.real
      }
      puts "Total mapping queries: #{total_query_time}"
    end
    puts
  
    # print size
    ds.print_size
  
    # close data space
    ds.close
    system "du -s \"#{BDB_PATH}\""
    system "du -sh \"#{BDB_PATH}\""
    system "ls -l \"#{BDB_PATH}\""
    system "ls -lh \"#{BDB_PATH}\""
    FileUtils::rm_r BDB_PATH
  
    puts
  }
  puts
}