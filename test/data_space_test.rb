#!/usr/bin/env ruby -w

require "test/unit"
require "fileutils"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "data_space"
require "root_entity"
require "entity"
require "test_vars"
 
class DataSpaceTest < Test::Unit::TestCase
  
  BDB_PATH = File.join(File.dirname(__FILE__), "test.bdb")
  
  # Runs a test block in all index modes of the data space.
  #
  def run_test_block
    [[false, false], [true, false], [true, true]].each { |args|
      FileUtils::rm_r BDB_PATH if File.exists? BDB_PATH
      @ds = DataSpace.new BDB_PATH, args.shift, args.shift
      yield
      @ds.close
      FileUtils::rm_r BDB_PATH
    }
  end

  def test_insert_entity
    
    run_test_block {
    
      # test ok
      @ds.insert_entity TestVars::ID1
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY)
    
      # test entity exists
      assert_raise(DataSpace::EntityExistsError) {
        @ds.insert_entity TestVars::ID1
      }
    
      # test invalid id
      assert_raise(ArgumentError) { @ds.insert_entity TestVars::INVAL }
    }
  end
  
  def test_delete_entity
    
    run_test_block {
    
      # test ok
      @ds.insert_entity TestVars::ID1
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY)
      @ds.delete_entity TestVars::ID1
      assert_equal [], @ds.search(TestVars::ENTITY)
    
      # test no entity
      assert_raise(DataSpace::NoEntityError) {
        @ds.delete_entity TestVars::ID1
      }
    
      # test with attributes
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.delete_entity TestVars::ID1
      assert_equal [], @ds.search(TestVars::ENTITY)
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute(TestVars::ID1, TestVars::KEY1, TestVars::STR1)
      }
    
      # test with referencing attribute
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      @ds.delete_entity TestVars::ID2
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute(TestVars::ID1, TestVars::KEY1, TestVars::ID2)
      }
      @ds.delete_entity TestVars::ID1

      # test invalid id
      assert_raise(ArgumentError) { @ds.delete_entity TestVars::INVAL }
    }
  end
 
  def test_insert_attribute
    
    run_test_block {
    
      # test ok
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_STR_ATTRIB)
      @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_entity TestVars::ID2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_ID_ATTRIB)
      @ds.delete_entity TestVars::ID1            
      @ds.delete_entity TestVars::ID2
    
      # test no entity
      assert_raise(DataSpace::NoEntityError) {
        @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      }
    
      # test same key different values
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR2
      assert_equal [ TestVars::ID1 ],
        @ds.search(RootEntity.new(TestVars::ID1, [
          Entity.new(TestVars::KEY1, TestVars::STR1),
          Entity.new(TestVars::KEY1, TestVars::STR2)
        ]))
      @ds.delete_entity TestVars::ID1

      # test same key same values
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      assert_raise(DataSpace::AttributeExistsError) {
        @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      }
      @ds.delete_entity TestVars::ID1
  
      # test no entity (attribute value)
      @ds.insert_entity TestVars::ID1
      assert_raise(DataSpace::NoEntityError) {
        @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      }
      @ds.delete_entity TestVars::ID1
    }
  end
  
  def test_delete_attribute
    
    run_test_block {
      
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      
      # test ok
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_STR_ATTRIB)
      @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      assert_equal [], @ds.search(TestVars::ENTITY_STR_ATTRIB)
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_ID_ATTRIB)
      @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      assert_equal [], @ds.search(TestVars::ENTITY_ID_ATTRIB)
      
      # test no attribute
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      }
      
      # test all attributes
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      @ds.delete_attribute TestVars::ID1, Entity::ANY_VALUE, Entity::ANY_VALUE
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      }
      
      # test all attributes with same key
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
      @ds.delete_attribute TestVars::ID1, TestVars::KEY2, Entity::ANY_VALUE
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
      }
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_STR_ATTRIB)
    }
  end
  
  def test_search
    
    run_test_block {
    
      # test no results
      assert_equal [], @ds.search(TestVars::ENTITY_STR_ATTRIB)
    
      # test all entities
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      assert_equal [ TestVars::ID1, TestVars::ID2 ].sort,
                   @ds.search(TestVars::ENTITY_ANY).sort
    
      # test entity exists but attribute not
      assert_equal [], @ds.search(TestVars::ENTITY_STR_ATTRIB)
    
      # test entity exits but attribute has wrong value
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      assert_equal [],
        @ds.search(RootEntity.new TestVars::ID1,
                   [ Entity.new(TestVars::KEY1, TestVars::STR2) ])
    
      # test attribute key wildcard
      assert_equal [ TestVars::ID1 ],
        @ds.search(RootEntity.new TestVars::ID1,
                   [ Entity.new(Entity::ANY_VALUE, TestVars::STR1) ])
    
      # test attribute value wildcard
      assert_equal [ TestVars::ID1 ],
        @ds.search(RootEntity.new TestVars::ID1,
                   [ Entity.new(TestVars::KEY1, Entity::ANY_VALUE) ])
                 
      # test two attribute childs
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::ID2
      e = RootEntity.new TestVars::ID1,
                         [ Entity.new(TestVars::KEY1, TestVars::STR1),
                           Entity.new(TestVars::KEY2, TestVars::ID2) ]
      assert_equal [ TestVars::ID1 ], @ds.search(e)
    
      # test cascaded childs
      @ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::STR2
      e.children[1].children << Entity.new(TestVars::KEY1, TestVars::STR2)
      assert_equal [ TestVars::ID1 ], @ds.search(e)
    
      # test selfloop 
      @ds.insert_attribute TestVars::ID1, TestVars::KEY3, TestVars::ID1
      e.children << Entity.new(TestVars::KEY3, TestVars::ID1)
      assert_equal [ TestVars::ID1 ], @ds.search(e)
    
      # test looping references between entities
      @ds.insert_attribute TestVars::ID2, TestVars::KEY2, TestVars::ID1
      e.children[1].children << Entity.new(TestVars::KEY2, TestVars::ID1)
      assert_equal [ TestVars::ID1 ], @ds.search(e)
    
      # test wildcard in second of three attribute levels
      assert_equal [ TestVars::ID1 ], 
        @ds.search(RootEntity.new(Entity::ANY_VALUE, [
          Entity.new(TestVars::KEY2, Entity::ANY_VALUE, [
            Entity.new TestVars::KEY2, TestVars::ID1
          ])
        ]))
    
      # test multiple results
      assert_equal [ TestVars::ID1, TestVars::ID2 ].sort,
                   @ds.search(TestVars::ENTITY_ANY).sort
    }
  end

  def test_search_with_vars

    run_test_block {
      
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      @ds.insert_entity TestVars::ID3
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID1
      @ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::ID1
      @ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::ID2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::ID3
      @ds.insert_attribute TestVars::ID3, TestVars::KEY2, TestVars::ID1
      
      assert_equal [ TestVars::ID1, TestVars::ID2 ],
        @ds.search(RootEntity.new(TestVars::VAR1, [
          Entity.new(TestVars::KEY1, TestVars::VAR1)
        ]))
        
      assert_equal [ TestVars::ID2 ],
        @ds.search(RootEntity.new(TestVars::VAR1, [
          Entity.new(TestVars::KEY1, TestVars::VAR1, [
            Entity.new(TestVars::KEY1, TestVars::VAR2, [
              Entity.new(TestVars::KEY1, TestVars::VAR2)
            ])
          ])
        ]))
      
      assert_equal [ TestVars::ID2 ],
        @ds.search(RootEntity.new(TestVars::VAR1, [
          Entity.new(TestVars::KEY1, TestVars::VAR1),
          Entity.new(TestVars::KEY1, TestVars::VAR2, [
            Entity.new(TestVars::KEY1, TestVars::VAR2),
            Entity.new(TestVars::KEY2, TestVars::VAR3, [
              Entity.new(TestVars::KEY2, TestVars::VAR2, [
                Entity.new(TestVars::KEY1, TestVars::VAR2)
              ])
            ])
          ])
        ]))
        
      # TODO check single variable
      # TODO check variable for string value
      # TODO test mixed with wildcard
    }
  end
  
  def test_get_entity
    
    run_test_block {
      
      # test ok
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      @ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID2, TestVars::KEY2, TestVars::ID1
      e = RootEntity.new TestVars::ID1,
                         [ Entity.new(TestVars::KEY1, TestVars::STR1),
                           Entity.new(TestVars::KEY1, TestVars::STR2),
                           Entity.new(TestVars::KEY1, TestVars::ID1),
                           Entity.new(TestVars::KEY1, TestVars::ID2,
                           [
                             Entity.new(TestVars::KEY1, TestVars::STR1),
                             Entity.new(TestVars::KEY2, TestVars::ID1)
                           ]) ]      
      assert_equal e, @ds.get_entity(TestVars::ID1)
      @ds.delete_entity TestVars::ID1
      @ds.delete_entity TestVars::ID2
      
      # test invalid id
      assert_raise(DataSpace::NoEntityError) {
        @ds.get_entity TestVars::ID2
      }
    }
  end
  
  def test_exhaustive
    
    run_test_block {
    
      num_nodes = 50
    
      # fully connected graph
      (0..num_nodes).each { |n|
        @ds.insert_entity TestVars::ID1 + n.to_s
      }
      (0..num_nodes).each { |n1|
        (0..num_nodes).each { |n2|
          @ds.insert_attribute TestVars::ID1 + n1.to_s, TestVars::KEY1, TestVars::ID1 + n2.to_s
        }
      }
      
      # e = RootEntity.new TestVars::ID1 + "*"
      # [1..99].each { |n|
      #   e.children << Entity.new TestVars::ID1 + "*"
      #   
      # }
      
      (0..num_nodes).each { |n|
        @ds.delete_entity TestVars::ID1 + n.to_s
      }
    }
  end
end