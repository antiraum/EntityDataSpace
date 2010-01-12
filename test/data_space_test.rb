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
    [
      {},
      {:use_indexes => true},
      {:use_all_indexes => true}
    ].each { |options|
      FileUtils::rm_r BDB_PATH if File.exists? BDB_PATH
      @ds = DataSpace.new BDB_PATH, options
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
      assert_raise(DataSpace::NoEntityError) {
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
  
  def test_clear
    
    run_test_block {
      @ds.insert_entity TestVars::ID1
      @ds.insert_entity TestVars::ID2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
      assert_equal [ TestVars::ID1, TestVars::ID2 ].sort,
                   @ds.search(TestVars::ENTITY_ANY).sort
      @ds.clear
      assert_equal [], @ds.search(TestVars::ENTITY_ANY)
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
        @ds.delete_attribute TestVars::ID1, Entity::ANY_VALUE,
                             Entity::ANY_VALUE
      }
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
        @ds.delete_attribute TestVars::ID1, TestVars::KEY2, Entity::ANY_VALUE
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
      }
      assert_equal [ TestVars::ID1 ], @ds.search(TestVars::ENTITY_STR_ATTRIB)
      @ds.delete_attribute TestVars::ID1, Entity::ANY_VALUE, Entity::ANY_VALUE
      
      # test all attributes with same value
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      assert_equal [ TestVars::ID1 ],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY1, TestVars::STR1),
                     Entity.new(TestVars::KEY2, TestVars::STR1)
                   ]))
      @ds.delete_attribute TestVars::ID1, Entity::ANY_VALUE, TestVars::STR1
      assert_raise(DataSpace::NoAttributeError) {
        @ds.delete_attribute TestVars::ID1, Entity::ANY_VALUE, TestVars::STR1
      }
      assert_equal [],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY1, TestVars::STR1)
                   ]))
      assert_equal [],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY2, TestVars::STR1)
                   ]))
      
      # test one attribute of several with same key
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
      assert_equal [ TestVars::ID1 ],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY2, TestVars::STR1),
                     Entity.new(TestVars::KEY2, TestVars::STR2)
                   ]))
      @ds.delete_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      assert_equal [],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY2, TestVars::STR1),
                     Entity.new(TestVars::KEY2, TestVars::STR2)
                   ]))
      assert_equal [ TestVars::ID1 ],
                   @ds.search(RootEntity.new(TestVars::ID1, [
                     Entity.new(TestVars::KEY2, TestVars::STR2)
                   ]))
                   
      # TODO test mapping removal
    }
  end

  def test_insert_attribute_mapping
    
    run_test_block {
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
    
      # test ok
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2 }
      )
      assert_equal [ TestVars::ID1 ],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
                     
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY2 => TestVars::STR2 },
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY3 => TestVars::STR2 }
      )
      
      assert_equal [ TestVars::ID1 ],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY1, TestVars::STR1),
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
    
      # test argument error
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          nil
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          nil,
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          [ TestVars::KEY1, TestVars::STR2 ]
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          [ TestVars::KEY1, TestVars::STR2 ],
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY2 => TestVars::STR2 }
        )
      }
      assert_raise(ArgumentError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY2 => TestVars::STR2 },
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
    
      # test no entity error
      assert_raise(DataSpace::NoEntityError) {
        @ds.insert_attribute_mapping(
          TestVars::ID2,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY1 => TestVars::STR2 }
        )
      }

      # test no attribute error
      assert_raise(DataSpace::NoAttributeError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY3 => TestVars::STR1 },
          { TestVars::KEY1 => TestVars::STR2 }
        )
      }
      assert_raise(DataSpace::NoAttributeError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY2 => TestVars::ID1 },
          { TestVars::KEY1 => TestVars::STR2,
            TestVars::KEY2 => TestVars::STR2 }
        )
      }
    
      # test mapping exists error
      assert_raise(DataSpace::MappingExistsError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY3 => TestVars::STR2 }
        )
      }
      assert_raise(DataSpace::MappingExistsError) {
        @ds.insert_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY2 => TestVars::STR2 },
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY3 => TestVars::STR2 }
        )
      }
    }
  end
  
  def test_delete_attribute_mapping
    
    run_test_block {
      @ds.insert_entity TestVars::ID1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR2
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR1
      @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::STR2
    
      # test ok
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2 }
      )  
      assert_equal [ TestVars::ID1 ],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY2 => TestVars::STR2 },
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY3 => TestVars::STR2 }
      )
      assert_equal [ TestVars::ID1 ],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY1, TestVars::STR1),
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      @ds.delete_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2 }
      )
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      @ds.delete_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY2 => TestVars::STR2 },
        { TestVars::KEY1 => TestVars::STR1,
          TestVars::KEY3 => TestVars::STR2 }
      )
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY1, TestVars::STR1),
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      
      # test ok with wildcard
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2 }
      )
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2,
          TestVars::KEY3 => TestVars::STR1 }
      )
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR2 },
        { TestVars::KEY3 => TestVars::STR1 }
      )
      @ds.delete_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        "*"
      )
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2),
                       Entity.new(TestVars::KEY2, TestVars::STR1)
                     ]), :use_mappings => true)
      assert_equal [ TestVars::ID1 ],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR1)
                     ]), :use_mappings => true)
      @ds.insert_attribute_mapping(
        TestVars::ID1,
        { TestVars::KEY1 => TestVars::STR1 },
        { TestVars::KEY3 => TestVars::STR2 }
      )
      @ds.delete_attribute_mapping(TestVars::ID1, "*", "*")
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR2)
                     ]), :use_mappings => true)
      assert_equal [],
                   @ds.search(
                     RootEntity.new(TestVars::ID1, [
                       Entity.new(TestVars::KEY3, TestVars::STR1)
                     ]), :use_mappings => true)
    
      # test argument error
      assert_raise(ArgumentError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          nil
        )
      }
      assert_raise(ArgumentError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          nil,
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
      assert_raise(ArgumentError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          [ TestVars::KEY1, TestVars::STR2 ]
        )
      }
      assert_raise(ArgumentError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          [ TestVars::KEY1, TestVars::STR2 ],
          { TestVars::KEY1 => TestVars::STR1 }
        )
      }
      
      # test no entity error
      assert_raise(DataSpace::NoEntityError) {
        @ds.delete_attribute_mapping(
          TestVars::ID2,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY3 => TestVars::STR2 }
        )
      }
    
      # test no mapping error
      assert_raise(DataSpace::NoMappingError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          { TestVars::KEY3 => TestVars::STR2 }
        )
      }
      assert_raise(DataSpace::NoMappingError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY2 => TestVars::STR2 },
          { TestVars::KEY1 => TestVars::STR1,
            TestVars::KEY3 => TestVars::STR2 }
        )
      }
      assert_raise(DataSpace::NoMappingError) {
        @ds.delete_attribute_mapping(
          TestVars::ID1,
          { TestVars::KEY1 => TestVars::STR1 },
          "*"
        )
      }
      assert_raise(DataSpace::NoMappingError) {
        @ds.delete_attribute_mapping(TestVars::ID1, "*", "*")
      }
    }
  end
  
  def test_search
    
    run_test_block {
      
      # test invalid query
      assert_raise(ArgumentError) {
        @ds.search("x")
      }
    
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
                   @ds.search(RootEntity.new TestVars::ID1, [
                     Entity.new(TestVars::KEY1, TestVars::STR2)
                   ])
    
      # test attribute key wildcard
      assert_equal [ TestVars::ID1 ],
                   @ds.search(RootEntity.new TestVars::ID1, [
                     Entity.new(Entity::ANY_VALUE, TestVars::STR1)
                   ])
    
      # test attribute value wildcard
      assert_equal [ TestVars::ID1 ],
                   @ds.search(RootEntity.new TestVars::ID1, [
                     Entity.new(TestVars::KEY1, Entity::ANY_VALUE)
                   ])
                 
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
      
      # check simple
      assert_equal [ TestVars::ID1, TestVars::ID2 ].sort,
                   @ds.search(RootEntity.new(TestVars::VAR1, [
                     Entity.new(TestVars::KEY1, TestVars::VAR1)
                   ])).sort
      
      # check more complex
      assert_equal [ TestVars::ID2 ],
                   @ds.search(RootEntity.new(TestVars::VAR1, [
                     Entity.new(TestVars::KEY1, TestVars::VAR1, [
                       Entity.new(TestVars::KEY1, TestVars::VAR2, [
                         Entity.new(TestVars::KEY1, TestVars::VAR2)
                       ])
                     ])
                   ]))
      
      # check more complex
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
        
      # check single variable (should be treated like wildcard)
      assert_equal [ TestVars::ID1, TestVars::ID2, TestVars::ID3 ].sort,
                   @ds.search(RootEntity.new(TestVars::VAR1)).sort
      assert_equal [ TestVars::ID2 ],
                   @ds.search(RootEntity.new(TestVars::VAR1, [
                     Entity.new(TestVars::KEY1, TestVars::VAR2)
                   ]))
      
      # check mixed with wildcards
      assert_equal [ TestVars::ID1, TestVars::ID2 ].sort,
                   @ds.search(RootEntity.new(Entity::ANY_VALUE, [
                     Entity.new(TestVars::KEY1, TestVars::VAR1, [
                       Entity.new(TestVars::KEY1, TestVars::VAR1, [
                         Entity.new(TestVars::KEY2, Entity::ANY_VALUE)
                       ])
                     ])
                   ])).sort
      
      # check key variable
      assert_equal [ TestVars::ID2 ],
                   @ds.search(RootEntity.new(Entity::ANY_VALUE, [
                     Entity.new(TestVars::VAR1, TestVars::ID1),
                     Entity.new(TestVars::VAR1, TestVars::ID2)
                   ]))

      # check key and value wildcards
      assert_equal [ TestVars::ID1, TestVars::ID3 ],
                   @ds.search(RootEntity.new(TestVars::VAR1, [
                     Entity.new(Entity::ANY_VALUE, Entity::ANY_VALUE, [
                       Entity.new(TestVars::KEY2, TestVars::VAR1)
                     ])
                   ]))
                   
      # check key and value variables
      assert_equal [ TestVars::ID2 ],
                   @ds.search(RootEntity.new(Entity::ANY_VALUE, [
                     Entity.new(TestVars::VAR1, TestVars::VAR2),
                     Entity.new(TestVars::VAR1, TestVars::VAR3)
                   ]))
      
      # check triple usage of variable
      assert_equal [ TestVars::ID2 ],
                   @ds.search(RootEntity.new(TestVars::VAR1, [
                     Entity.new(TestVars::VAR2, TestVars::VAR3, [
                       Entity.new(TestVars::VAR2, TestVars::VAR3, [
                         Entity.new(TestVars::KEY2, Entity::ANY_VALUE, [
                           Entity.new(TestVars::KEY2, TestVars::VAR3, [
                             Entity.new(Entity::ANY_VALUE, TestVars::VAR3)
                           ])
                         ])
                       ])
                     ])
                   ]))
      
      # TODO test same variable for key and value
      # TODO test common variables among root children
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
      e = RootEntity.new TestVars::ID1, [
        Entity.new(TestVars::KEY1, TestVars::STR1),
        Entity.new(TestVars::KEY1, TestVars::STR2),
        Entity.new(TestVars::KEY1, TestVars::ID1),
        Entity.new(TestVars::KEY1, TestVars::ID2, [
          Entity.new(TestVars::KEY1, TestVars::STR1),
          Entity.new(TestVars::KEY2, TestVars::ID1)
        ])
      ]      
      assert_equal e, @ds.get_entity(TestVars::ID1)
      @ds.delete_entity TestVars::ID1
      @ds.delete_entity TestVars::ID2
      
      # test invalid id
      assert_raise(DataSpace::NoEntityError) {
        @ds.get_entity TestVars::ID2
      }
    }
  end
end