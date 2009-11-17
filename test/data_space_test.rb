#!/usr/bin/env ruby -w

require "test/unit"
require "tempfile"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "data_space"
require "root_entity"
require "entity"
require "test_vars"
 
class DataSpaceTest < Test::Unit::TestCase
  
  def setup
    @ds = DataSpace.new File.join(File.dirname(__FILE__), "test.bdb")
    @ds.clear
  end

  def teardown
    @ds.clear
    @ds.close
  end

  def test_insert_entity
    
    # test ok
    @ds.insert_entity TestVars::ID1
    assert_equal [ RootEntity.new(TestVars::ID1) ],
                 @ds.search(TestVars::ENTITY)
    
    # test entity exists
    assert_raise(DataSpace::EntityExistsError) {
      @ds.insert_entity TestVars::ID1
    }
    
    # test invalid id
    assert_raise(ArgumentError) { @ds.insert_entity TestVars::INVAL }
  end
  
  def test_delete_entity
    
    # test ok
    @ds.insert_entity TestVars::ID1
    assert_equal [ RootEntity.new(TestVars::ID1) ],
                 @ds.search(TestVars::ENTITY)
    @ds.delete_entity TestVars::ID1
    assert_equal [], @ds.search(TestVars::ENTITY)
    
    # test no entity
    assert_raise(DataSpace::NoEntityError) { @ds.delete_entity TestVars::ID1 }
    
    # test with attributes
    @ds.insert_entity TestVars::ID1
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    @ds.delete_entity TestVars::ID1
    assert_equal [], @ds.search(TestVars::ENTITY)
    assert_raise(DataSpace::NoAttributeError) {
      @ds.delete_attribute(TestVars::ID1, TestVars::KEY1)
    }
    
    # test with referencing attribute
    @ds.insert_entity TestVars::ID1
    @ds.insert_entity TestVars::ID2
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
    @ds.delete_entity TestVars::ID2
    assert_raise(DataSpace::NoAttributeError) {
      @ds.delete_attribute(TestVars::ID1, TestVars::KEY1)
    }
    @ds.delete_entity TestVars::ID1

    # test invalid id
    assert_raise(ArgumentError) { @ds.delete_entity TestVars::INVAL }
  end
 
  def test_insert_attribute
    
    # test ok
    @ds.insert_entity TestVars::ID1
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    assert_equal [ TestVars::ENTITY_STR_ATTRIB ],
                 @ds.search(TestVars::ENTITY_STR_ATTRIB)
    @ds.delete_attribute TestVars::ID1, TestVars::KEY1
    @ds.insert_entity TestVars::ID2
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
    assert_equal [ TestVars::ENTITY_ID_ATTRIB ],
                 @ds.search(TestVars::ENTITY_ID_ATTRIB)
    @ds.delete_entity TestVars::ID1            
    @ds.delete_entity TestVars::ID2
    
    # test no entity
    assert_raise(DataSpace::NoEntityError) {
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    }
    
    # test attribute exists
    @ds.insert_entity TestVars::ID1
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    assert_raise(DataSpace::AttributeExistsError) {
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    }
    @ds.delete_entity TestVars::ID1
  
    # test no entity (attribute value)
    @ds.insert_entity TestVars::ID1
    assert_raise(DataSpace::NoEntityError) {
      @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::ID2
    }
    @ds.delete_entity TestVars::ID1
  end
  
  def test_search
    
    # test no results
    assert_equal [], @ds.search(TestVars::ENTITY_STR_ATTRIB)
    
    # test all entities
    @ds.insert_entity TestVars::ID1
    @ds.insert_entity TestVars::ID2
    assert_equal(
      [ TestVars::ID1, TestVars::ID2 ].map { |id| RootEntity.new id }.sort,
      @ds.search(RootEntity.new Entity::ANY_VALUE).sort
    )
    
    # test entity exists but attribute not
    assert_equal([], @ds.search(TestVars::ENTITY_STR_ATTRIB))
    
    # test entity exits but attribute has wrong value
    @ds.insert_attribute TestVars::ID1, TestVars::KEY1, TestVars::STR_VALUE1
    assert_equal([],
      @ds.search(RootEntity.new TestVars::ID1,
                 [ Entity.new(TestVars::KEY1, TestVars::STR_VALUE2) ]))
    
    # test attribute key wildcard
    assert_equal([ TestVars::ENTITY_STR_ATTRIB ],
      @ds.search(RootEntity.new TestVars::ID1,
                 [ Entity.new(Entity::ANY_VALUE, TestVars::STR_VALUE1) ]))
    
    # test attribute value wildcard
    assert_equal([ TestVars::ENTITY_STR_ATTRIB ],
      @ds.search(RootEntity.new TestVars::ID1,
                 [ Entity.new(TestVars::KEY1, Entity::ANY_VALUE) ]))
                 
    # test two attribute childs
    @ds.insert_attribute TestVars::ID1, TestVars::KEY2, TestVars::ID2
    e = RootEntity.new TestVars::ID1,
                       [ Entity.new(TestVars::KEY1, TestVars::STR_VALUE1),
                         Entity.new(TestVars::KEY2, TestVars::ID2) ]
    assert_equal([ e ], @ds.search(e))
    
    # test cascaded childs
    @ds.insert_attribute TestVars::ID2, TestVars::KEY1, TestVars::STR_VALUE2
    e.children[1].children.push Entity.new(TestVars::KEY1,
                                           TestVars::STR_VALUE2)
    assert_equal([ e ], @ds.search(e))
    
    # test selfloop 
    @ds.insert_attribute TestVars::ID1, TestVars::KEY3, TestVars::ID1
    e.children.push Entity.new(TestVars::KEY3, TestVars::ID1)
    assert_equal([ e ], @ds.search(e))
    
    # test looping references between entities
    @ds.insert_attribute TestVars::ID2, TestVars::KEY2, TestVars::ID1
    e.children[1].children.push Entity.new(TestVars::KEY2, TestVars::ID1)
    assert_equal([ e ], @ds.search(e))
    
    # test wildcard in second of three attribute levels
    assert_equal([ e ], 
      @ds.search(RootEntity.new(Entity::ANY_VALUE, [
        Entity.new(TestVars::KEY2, Entity::ANY_VALUE, [
          Entity.new TestVars::KEY2, TestVars::ID1
        ])
      ]))
    )
    
    # test multiple results
    assert_equal([ e, @ds.search(RootEntity.new(TestVars::ID2)).shift ].sort,
                 @ds.search(RootEntity.new(Entity::ANY_VALUE)).sort)
  end
  
end