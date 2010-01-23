#!/usr/bin/env ruby -w

require "test/unit"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "attributes"
require "test_vars"
 
class AttributesTest < Test::Unit::TestCase

  @@pairs = [[TestVars::KEY1, TestVars::ID1], [TestVars::KEY1, TestVars::ID2],
            [TestVars::KEY2, TestVars::STR1]]

  def build_attributes(pairs = @@pairs)
    Attributes.new(*pairs)
  end
  
  def test_attributes
    
    attrib = build_attributes
    assert_equal @@pairs, attrib.pairs
    
    assert_raise(ArgumentError) {
      Attributes.new({TestVars::KEY1 => TestVars::ID1},
                     TestVars::KEY1, TestVars::ID2)
    }
    assert_raise(ArgumentError) {
      Attributes.new [TestVars::KEY1, TestVars::ID1],
                     [TestVars::ID1, TestVars::KEY1, TestVars::ID2]
    }
  end
  
  def test_from_s
    pairs = [["key1", "id1"], ["key1", "id2"], ["key 2", '"str 1"']]
    str = 'key1:id1, key1 : id2,key 2:"str 1"'
    assert_equal build_attributes(pairs), Attributes.from_s(str)
  end
  
  def test_contains?
    a1 = build_attributes
    a2 = build_attributes [[TestVars::KEY1, TestVars::ID1]]
    assert_equal true, a1.contains?(a2)
    a3 = build_attributes [[TestVars::KEY1, TestVars::ID1],
                           [TestVars::KEY2, TestVars::STR1]]
    assert_equal true, a1.contains?(a3)
  end
end