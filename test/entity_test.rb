#!/usr/bin/env ruby -w

require "test/unit"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
$LOAD_PATH.unshift File.dirname(__FILE__)
require "entity"
require "test_vars"
 
class EntityTest < Test::Unit::TestCase
  
  def build_entity
    Entity.new TestVars::KEY1, TestVars::ID1,
               [ Entity.new(TestVars::KEY1, TestVars::ID2),
                 Entity.new(TestVars::KEY2, TestVars::STR1) ]
  end
  
  def test_entity
    e = build_entity
    assert_equal TestVars::KEY1, e.key
    assert_equal TestVars::ID1, e.value
    assert_equal TestVars::KEY1, e.children[0].key
    assert_equal TestVars::ID2, e.children[0].value
    assert_equal TestVars::KEY2, e.children[1].key
    assert_equal TestVars::STR1, e.children[1].value
  end
  
  def test_from_s
    e = Entity.new("key1", "id1", [
          Entity.new("key1", "id2", [
            Entity.new("key 3", "id 3")
          ]),
          Entity.new("key2", '"str1"'),
          Entity.new("key2", "id2", [
            Entity.new("key2", '"str2"'),
            Entity.new("key1", "id1" , [
              Entity.new("key 3", '" str 3 "')
            ])
          ])
        ])
    str = 'key1:id1 (key1 : id2 (key 3 : id 3) , key2: "str1", ' + 
          'key2 : id2(key2:"str2",key1 :id1(key 3:" str 3 ")))'
    assert_equal e, Entity.from_s(str)
  end
  
  def test_to_s
    e = build_entity
    expected = <<STR
#{e.key}:#{e.value} (
  #{e.children[0].key}:#{e.children[0].value}
  #{e.children[1].key}:#{e.children[1].value}
)
STR
    assert_equal expected, e.to_s
  end

  def test_to_s_jb
    e = Entity.new nil, "JB", [
          Entity.new(
            "lives", "TRC",
            [ %w{name "Trento"}, %w{locatedIn TR}, %w{country IT} ].map { |e|
              Entity.new(e.shift, e.shift)
            }
          ),
          Entity.new("married", "CC")
        ]
    expected = <<STR
JB (
  lives:TRC (
    name:"Trento"
    locatedIn:TR
    country:IT
  )
  married:CC
)
STR
    # puts "\n\nExpected:\n" + expected + "\nActual:\n" + e.to_s + "\n"
    assert_equal expected, e.to_s
  end
  
  def test_equal
    assert_equal build_entity, build_entity
  end
  
  def test_compare
    assert_equal("a" <=> "b", Entity.new(nil, "a") <=> Entity.new(nil, "b"))
    assert_equal(-1, Entity.new(nil, "b") <=> Entity.new("a", "a"))
    assert_equal(1, Entity.new("a", "b") <=> Entity.new(nil, "a"))
    assert_equal("a" <=> "b", Entity.new("a", "b") <=> Entity.new("b", "a"))
  end
end