#!/usr/bin/env ruby -w

# This class implements an data type for an entity used to query the
# DataSpace and to represent the search results.
# This is _not_ the representation used internally in the DataSpace.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:Entity

class Entity
  
  ANY_VALUE = "*"
  
  attr_accessor :key, :value, :children
  
  # An entity consists of a key, a value, and an array of child entities.
  # The value can be be a string (recognized by quotes, i.e., "Trento" instead
  # of Trento) or an entity identifier. Entity identifiers (ids) are strings
  # (without quotes).
  #
  # === Parameters
  # * _key_:: attribute name (can be nil when root; can be * in a query)
  # * _value_:: attribute value (can be * in a query)
  # * _children_:: array of child Entity objects
  #
  def initialize(key, value, children=[])
    
    if key == "*" && value == "*"
      raise ArgumentError, "Only either the key or the value can be *."
    end
    
    @key = key
    @value = value
    @children = children
  end
  
  # def Entity.from_s(str)
  #   key, valu
  #   new key, value, childs
  # end
  # 
  # def split_s(str)
  #   str =~ /^[^"']*["']()
  #   "12:50am" =~ /(\d\d):(\d\d)(..)/  » 0
  #   "Hour is #$1, minute #$2"
  #   a = s.split(",", 2)
  #   key = a.shift
  #   a = a.shift.split()
  # end
  
  def to_s(indent=0)
    s = " " * indent + (@key ? @key.to_s + ":" : "") + @value.to_s
    if !@children.empty?
      s << " (\n" +
           @children.map { |c| c.to_s(indent + 2) }.join("") +
           " " * indent + ")"
    end
    s << "\n"
  end
  
  def ==(other)
    return false if !other
    # ((!@key && !other.key) || (@key && other.key && @key == other.key)) &&
    @key == other.key &&
    @value == other.value && @children.sort == other.children.sort
  end
  
  def <=>(other)
    return @value <=> other.value if !@key && !other.key
    return -1 if !@key
    return 1 if !other.key
    @key <=> other.key 
  end
end