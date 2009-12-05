#!/usr/bin/env ruby -w

# This class implements an data type for an entity used to query the
# +DataSpace+ and to represent the search results.
# This is _not_ the representation used internally in the +DataSpace+.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:Entity

class Entity
  
  ANY_VALUE = "*"
  
  attr_accessor :key, :value, :children
  
  # An entity consists of a key, a value, and an array of child entities.
  #
  # The value can be be a string (recognized by quotes, i.e., "Trento" instead
  # of Trento) or an entity identifier. Entity identifiers (ids) are strings
  # (without quotes).
  #
  # Key and value can be a wildcard "*" or a variable (string beginning with
  # "$").
  #
  # === Parameters
  # * _key_:: attribute name (can be nil when root; * or $... in a query)
  # * _value_:: attribute value (can be * or $... in a query)
  # * _children_:: array of child +Entity+ objects
  #
  def initialize(key, value, children = [])
    @key, @value, @children = key, value, children
  end
  
  # def Entity.from_s(str)
  #   new key, value, childs
  # end
  
  def Entity.split_s(str)
    # "(?<!\\\\)%.*$"
    key, value, childs_str = 
    case str
      when /^([^:]+):([^\(]+)(.*)$/
        [$1, $2, $3]
      when /^([^\(]+)(.*)$/
        [nil, $1, $2]
      else
        return nil
    end
    childs = []
    if childs_str =~ /^\s*\((.+)\)\s*$/
      childs = $1.split(/, ?/).map { |s| split_s(s) }
    end
    Entity.new key, value, childs
  end
  
  def to_s(indent = 0)
    s = " " * indent + (@key ? @key.to_s + ":" : "") + @value.to_s
    unless @children.empty?
      s << " (\n" +
           @children.map { |child| child.to_s(indent + 2) }.join("") +
           " " * indent + ")"
    end
    s << "\n"
  end
  
  def ==(other)
    return false unless other
    # ((!@key && !other.key) || (@key && other.key && @key == other.key)) &&
    @key == other.key &&
    @value == other.value && @children.sort == other.children.sort
  end
  
  def <=>(other)
    return @value <=> other.value unless @key || other.key
    return -1 unless @key
    return 1 unless other.key
    @key <=> other.key 
  end
end

# puts Entity.split_s("bla:blu(bl:bl, ha:ha(b:b, x:y), x:y,a:b(karl:otto))")