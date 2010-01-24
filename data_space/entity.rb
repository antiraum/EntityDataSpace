#!/usr/bin/env ruby -w

# This class implements an data type for an entity used to query the
# +DataSpace+ and to represent the search results.
# This is _not_ the representation used internally in the +DataSpace+.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:Entity

class Entity
  
  # this is the wildcard character for any attribute name or value
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
  # _key_:: Attribute name (can be nil when root; * or $... in a query) [+String+]
  # _value_:: Attribute value (can be * or $... in a query) [+String+]
  # _children_:: Child +Entity+ objects [+Array+]
  #
  def initialize(key, value, children = [])
    @key, @value, @children = key, value, children
  end
  
  # Creates an +Entity+ object tree from a string in the following format:
  #
  # * key:value(name:value, name:value(name:value, name:value))
  #
  # Parsing capabilities are limited. Currently the names and values cannot 
  # contain "(", ")", ":", nor ",".
  #
  # === Parameters
  # _str_:: string to parse [+String+]
  # _verb_:: print debug output [+Boolean+]
  #
  # === Returns
  # +Entity+ object tree
  #
  def Entity.from_s(str, verb = false)
    
    if str =~ @@COMMA_REGEX
      return nil
    end
    
    # transform brackets into unique strings
    bracket_count = 0
    brackets = {}
    str.gsub!(/[\(\)]/) { |b|
      if b =~ /\(/
        bracket_count += 1
        brackets[bracket_count] = false
        "(#{bracket_count})"
      else
        bracket_id = 0
        bracket_count.downto(1) { |id|
          next if brackets[id]
          bracket_id = id
          break
        }
        brackets[bracket_id] = 1
        "(#{bracket_id})"
      end
    }
    
    parse_s(str, verb)
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
  
  private
  
  @@COMMA = "VeRysTr4nGEsTr1Ngn0b0dYW1lLeVerW4NTt0Use4s1d0RKey"
  @@COMMA_REGEX = /#{Regexp.escape(@@COMMA)}/
  
  def Entity.parse_s(str, verb)
    if verb
      puts "-" * 60
      puts str
    end
    
    # separate key, value, and childs
    key, value, childs_str = 
    case str
      when /^
             \s*
             ([^:\(]*[^:\(\s]+)
             \s*
             :
             \s*
             ([^\(]*[^\(\s]+)
             (.*)
             \s*
           $/x
        [$1, $2, $3]
      when /^
             \s*
             ([^\(]*[^\(\s]+)
             (.*)
             \s*
           $/x
        [nil, $1, $2]
      else
        return nil
    end
    if verb
      puts "key: " + (key ? key : "nil")
      puts "value: " + value
      puts "childs: " + childs_str
    end
    
    # split childs
    childs = []
    if childs_str =~ /^
                       \s*
                       (\(\d+\))(.+)\1
                       \s*
                     $/x
      childs_str = $2
      puts "childs_str: " + childs_str if verb
      if childs_str =~ /\(\d+\)/
        # mask kommas in subchilds
        childs_str.gsub!(/(\(\d+\)).*?\1/) { |subchilds_str|
           subchilds_str.gsub(/,/, @@COMMA)
        }
        puts "masked: " + childs_str if verb
      end
      childs = childs_str.split(/, ?/).map { |child|
        parse_s(child.gsub(@@COMMA_REGEX, ","), verb)
      }
    end
    Entity.new key, value, childs
  end
end