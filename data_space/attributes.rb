#!/usr/bin/env ruby -w

# This class ...
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:Attributes

class Attributes
  
  attr_accessor :pairs
  
  def initialize(*pairs)
  
    pairs.each { |pair|
      unless pair.instance_of?(Array) && pair.length == 2
        raise ArgumentError, "pairs must be name/value pair arrays"
      end
    }
  
    @pairs = pairs
  end
  
  def Attributes.from_s(str)
    
    pairs = str.split(/\s*,\s*/).map { |pair|
      pair.split(/\s*:\s*/)
    }
    Attributes.new(*pairs)
  end
  
  def contains?(other)
    @pairs & other.pairs == other.pairs
  end

  def to_s
    s = ""
    pairs.each { |pair|
      s += "#{pair[0]} => #{pair[1]}, "
    }
    s.chomp(", ")
  end

  def ==(other)
    return false unless other
    @pairs.sort == other.pairs.sort
  end
end