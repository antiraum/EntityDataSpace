#!/usr/bin/env ruby -w

# This class implements a data type for a set of attribute name/value pairs as
# used by the +DataSpace+ for attribute mappings.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:Attributes

class Attributes
  
  attr_accessor :pairs
  
  # A set of attributes consists of one or multiple attribute name/value
  # pairs.
  #
  # === Parameters
  # <em>*pairs</em>:: The attribute name/value pairs. Each pair must be an array with two items. The first item is the name, the second the value. [+Arrays+]
  #
  # === Throws
  # _ArgumentError_:: If one of the <em>*pairs</em> is no array or doesn't contain exactly two items
  #
  def initialize(*pairs)
  
    pairs.each { |pair|
      unless pair.instance_of?(Array) && pair.length == 2
        raise ArgumentError, "pairs must be name/value pair arrays"
      end
    }
  
    @pairs = pairs
  end

  # Creates an +Attributes+ object from a string in the following format:
  #
  # * (name:value, name:value, ...)
  #
  # Parsing capabilities are limited. Currently the names and values cannot 
  # contain "(", ")", ":", nor ",".
  #
  # === Parameters
  # _str_:: string to parse [+String+]
  #
  # === Returns
  # +Attributes+ object
  #
  def Attributes.from_s(str)
    
    pairs = str.split(/\s*,\s*/).map { |pair|
      pair.split(/\s*:\s*/)
    }
    Attributes.new(*pairs)
  end
  
  # Checks if the pairs of another +Attributes+ object are all also in this
  # set of attributes.
  #
  # === Parameters
  # _other_:: [+Attributes+]
  #
  # === Returns
  # +true+ if the other pairs are contained, or +false+ if not
  #
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