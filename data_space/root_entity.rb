#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.dirname(__FILE__)
require "entity"

# This class implements the root of the query tree used for searching the
# FlatDataSpace.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:RootEntity

class RootEntity < Entity
  
  # The +new+ class method initializes the class.
  #
  # === Parameters
  # * _id_:: identifier of the root entity (can be * in a query)
  # * _children_:: array of child Entity objects
  #
  def initialize(id, children=[])
    super nil, id, children
  end
  
  
end