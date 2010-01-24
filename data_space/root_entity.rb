#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.dirname(__FILE__)
require "entity"

# This class implements the root of the query tree used for searching the
# +DataSpace+. It is also used for the root of an entity representation.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:RootEntity

class RootEntity < Entity
  
  # An root entity consists of an entity id and an array of child entities.
  # The entity id can be a wildcard "*" or a variable (string beginning with
  # "$").
  #
  # === Parameters
  # _id_:: Identifier of the root entity (can be * or $... in a query) [+String+]
  # _children_:: Child +Entity+ objects [+Array+]
  #
  def initialize(id, children = [])
    super nil, id, children
  end
  
end