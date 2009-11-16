#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "data_space"))
require "data_space"

data_space = DataSpace.new

begin
  data_space.insert_entity(id)
rescue DataSpace::NoEntityError => e
  
end
