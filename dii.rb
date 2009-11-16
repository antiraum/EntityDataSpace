#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "data_space"))
require "data_space"

data_space = DataSpace.new File.join(File.dirname(__FILE__), "dii.bdb");

begin
  data_space.insert_entity(id)
rescue DataSpace::NoEntityError => e
  
end


data_space.close