#!/usr/bin/env ruby -w
require "data_space"

data_space = DataSpace.new

begin
  data_space.insert_entity(id)
rescue DataSpace::NoEntityError => e
  
end
