#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "data_space")
require "data_space"
require "root_entity"
require "entity"

# This scripts shows how to use the data space implementation.
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)

# 1. Create the DataSpace object
#
ds = DataSpace.new File.join(File.dirname(__FILE__), "dii.bdb"),
                   :use_all_indexes => true

# 2. Insert some entities
#
%w{ LAURA PETER SUSAN TRENTO MILAN ITALY VW_GOLF FERRARI PIZZA }.each { |id|
  begin
    ds.insert_entity id
  rescue DataSpace::EntityExistsError => e
  end
}

# 3. Insert some attributes
#
[ %w{LAURA hasFullName "Laura\ Smith"}, %w{PETER hasFirstName "Peter"},
  %w{PETER hasSurname "Bond"}, %w{SUSAN hasFirstName "Susan"},
  %w{SUSAN hasSurname "Bond"}, %w{LAURA livesIn TRENTO},
  %w{LAURA worksIn MILAN}, %w{PETER hasCar VW_GOLF}, %w{LAURA hasCar FERRARI},
  %w{PETER livesIn TRENTO}, %w{PETER likes PIZZA}, %w{LAURA likes PIZZA},
  %w{PIZZA comesFrom ITALY}, %w{FERRARI comesFrom ITALY},
  %w{TRENTO isIn ITALY}, %w{TRENTO hasItalianName "Trento"},
  %w{TRENTO hasGermanName "Trient"}, %w{MILAN isIn ITALY},
  %w{MILAN hasItalianName "Milano"}, %w{MILAN hasGermanName "Mailand"},
  %w{PETER hasDaughter SUSAN}, %w{SUSAN hasFather PETER},
  %w{PETER isCookingFor PETER}, %w{PETER isCookingFor SUSAN} ].each { |attrib|
  begin
    ds.insert_attribute attrib.shift, attrib.shift, attrib.shift
  rescue DataSpace::NoEntityError => e
    puts e
  rescue DataSpace::AttributeExistsError => e
  end
}

# 4. Perform some queries
#
puts "\n"
puts "All entities that are related to italy:"
puts "---------------------------------------"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("*", "ITALY")
  ])
).map { |id| ds.get_entity id }

puts "\n"
puts "All entities that live in a city in italy:"
puts "------------------------------------------"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("livesIn", "*", [
      Entity.new("isIn", "ITALY")
    ])
  ])
).map { |id| ds.get_entity id }

puts "\n"
puts "All entities that have a german name:"
puts "-------------------------------------"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("hasGermanName", "*")
  ])
).map { |id| ds.get_entity id }

puts "\n"
puts "All about SUSAN:"
puts "----------------"
puts ds.search(RootEntity.new("SUSAN")).map { |id| ds.get_entity id }

puts "\n"
puts "All entities that have a daughter who has a father who likes Pizza,"
puts "has a VW Golf and lives in an entity with an attribute \"Trient\":"
puts "-------------------------------------------------------------------"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("hasDaughter", "*", [
      Entity.new("hasFather", "*", [
        Entity.new("likes", "PIZZA"),
        Entity.new("hasCar", "VW_GOLF"),
        Entity.new("livesIn", "*", [
          Entity.new("*", '"Trient"')
        ])
      ])
    ])
  ])
).map { |id| ds.get_entity id }

# 5. Queries can also contain variables
#
puts "\n"
puts "All entities that have the same surname and are related:"
puts "--------------------------------------------------------"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("hasSurname", "$x"),
    Entity.new("*", "*", [
      Entity.new("hasSurname", "$x")
    ])
  ])
).map { |id| ds.get_entity id }

puts "\n"
puts "All entities that are related to each other:"
puts "--------------------------------------------"
puts ds.search(
  RootEntity.new("$x", [
    Entity.new("*", "*", [
      Entity.new("*", "$x")
    ])
  ])
).map { |id| ds.get_entity id }

puts "\n"
puts "All entities that are related to oneself and to another entity with the"
puts "same attribute:"
puts "-----------------------------------------------------------------------"
puts ds.search(
  RootEntity.new("$x", [
    Entity.new("$y", "$x"),
    Entity.new("$y", "*")
  ])
).map { |id| ds.get_entity id }

# 6. The Entity class has a basic parsing support for query strings
#
puts "\n"
puts "All entities that live and work in a city in the same country:"
puts "--------------------------------------------------------------"
puts ds.search(
  Entity.from_s("*(livesIn:*(isIn:$1), worksIn:*(isIn:$1))")
).map { |id| ds.get_entity id }

# 7. Close the data space
#
# ds.clear
ds.close