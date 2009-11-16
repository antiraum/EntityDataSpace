#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "ds")
require "ds"

# This scripts shows how to use the DataSpace.

# 1. Create DataSpace object
#
ds = DataSpace.new File.join(File.dirname(__FILE__), "dii.bdb");

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
  %w{PETER hasDaughter SUSAN}, %w{SUSAN hasFather PETER} ].each { |attrib|
  begin
    ds.insert_attribute attrib.shift, attrib.shift, attrib.shift
  rescue DataSpace::NoEntityError => e
    puts e
  rescue DataSpace::AttributeExistsError => e
  end
}

# 4. Perform some queries
#
puts "\nAll entities that are related to italy:"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("*", "ITALY")
  ])
)

puts "\nAll entities that live in a city in italy:"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("livesIn", "*", [
      Entity.new("isIn", "ITALY")
    ])
  ])
)

puts "\nAll entities that have a german name:"
puts ds.search(
  RootEntity.new("*", [
    Entity.new("hasGermanName", "*")
  ])
)

puts "\nAll about SUSAN"
puts ds.search RootEntity.new("SUSAN")

puts "\nAll entities that have a daughter who has a father who likes Pizza, has a VW_GOLF and lives in an entity with an attribute \"Trient\":"
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
)

# 5. Can 

# 6. Close the DataSpace
# ds.clear
ds.close