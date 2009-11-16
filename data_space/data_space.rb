#!/usr/bin/env ruby -w

require "rubygems"
# require "moneta"
# require "moneta/berkeley"
require "bdb"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "root_entity"
require "entity"

# This class implements the dataspace in a flat way such that the entities and
# all their attributes are stored as key/value pairs in the database. This
# should enable faster access and search than a serialization of the entities.
#
# A entity is saved in this way:
# * entity_id -> nil
# * entity_id_-_attrib1_key -> "attrib1_value" (string value)
# * entity_id_-_attrib2_key -> attrib2_value (entity id value)
# * ...
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:DataSpace

class DataSpace

  # separator between entity id and attribute key in the database key
  ENTITY_ATTRIB_SEP = " - "
  # this string is not allowed in entity ids and attribute keys
  # occurances of ENTITY_ATTRIB_SEP are replaced by this string
  ID_AND_KEY_INVALID = "VeRysTr4nGEsTr1Ngn0b0dYW1lLeVerW4NTt0Use4s1d0RKey"
  
  # Opens the Berkeley DB.
  #
  # === Parameters
  # * _bdb_path_:: Berkeley DB file to use
  #
  def initialize(bdb_path)
    # @db = {}
    # @db = Moneta::Berkeley.new(:file => bdb_path, :skip_expires => true)
    
    @db = Bdb::Db.new()
    @db.open(nil, bdb_path, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
  end

  # Closes the Berkley DB.
  #
  def close
    @db.close(0)
  end

  # Adds a new entity.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  #
  # === Throws
  # * _EntityExistsError_
  #
  def insert_entity(id)
    
    db_key = str_to_db_key(id)
    
    if db_key? db_key
      raise EntityExistsError.new id
    end
    
    db_put db_key, "1"
  end

  # Removes an existing entity.
  # Deletes also existing attributes that have the entity as value.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  #
  # === Throws
  # * _NoEntityError_
  #
  def delete_entity(id)
    
    db_key = str_to_db_key(id)
    
    if !db_key? db_key
      raise NoEntityError.new id
    end

    # delete entity and its attributes
    db_key_regex = /^#{Regexp.escape(db_key)}/
    db_each { |k, v| db_del(k) if k =~ db_key_regex }

    # delete attributes that have this entity as value
    db_each { |k, v| db_del(k) if v == id }
  end

  # Removes all existing entities.
  #
  def clear
    @db.truncate(nil)
  end

  # Adds a new attribute to an entity.
  # A attribute value can be a string (recognized by quotes, i.e., "Trento"
  # instead of Trento) or an entity identifier. Entity identifiers (id) are
  # strings (without quotes).
  #
  # === Parameters
  # * _id_:: identifier of the entity
  # * _key_:: name of the attribute
  # * _value_:: value of the attribute
  #
  # === Throws
  # * _NoEntityError_:: if the entity, the attribute is for or the attribute
  #                  :: referes to, does not exist
  # * _AttributeExistsError_::
  #
  def insert_attribute(id, key, value)
    
    entity_db_key = str_to_db_key(id)
    
    if !db_key? entity_db_key
      raise NoEntityError.new id
    end
    
    attrib_db_key = entity_db_key + ENTITY_ATTRIB_SEP + str_to_db_key(key)
    
    if db_key? attrib_db_key
      raise AttributeExistsError.new id, key
    end
    
    # check value entity id
    if value !~ @@ATTRIB_STR_VALUE_REGEX && !db_key?(str_to_db_key(value))
      raise NoEntityError.new value
    end
    
    db_put attrib_db_key, value
  end

  # Removes an existing attribute from an entity.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  # * _key_:: name of the attribute
  #
  # === Throws
  # * _NoAttributeError_
  #
  def delete_attribute(id, key)
    
    attrib_db_key = str_to_db_key(id) + ENTITY_ATTRIB_SEP + str_to_db_key(key)
    
    if !db_key? attrib_db_key
      raise NoAttributeError.new id, key
    end
    
    db_del attrib_db_key
  end

  # Performs a search specified by a RootEntity object. Returns an array of
  # Entity objects as result.
  #
  # === Parameters
  # * _query_:: RootEntity object
  #
  def search(query)
    
    results = []
    
    # collect entities
    if query.value == Entity::ANY_VALUE
      db_each { |k, v| 
        results.push db_key_to_str(k) if k !~ @@ENTITY_ATTRIB_SEP_REGEX
      }
    else
      results.push query.value if db_key? str_to_db_key(query.value)
    end
    
    # check entities
    results.delete_if { |e| !entity_complies?(e, query.children) }
    
    # build Entity tree
    @created_entities = {}
    results.map { |e| build_entity(nil, e) }
  end
  
  class EntityExistsError < StandardError
    
    def initialize(id)
      @id = id
      super "entity '#{@id}' already exists"
    end
    
    attr_reader :id
  end
  
  class AttributeExistsError < StandardError
    
    def initialize(id, key)
      @id = id
      @key = key
      super "attribute '#{@key}' for entity '#{@id}' already exists"
    end
    
    attr_reader :id, :key
  end
  
  class NoEntityError < StandardError
    
    def initialize(id)
      @id = id
      super "entity '#{@id}' does not exists"
    end
    
    attr_reader :id
  end
  
  class NoAttributeError < StandardError
    
    def initialize(id, key)
      @id = id
      @key = key
      super "attribute '#{@key}' for entity '#{@id}' does not exists"
    end
    
    attr_reader :id, :key
  end

  private
    
  @@ENTITY_ATTRIB_SEP_REGEX = /#{Regexp.escape(ENTITY_ATTRIB_SEP)}/
  @@ID_AND_KEY_INVALID_REGEX = /#{Regexp.escape(ID_AND_KEY_INVALID)}/
  @@ATTRIB_STR_VALUE_REGEX = /^".*"$/

  def db_key?(key)
    # @db.key? key
    nil | @db[key]
  end
  
  def db_put(key, value)
    @db[key] = value
  end
  
  def db_get(key)
    @db[key]
  end
  
  def db_del(key)
    # @db.delete key
    @db.del(nil, key, 0) if @db[key]
  end
  
  def db_each
    # @db.each { |k, v| yield k, v }
    dbc = @db.cursor(nil, 0)
    k, v = dbc.get(nil, nil, Bdb::DB_FIRST)
    while k
      yield k, v
      k, v = dbc.get(nil, nil, Bdb::DB_NEXT)
    end
    dbc.close
  end
  
  # Prepares entity id or attribute key for usage as a database key by
  # replacing occurences of ENTITY_ATTRIB_SEP
  #
  # === Parameters
  # * _str_:: entity identifier or attribute name
  #
  # === Throws
  # * _ArgumentError_:: if _str_ contains an occurance of ID_AND_KEY_INVALID
  #
  def str_to_db_key(str)
    
    if str =~ @@ID_AND_KEY_INVALID_REGEX
      raise ArgumentError,
            "entity identifiers and attribute names cannot contain the " +
            "string '#{ID_AND_KEY_INVALID}'"
    end
    
    str.gsub @@ENTITY_ATTRIB_SEP_REGEX, ID_AND_KEY_INVALID
  end
  
  # Reverts the replacements done by str_to_db_key.
  #
  # === Parameters
  # * _key_:: entity identifier or attribute name modified by str_to_db_key
  #
  def db_key_to_str(key)
    key.gsub @@ID_AND_KEY_INVALID_REGEX, ENTITY_ATTRIB_SEP
  end

  # Checks if an entity fulfills the conditions expressed by Entity objects.
  #
  # === Parameters
  # * _id_:: id of the entity
  # * _conditions_:: array of Entity objects
  #
  def entity_complies?(id, conditions)

    return true if conditions.empty?

    entity_regex = /^#{Regexp.escape(str_to_db_key(id) + ENTITY_ATTRIB_SEP)}/

    conditions.each { |c|

      if c.key == Entity::ANY_VALUE && c.value == Entity::ANY_VALUE
        # this is not valid - we silently stop traversing here
        next
      end
      
      if c.key == Entity::ANY_VALUE # any one attribute must have the value
        
        matched = false
        db_each { |k, v|
          next if matched || k !~ entity_regex || db_get(k) != c.value
          matched = true
          # break
        }
        return false if !matched
        
      else # the one attribute has to exist and must have the value
        
        attrib_db_key =
          str_to_db_key(id) + ENTITY_ATTRIB_SEP + str_to_db_key(c.key)

        if !db_key? attrib_db_key
          # attribute doesn't exist -> cannot satisfy condition
          return false
        end
        
        return false if c.value != Entity::ANY_VALUE &&
                        db_get(attrib_db_key) != c.value
      end
      
      # recurse if value is entity id and child conditions
      next if c.value =~ @@ATTRIB_STR_VALUE_REGEX || c.children.empty?
      
      if c.value == Entity::ANY_VALUE # any one entity must comply
        matched = false
        db_each { |k, v|
          next if matched || k =~ @@ENTITY_ATTRIB_SEP_REGEX ||
                  !entity_complies?(db_key_to_str(k), c.children)
          matched = true
          # break
        }
        return false if !matched
      else # the one entity must comply
        return false if !entity_complies?(c.value, c.children)
      end
    }      

    true
  end

  # Creates an Entity object tree for an entity id.
  #
  # === Parameters
  # * _key_:: key of the attribute whose value this entity is (nil if root)
  # * _value_:: value of the entity (string or entity id)
  #
  def build_entity(key, value)
    
    # is string attribute or entity already in the tree
    return Entity.new(key, value) if value =~ @@ATTRIB_STR_VALUE_REGEX ||
                                     @created_entities[value]

    # mark entity as build to prevent loop when entities reference each other
    @created_entities[value] = 1
    
    # is entity (attribute)
    childs = []
    db_key_regex =
      /^#{Regexp.escape(str_to_db_key(value) + ENTITY_ATTRIB_SEP)}/
    db_each { |k, v|
      next if k !~ db_key_regex
      childs.push build_entity(db_key_to_str(k.sub(db_key_regex, "")),
                               db_get(k))
    }
    key ? Entity.new(key, value, childs) : RootEntity.new(value, childs)
  end
end