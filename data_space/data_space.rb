#!/usr/bin/env ruby -w

require "rubygems"
require "bdb"
require "fileutils"
require "pp"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "root_entity"
require "entity"

# This class implements the data space in a flat way such that the entities
# and all their attributes are stored as key/value pairs in the database.
#
# A entity is saved in this way:
# * entity_id -> nil
# * entity_id///attribute1_key -> attribute1_value1///attribute1_value2///...
# * ...
#
# If the +use_idx+ parameter is set to true, the data space uses two inverted
# indexes with the following structure:
# * first inverted index:
#   * attrib_value///attrib_key -> entity_id1///entity_id2///...
#   * ...
# * second inverted index:
#   * entity_id///attrib_value -> attrib_key1///attrib_key2///...
#   * ...
#
# If the +use_add_idx+ parameter is set to true, these additional indexes are
# used:
# * key index:
#   * attrib_key -> entity_id1///entity_id2///...
#   * ...
# * value index:
#   * attrib_value -> entity_id1///entity_id2///...
#   * ...
# * id index:
#   * entity_id -> attrib_key1///attrib_key2///...
#   * ...
#
# The data space supports mappings for attributes. One or a set of attributes
# for an entity can be mapped to a synonym attribute or set of attributes of
# the same entity. The mappings can be used by the search method.
#
# The mappings are saved in this way:
# * entity_id///serialized_original_attrib_set ->
#   serialized_synonym_attrib_set1///serialized_synonym_attrib_set2///...
# * ...
#
# Author: Thomas Hess (139467) (mailto:thomas.hess@studenti.unitn.it)
#
# :title:DataSpace

class DataSpace

  # separator in the database key and value fields
  DB_SEP = "///"
  
  # this string is not allowed in entity ids and attribute keys and values
  # occurances of +DB_SEP+ are replaced by this string
  DB_INVALID = "VeRysTr4nGEsTr1Ngn0b0dYW1lLeVerW4NTt0Use4s1d0RKey"
  
  # Generates a new +DataSpace+ instance. Opens the Berkeley DB file and its
  # databases.
  #
  # === Parameters
  # * _bdb_path_:: directory containing the Berkeley DB files to use
  #             :: (is created if doesn't exist)
  # * _use_idx_:: use inverted indexes
  # * _use_add_idx_:: use inverted indexes and additional indexes
  #
  def initialize(bdb_path, use_idx = true, use_add_idx = true)
    
    @use_idx = @use_add_idx = use_add_idx
    @use_idx ||= use_idx
    
    FileUtils::mkdir bdb_path unless File.exists? bdb_path
    
    @store = open_db(bdb_path, "store")
    @maps = open_db(bdb_path, "mappings")
    
    return unless @use_idx
    
    @idx1 = open_db(bdb_path, "index_1")
    @idx2 = open_db(bdb_path, "index_2")
    
    return unless @use_add_idx
    
    @k_idx = open_db(bdb_path, "key_index")
    @v_idx = open_db(bdb_path, "value_index")
    @id_idx = open_db(bdb_path, "id_index")
  end

  # Closes the databases. Call this before destroying the +DataSpace+
  # instance.
  #
  def close
    
    close_db @store
    close_db @maps
    
    return unless @use_idx
    
    [ @idx1, @idx2 ].each { |db| close_db db }
    
    return unless @use_add_idx
    
    [ @k_idx, @v_idx, @id_idx ].each { |db| close_db db }
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
    
    id_dbs = s_to_dbs(id)
    
    if @store[id_dbs]
      raise EntityExistsError.new id
    end
    
    @store[id_dbs] = "1"
  end

  # Removes an existing entity and all its attributes. Deletes also attributes
  # of other entities that have the entity as value.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  #
  # === Throws
  # * _NoEntityError_
  #
  def delete_entity(id)
    
    id_dbs = s_to_dbs(id)
    
    unless @store[id_dbs]
      raise NoEntityError.new id
    end

    # delete entity and its attributes
    db_del @store, id_dbs
    remove_entity_attributes id_dbs
    
    # delete attributes that have this entity as value
    #
    if @use_add_idx
      # lookup in +@v_idx+
      return unless v_idx_value = @v_idx[id_dbs]
      v_idx_value.split(DB_SEP).each { |v_idx_id_dbs|
        # lookup in +@idx2+
        next unless idx2_value = @idx2[v_idx_id_dbs + DB_SEP + id_dbs]
        idx2_value.split(DB_SEP).each { |key_dbs|
          db_remove_from_value @store, v_idx_id_dbs + DB_SEP + key_dbs, id_dbs
          remove_attribute_from_indexes v_idx_id_dbs, key_dbs, id_dbs
        }
      }
    elsif @use_idx
      # loop over +@idx1+
      idx1_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
      db_each(@idx1) { |idx1_key, idx1_value|
        next unless idx1_key =~ idx1_key_regex
        key_dbs = $'
        idx1_value.split(DB_SEP).each { |idx1_id_dbs|
          db_remove_from_value @store, idx1_id_dbs + DB_SEP + key_dbs, id_dbs
          remove_attribute_from_indexes idx1_id_dbs, key_dbs, id_dbs
        }
      }
    else
      # loop over +@store+
      db_each(@store) { |store_key, store_value|
        next unless store_key =~ @@DB_SEP_REGEX
        if db_remove_from_value @store, store_key, id_dbs
          remove_attribute_from_indexes $`, $', id_dbs
        end
      }
    end
  end

  # Removes all existing entities and their attributes by truncating the
  # databases.
  #
  def clear
    
    truncate_db @store
    truncate_db @maps
    
    return unless @use_idx
    
    [ @idx1, @idx2 ].each { |db| truncate_db db }
    
    return unless @use_add_idx
    
    [ @k_idx, @v_idx, @id_idx ].each { |db| truncate_db db }
  end

  # Adds a new attribute to an existing entity. An entity can have multiple
  # attributes with the same name or the same value, but not with the same
  # name and value. An attribute value can be a string (recognized by
  # surrounding quotes, i.e., "Trento" instead of Trento) or an entity
  # identifier. Entity identifiers (ids) are strings (without quotes).
  #
  # === Parameters
  # * _id_:: identifier of the entity
  # * _key_:: name of the attribute
  # * _value_:: value of the attribute
  #
  # === Throws
  # * _NoEntityError_:: if the entity, the attribute is for, or the entity,
  #                  :: the attribute referes to, does not exist
  # * _AttributeExistsError_:: if the entity already has an attribute with
  #                         :: this name and value
  #
  def insert_attribute(id, key, value)
    
    id_dbs = s_to_dbs(id)
    
    unless @store[id_dbs]
      raise NoEntityError.new id
    end
    
    key_dbs, value_dbs = s_to_dbs(key), s_to_dbs(value)
    
    # lookup in +@store+
    if db_value_contains?(@store, id_dbs + DB_SEP + key_dbs, value_dbs)
      raise AttributeExistsError.new id, key, value
    end
    
    # check value entity id
    unless value =~ @@ATTRIB_STR_VALUE_REGEX || @store[value_dbs]
      raise NoEntityError.new value
    end
    
    db_add_to_value @store, id_dbs + DB_SEP + key_dbs, value_dbs
    add_attribute_to_indexes id_dbs, key_dbs, value_dbs
  end

  # Removes an existing attribute from an entity. Also removes the attribute
  # from all mappings containing it.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  # * _key_:: name of the attribute (* to remove all attributes)
  # * _value_:: value of the attribute (* to remove all attributes with _key_)
  #
  # === Throws
  # * _NoAttributeError_
  #
  def delete_attribute(id, key, value)
    
    id_dbs, key_dbs, value_dbs = s_to_dbs(id), s_to_dbs(key), s_to_dbs(value)
    
    if key == Entity::ANY_VALUE && value == Entity::ANY_VALUE
      unless remove_entity_attributes(id_dbs)
        raise NoAttributeError.new id, key, value
      end
      return
    end
  
    if key == Entity::ANY_VALUE
      
      if @use_idx
        # lookup in +@idx2+
        unless idx2_value = @idx2[id_dbs + DB_SEP + value_dbs]
          raise NoAttributeError.new id, key, value
        end
        idx2_value.split(DB_SEP).each { |k_dbs|
          db_remove_from_value @store, id_dbs + DB_SEP + k_dbs, value_dbs
          remove_attribute_from_indexes id_dbs, k_dbs, value_dbs
        }
      else
        # loop over +@store+
        store_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
        store_value_regex = build_value_grep_regex value_dbs
        had_attrib = false
        db_each(@store) { |store_key, store_value|
          next unless store_key =~ store_key_regex
          k_dbs = $'
          next unless store_value =~ store_value_regex
          had_attrib = true
          db_remove_from_value @store, store_key, value_dbs
          remove_attribute_from_indexes id_dbs, k_dbs, value_dbs
        }
        raise NoAttributeError.new id, key, value unless had_attrib
      end
      
    elsif value == Entity::ANY_VALUE

      store_key = id_dbs + DB_SEP + key_dbs
      # lookup in +@store+
      unless store_value = @store[store_key]
        raise NoAttributeError.new id, key, value 
      end
      store_value.split(DB_SEP).each { |v_dbs|
        remove_attribute_from_indexes id_dbs, key_dbs, v_dbs
      }
      db_del @store, store_key
      
    else
      
      unless db_remove_from_value(@store, id_dbs + DB_SEP + key_dbs,
                                  value_dbs)
        raise NoAttributeError.new id, key, value
      end
      remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
      
    end  
      
    return if key == Entity::ANY_VALUE && value == Entity::ANY_VALUE
      
    # remove from mappings
    maps_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
    
    # loop over +@maps+
    db_each(@maps) { |maps_key, maps_value|
      next unless maps_key =~ maps_key_regex

      attribs = dbs_to_hash $'
      changed_attribs = false
      
      if key == Entity::ANY_VALUE
        
        contains_attrib = false
        attribs.each { |k, v|
          next unless value == v
          contains_attrib = true
          attribs.delete k
        }
        
        if contains_attrib
          db_del @maps, maps_key
          next if attribs.empty?
          @maps[id_dbs + DB_SEP + hash_to_dbs(attribs)] = maps_value
          changed_attribs = true
        end
        
      elsif attribs.key?(key) &&
            (value == Entity::ANY_VALUE || attribs[key] == value)
      
        db_del @maps, maps_key
        next if attribs.length == 1
    
        attribs.delete key
        @maps[id_dbs + DB_SEP + hash_to_dbs(attribs)] = maps_value
        changed_attribs = true
      end
      
      maps_value.split(DB_SEP).each { |maps_dbs|
        
        maps = dbs_to_hash maps_dbs
        if key == Entity::ANY_VALUE
          
          contains_attrib = false
          maps.each { |k, v|
            next unless value == v
            contains_attrib = true
            maps.delete k
          }

          if contains_attrib
            db_remove_from_value @maps, maps_key, maps_dbs
            next if maps.empty?
            db_add_to_value @maps, maps_key, hash_to_dbs(maps)
          end
          
        elsif maps.key?(key) && 
              (value == Entity::ANY_VALUE || maps[key] == value)
      
          db_remove_from_value @maps, maps_key, maps_dbs
          next if maps.length == 1
    
          maps.delete key
          db_add_to_value @maps, maps_key, hash_to_dbs(maps)
        end
        
        next unless changed_attribs && maps.contains?(attribs)
        # original attributes are included in synonym attributes, this
        # mapping makes no sense anymore
        db_remove_from_value @maps, maps_key, maps_dbs
      }
    }
  end

  # Adds a new mapping for one or a set of existing attributes of an entity to
  # another existing attribute or set of attributes of the same entity. Use
  # this to express attribute synonymity.
  #
  # === Parameters
  # * _id_:: identifier of the attributes' entity
  # * _attribs_:: hash with the original attribute name/value pairs
  # * _maps_:: hash with the synonym attribute name/value pairs
  #
  # === Throws
  # * _ArgumentError_:: if _attrib_ or _map_ is no hash
  # * _NoAttributeError_:: if a (original or synonym) attribute doesn't exist
  # * _MappingExistsError_:: if the mapping already exists
  #
  def insert_attribute_mapping(id, attribs, maps)

    unless attribs.instance_of?(Hash) && maps.instance_of?(Hash)
      raise ArgumentError, "attribs and maps must be hashes"
    end

    if maps.contains? attribs
      raise ArgumentError, "original attributes are included in synonym attributes, this mapping makes no sense"
    end

    id_dbs = s_to_dbs(id)

    [attribs, maps].each { |hash|
      hash.each { |k, v|
        unless db_value_contains? @store, id_dbs + DB_SEP + s_to_dbs(k),
                                  s_to_dbs(v)
          raise NoAttributeError.new id, k, v
        end
      }
    }

    maps_key = id_dbs + DB_SEP + hash_to_dbs(attribs)
    maps_dbs = hash_to_dbs maps
    if db_value_contains? @maps, maps_key, maps_dbs
      raise MappingExistsError.new id, attribs, maps
    end

    db_add_to_value @maps, maps_key, maps_dbs
  end

  # Deletes an existing mapping for one or a set of attributes of an entity.
  #
  # === Parameters
  # * _id_:: identifier of the attributes' entity
  # * _attribs_:: hash with the original attribute name/value pairs
  # * _maps_:: hash with the synonym attribute name/value pairs (or * to
  #         :: remove all mappings for _attribs_)
  #
  # === Throws
  # * _ArgumentError_:: if _attribs_ is no hash or _maps_ is neither hash nor
  #                  :: *
  # * _NoMappingError_:: if the mapping does not exist
  #
  def delete_attribute_mapping(id, attribs, maps)

    unless attribs.instance_of?(Hash) &&
           (maps.instance_of?(Hash) || maps == Entity::ANY_VALUE)
      raise ArgumentError, "attribs must be a hash and maps must be hash or *"
    end

    maps_key = s_to_dbs(id) + DB_SEP + hash_to_dbs(attribs);
    
    if maps.instance_of? Hash
      unless db_remove_from_value(@maps, maps_key, hash_to_dbs(maps))
        raise NoMappingError.new id, attribs, maps
      end
    else
      db_del @maps, maps_key
    end
  end

  # Performs a search specified by an +Entity+ object tree. Returns an array
  # of matching entity ids as result.
  #
  # === Parameters
  # * _query_:: +RootEntity+ object
  # * _use_maps_:: use mappings
  # * _verb_:: print debug output
  #
  # === Returns
  # * Array of entity ids.
  #
  def search(query, use_maps = false, verb = false)
    
    unless query.instance_of?(RootEntity) || query.instance_of?(Entity)
      raise ArgumentError, "query must be an instance of RootEntity or Entity"
    end
    
    results = []
    
    # collect entities
    if query.value == Entity::ANY_VALUE || query.value =~ @@VAR_REGEX
      # loop over +@store+
      db_each(@store) { |store_key, store_value|
        results << store_key unless store_key =~ @@DB_SEP_REGEX
      }
    else
      # lookup in +@store+
      id_dbs = s_to_dbs(query.value)
      results << id_dbs if @store[id_dbs]
    end
    
    # check entities
    results.delete_if { |id_dbs|
      # TODO enable variables among results
      vars = query.value =~ @@VAR_REGEX ? {$' => id_dbs} : {}
      if entity_complies?(id_dbs, query.children, vars, use_maps, verb)
        puts "TRUE #{id_dbs} complies" if verb
        next
      end
      if verb
        puts "FALSE #{id_dbs} doesn't comply"
        puts "=" * 60
      end
      true
    }
    
    results.map { |id_dbs| dbs_to_s(id_dbs) }
  end
  
  # Retrieves the +Entity+ object tree for an entity id.
  #
  # === Parameters
  # * _id_:: entity id
  #
  # === Returns
  # * +RootEntity+ object
  #
  def get_entity(id)
    
    id_dbs = s_to_dbs(id)
    
    unless @store[id_dbs]
      raise NoEntityError.new id
    end
    
    @created_entities = {}
    build_entity nil, id, id_dbs
  end
  
  # Prints the content of all databases. For debugging only.
  #
  def dump
    
    dump_db "store", @store
    dump_db "mappings", @maps
    
    return unless @use_idx
    
    {
      "index 1" => @idx1,
      "index 2" => @idx2
    }.each { |name, db| dump_db name, db }
    
    return unless @use_add_idx
    
    {
      "key index" => @k_idx,
      "value index" => @v_idx,
      "id index" => @id_idx
    }.each { |name, db| dump_db name, db }
  end
  
  class OpenDatabaseError < StandardError
    
    def initialize(db)
      @db = db
      super "Error opening database file '#{@db}'."
    end
    
    attr_reader :db
  end

  class EntityExistsError < StandardError

    def initialize(id)
      @id = id
      super "Entity '#{@id}' already exists."
    end

    attr_reader :id
  end
  
  class AttributeExistsError < StandardError
    
    def initialize(id, key, value)
      @id, @key, @value = id, key, value
      super "Attribute '#{@key}' with value '#{@value}' for entity '#{@id}' already exists."
    end
    
    attr_reader :id, :key, :value
  end

  class MappingExistsError < StandardError

    def initialize(id, attribs, maps)
      @id, @attribs, @maps = id, attribs, maps
      super "'#{@attribs}' are already mapped to '#{@maps}' for entity '#{@id}'."
    end

    attr_reader :id, :attribs, :maps
  end
  
  class NoEntityError < StandardError
    
    def initialize(id)
      @id = id
      super "Entity '#{@id}' does not exists."
    end
    
    attr_reader :id
  end
  
  class NoAttributeError < StandardError
    
    def initialize(id, key, value)
      @id, @key, @value = id, key, value
      super "Attribute '#{@key}' with value '#{@value}' for entity '#{@id}' does not exists."
    end
    
    attr_reader :id, :key, :value
  end

  class NoMappingError < StandardError

    def initialize(id, attribs, maps)
      @id, @attribs, @maps = id, attribs, maps
      super "'#{@attribs}' are not mapped to '#{@maps}' for entity '#{@id}'."
    end

    attr_reader :id, :attribs, :maps
  end

  private
  
  @@SHOW_DB_USE = true
    
  @@DB_SEP_ESC = Regexp.escape DB_SEP
  @@DB_SEP_REGEX = /#{@@DB_SEP_ESC}/
  @@DB_INVALID_REGEX = /#{Regexp.escape(DB_INVALID)}/
  @@ATTRIB_STR_VALUE_REGEX = /^".*"$/
  @@VAR_REGEX = /^\$/
  
  # Opens a BDB database inside a directory.
  #
  # === Parameters
  # * _dir_path_:: directory containing the Berkeley DB file
  # * _bdb_name_:: base name of the Berkeley DB file
  #
  # === Throws
  # * _OpenDatabaseError_
  #
  # === Returns
  # * +Bdb::Db+ object
  #
  def open_db(dir_path, bdb_name)
    bdb_path = File.join(dir_path, bdb_name + ".bdb")
    db = Bdb::Db.new
    begin
      db.open nil, bdb_path, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0
    rescue
      raise OpenDatabaseError.new(bdb_path)
    end
  end
  
  # Closes a +Bdb::Db+ database.
  #
  # === Parameters
  # * _db_:: +Bdb::Db+ object
  #
  def close_db(db)
    db.close 0
  end
  
  # Truncates a +Bdb::Db+ database.
  #
  # === Parameters
  # * _db_:: +Bdb::Db+ object
  #
  def truncate_db(db)
    db.truncate nil
  end

  # Prints the content of a database.
  #
  # === Parameters
  # * _name_:: name to print as header
  # * _db_:: +Bdb::Db+ object
  #
  def dump_db(name, db)
    puts "\n#{name}"
    puts "-" * name.length
    db_each(db) { |k, v| puts "#{dbs_to_s(k)} => #{dbs_to_s(v)}" }
  end
  
  # Removes a pair from a database.
  #
  # === Parameters
  # * _db_:: database
  # * _key_dbs_:: key of the pair
  #
  def db_del(db, key_dbs)
    db.del(nil, key_dbs, 0)
  end
  
  # Loops over a database. Call with block. Return +false+ from the block to
  # break out of the loop.
  #
  # === Parameters
  # * _db_:: database
  #
  def db_each(db)
    dbc = db.cursor(nil, 0)
    k, v = dbc.get(nil, nil, Bdb::DB_FIRST)
    while k
      break if yield(k, v) == false
      k, v = dbc.get(nil, nil, Bdb::DB_NEXT)
    end
    dbc.close
  end

  # Checks if the value of a pair in a database contains a dbs. The value can
  # be the dbs or the dbs can be part of the value separated by +DB_SEP+.
  #
  # === Parameters
  # * _db_:: database
  # * _key_dbs_:: key of the pair
  # * _dbs_:: dbs to look for in the value
  #
  # === Returns
  # * +true+ if dbs is in value, or +false+ if not
  #
  def db_value_contains?(db, key_dbs, dbs)
    db[key_dbs] =~ build_value_grep_regex(dbs)
  end
  
  # Generates a +Regexp+ for matching a dbs in a database value separated by
  # +DB_SEP+.
  #
  # === Parameters
  # * _dbs_:: dbs to look for in the value
  #
  # === Returns
  # * +Regexpr+
  #
  def build_value_grep_regex(dbs)
    /(^ | #{@@DB_SEP_ESC})
     #{Regexp.escape(dbs)}
     (#{@@DB_SEP_ESC} | $)/x
  end

  # Adds a dbs to the value of a pair in a database. If the pair doesn't
  # exists, it is created. Otherwise dbs is appended to the value (separated
  # by +DB_SEP+).
  #
  # === Parameters
  # * _db_:: database
  # * _key_dbs_:: key of the pair
  # * _dbs_:: dbs to add to the value
  #
  def db_add_to_value(db, key_dbs, dbs)
    return if db_value_contains? db, key_dbs, dbs
    db[key_dbs] = db[key_dbs] ? db[key_dbs] + DB_SEP + dbs : dbs
  end

  # Removes a dbs from the value of a pair in a database. If the dbs is the
  # value for the key, the pair is deleted. If the dbs is part of the value
  # separated by +DB_SEP+, the dbs is removed from the value.
  #
  # === Parameters
  # * _db_:: database
  # * _key_dbs_:: key of the pair
  # * _dbs_:: dbs to remove from the value
  #
  # === Returns
  # * +true+ if removed, or +false+ if pair doesn't exist or dbs not in value
  #
  def db_remove_from_value(db, key_dbs, dbs)
    return false unless value_dbs = db[key_dbs]
    dbs_esc = Regexp.escape(dbs)
    return false unless value_dbs =~ /#{dbs_esc}/
    if value_dbs == dbs
      # is the value -> delete the pair
      db_del db, key_dbs
    else
      # is part of the value -> remove from value
      if value_dbs =~ /#{dbs_esc}$/
        db[key_dbs] = value_dbs.chomp(DB_SEP + dbs)
      else
        db[key_dbs] = value_dbs.sub(dbs + DB_SEP, "")
      end
    end
    true
  end
  
  # Prepares string for usage in the database by replacing occurences of
  # +DB_SEP+.
  #
  # === Parameters
  # * _s_:: string
  #
  # === Throws
  # * _ArgumentError_:: if _s_ contains an occurance of DB_INVALID
  #
  def s_to_dbs(s)
    
    if s =~ @@DB_INVALID_REGEX
      raise ArgumentError,
            "entity identifiers and attribute names cannot contain the " +
            "string '#{DB_INVALID}'"
    end
    
    s.gsub @@DB_SEP_REGEX, DB_INVALID
  end
  
  # Reverts the replacements done by +s_to_dbs+.
  #
  # === Parameters
  # * _dbs_:: string modified by +s_to_dbs+
  #
  def dbs_to_s(dbs)
    dbs.gsub @@DB_INVALID_REGEX, DB_SEP
  end
  
  # Prepares a hash for usage in the database by serializing and replacing
  # occurences of +DB_SEP+ in the serialization.
  #
  # === Parameters
  # * _hash_:: hash
  #
  def hash_to_dbs(hash)
    s_to_dbs Marshal.dump(hash)
  end
  
  # Reverts the modification done by +hash_to_dbs+.
  #
  # === Parameters
  # * _dbs_:: hash modified by +hash_to_dbs+
  #
  def dbs_to_hash(dbs)
    Marshal.load dbs_to_s(dbs)
  end

  # Removes all attributes of an entity.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  #
  # === Returns
  # * +true+ if any attributes removed, +false+ if none
  #
  def remove_entity_attributes(id_dbs)
    if @use_add_idx
      # lookup in +@id_idx+
      return false unless id_idx_value = @id_idx[id_dbs]
      id_idx_value.split(DB_SEP).each { |key_dbs|
        store_key = id_dbs + DB_SEP + key_dbs
        @store[store_key].split(DB_SEP).each { |value_dbs|
          remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
        }
        db_del @store, store_key
      }
    else
      # loop over +@store+
      store_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
      had_attrib = false
      db_each(@store) { |store_key, store_value|
        next unless store_key =~ store_key_regex
        had_attrib = true
        key_dbs = $'
        db_del @store, store_key
        store_value.split(DB_SEP).each { |value_dbs|
          remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
        }
      }
      return false unless had_attrib
    end
    maps_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
    # loop over +@maps+
    db_each(@maps) { |maps_key, maps_value|
      next unless maps_key =~ maps_key_regex
      db_del @maps, maps_key
    }
    true
  end
  
  # Adds an attribute to the indexes.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _key_dbs_:: attribute name
  # * _value_dbs_:: attribute value
  #
  def add_attribute_to_indexes(id_dbs, key_dbs, value_dbs)
    
    return unless @use_idx
    
    db_add_to_value @idx1, value_dbs + DB_SEP + key_dbs, id_dbs
    db_add_to_value @idx2, id_dbs + DB_SEP + value_dbs, key_dbs

    return unless @use_add_idx
    
    db_add_to_value @k_idx, key_dbs, id_dbs
    db_add_to_value @v_idx, value_dbs, id_dbs
    db_add_to_value @id_idx, id_dbs, key_dbs
  end
  
  # Removes an attribute from the indexes.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _key_dbs_:: attribute name
  # * _value_dbs_:: attribute value
  #
  def remove_attribute_from_indexes(id_dbs, key_dbs, value_dbs)

    return unless @use_idx
    
    db_remove_from_value @idx1, value_dbs + DB_SEP + key_dbs, id_dbs
    db_remove_from_value @idx2, id_dbs + DB_SEP + value_dbs, key_dbs

    return unless @use_add_idx
    
    db_remove_from_value @k_idx, key_dbs, id_dbs
    db_remove_from_value @v_idx, value_dbs, id_dbs
    db_remove_from_value @id_idx, id_dbs, key_dbs
  end
  
  # Checks if an entity fulfills the conditions expressed by +Entity+ objects.
  # Runs recursively through the +Entity+ object trees.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _conditions_:: array of +Entity+ object (trees)
  # * _vars_:: hash of query variables with their values
  # * _use_maps_:: use mappings
  # * _verb_:: print debug output
  #
  # === Returns
  # * +true+ if conditions fulfilled, +false+ if not
  #
  def entity_complies?(id_dbs, conditions, vars = {},
                       use_maps = false, verb = false)

    return true if conditions.empty?   
     
    if verb
      puts "-" * 60
      puts id_dbs
      puts conditions.each { |c| c.to_s }
      pp vars
      puts "-" * 60
    end

    conditions.each { |child|
        
      key_dbs, value_dbs = s_to_dbs(child.key), s_to_dbs(child.value)
      
      if child.key =~ @@VAR_REGEX
        if vars[$']
          # replace variable with value
          key_dbs = vars[$']
        else
          # treat as wildcard
          key_var = $'
        end
      end
      
      if child.value =~ @@VAR_REGEX
        if vars[$']
          # replace variable with value
          value_dbs = vars[$']
        else
          # treat as wildcard
          value_var = $'
        end
      end
      
      if (child.key == Entity::ANY_VALUE || key_var) &&
         (child.value == Entity::ANY_VALUE || value_var)
        # any attribute with any value 
        
        key_dbs, value_dbs = {}, []
        
        if @use_add_idx
          
          # lookup in +@id_idx+
          unless id_idx_value = @id_idx[id_dbs]
            puts "FALSE no @id_idx[#{id_dbs}]" if verb
            # XXX check mappings
            return false
          end
          id_idx_value.split(DB_SEP).each { |k_dbs|
            # lookup in +@store+
            v_dbs = @store[id_dbs + DB_SEP + k_dbs].split(DB_SEP)
            if key_var
              key_dbs[k_dbs] = v_dbs
            else
              value_dbs = value_dbs | v_dbs
            end
          }
        
        else
        
          # loop over +@store+
          store_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
          matched = false
          db_each(@store) { |store_key, store_value|
            next unless store_key =~ store_key_regex
            matched = true
            if key_var
              key_dbs[$'] = store_value.split(DB_SEP)
            else
              value_dbs = value_dbs | store_value.split(DB_SEP)
            end
          }  
          unless matched
            puts "FALSE #{id_dbs} not in @store[#{store_key_regex}]" if verb
            # XXX check mappings
            return false
          end
        
        end
        
        # recurse over all key and/or value values
        found_value = false
        if key_var
          key_dbs.each { |k_dbs, value_dbs|
            next if vars.value?(k_dbs) # vars must differ
            vars[key_var] = k_dbs # diff var value per run
            value_dbs.each { |v_dbs|
              next if value_var && vars.value?(v_dbs) # vars must differ
              puts "checking #{k_dbs} : #{v_dbs}" if verb
              vars[value_var] = v_dbs if value_var # diff var value per run
              unless entity_complies?(v_dbs, child.children, vars, verb)
                puts "FALSE #{v_dbs} doesn't comply" if verb
                next
              end
              found_value = true
              break
            }
            break if found_value
          }
        else
          value_dbs.each { |v_dbs|
            next if value_var && vars.value?(v_dbs) # vars must differ
            puts "checking * : #{v_dbs}" if verb
            vars[value_var] = v_dbs if value_var # diff var value per run
            unless entity_complies?(v_dbs, child.children, vars, verb)
              puts "FALSE #{v_dbs} doesn't comply" if verb
              next
            end
            found_value = true
            break
          }
        end
        unless found_value
          puts "FALSE no possible key and/or value value" if verb
          # XXX check mappings
          return false
        end
        
      elsif child.key == Entity::ANY_VALUE || key_var
        # any attribute must have the value
        
        key_dbs = []

        if @use_idx

          # lookup in +@idx2+
          unless idx2_value = @idx2[id_dbs + DB_SEP + value_dbs]
            puts "FALSE no @idx2[#{id_dbs + DB_SEP + key_dbs}]" if verb
            # XXX check mappings
            return false
          end
          key_dbs = key_dbs | idx2_value.split(DB_SEP) if key_var

        else

          # loop over +@store+
          store_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
          store_value_regex = build_value_grep_regex(value_dbs)
          matched = false
          db_each(@store) { |store_key, store_value|
            next unless store_key =~ store_key_regex
            k_dbs = $'
            next unless store_value =~ store_value_regex
            matched = true
            key_var ? key_dbs << k_dbs : false # collect var values or break
          }
          unless matched
            puts "FALSE #{value_dbs} not in @store[#{store_key_regex}]" if verb
            # XXX check mappings
            return false
          end

        end
        
        if key_var
          # recurse over all key values
          found_value = false
          key_dbs.each { |k_dbs|
            next if vars.value?(k_dbs) # vars must differ
            vars[key_var] = k_dbs # diff var value per run
            unless entity_complies?(value_dbs, child.children, vars, verb)
              puts "FALSE #{value_dbs} doesn't comply" if verb
              next
            end
            found_value = true
            break
          }
          unless found_value
            puts "FALSE no possible key value" if verb
            # XXX check mappings
            return false
          end
        else
          # recurse
          next if value_dbs =~ @@ATTRIB_STR_VALUE_REGEX
          unless entity_complies?(value_dbs, child.children, vars, verb)
            puts "FALSE #{value_dbs} doesn't comply" if verb
            # XXX check mappings
            return false
          end
        end
        
      elsif child.value == Entity::ANY_VALUE || value_var
        # the attribute must exist with any value
        
        # lookup in +@store+
        unless store_value = @store[id_dbs + DB_SEP + key_dbs]
          puts "FALSE no @store[#{id_dbs + DB_SEP + key_dbs}]" if verb
          # XXX check mappings
          return false
        end
        
        # recurse over all value values
        found_value = false
        store_value.split(DB_SEP).each { |v_dbs|
          next if value_var && vars.value?(v_dbs) # vars must differ
          vars[value_var] = v_dbs if value_var # diff var value per run
          unless entity_complies?(v_dbs, child.children, vars, verb)
            puts "FALSE #{v_dbs} doesn't comply" if verb
            next
          end
          found_value = true
          break
        }  
        unless found_value
          puts "FALSE no possible value value" if verb
          # XXX check mappings
          return false
        end
        
      else
        # the attribute must exist and must have the value
        
        # lookup in +@store+
        store_key = id_dbs + DB_SEP + key_dbs
        unless db_value_contains? @store, store_key, value_dbs
          if use_maps
            # XXX
          end
          puts "FALSE #{value_dbs} not in @store[#{store_key}]" if verb
          return false
        end
        
        # recurse
        next if value_dbs =~ @@ATTRIB_STR_VALUE_REGEX
        unless entity_complies?(value_dbs, child.children, vars, verb)
          puts "FALSE #{value_dbs} doesn't comply" if verb
          return false
        end
      end
    }      

    puts "TRUE" if verb
    true
  end

  # Creates an +Entity+ object tree for an entity id. Child entities are also
  # expanded as long as they haven't been expanded before in the tree. In that
  # case they are represented by their id. Uses the +@created_entities+ hash
  # to store the build entities.
  #
  # === Parameters
  # * _key_:: key of the attribute whose value this entity is (nil if root)
  # * _value_:: value of the entity (string or entity id)
  # * _value_dbs_:: _value_ prepared by +s_to_dbs+
  #
  def build_entity(key, value, value_dbs)
    
    # is string attribute or entity already in the tree
    return Entity.new(key, value) if value =~ @@ATTRIB_STR_VALUE_REGEX ||
                                     @created_entities[value]

    # mark entity as build to prevent loop when entities reference each other
    @created_entities[value] = 1
    
    # value is entity id
    childs = []
    if @use_add_idx
      # lookup in +@id_idx+
      if id_idx_value = @id_idx[value_dbs]
        id_idx_value.split(DB_SEP).each { |key_dbs|
          # lookup in +@store+
          child_key = dbs_to_s(key_dbs)
          @store[value_dbs + DB_SEP + key_dbs].split(DB_SEP).each { |v|
            childs << build_entity(child_key, dbs_to_s(v), v)
          }
        }
      end
    else
      # loop over +@store+
      store_key_regex = /^#{Regexp.escape(value_dbs) + @@DB_SEP_ESC}/
      db_each(@store) { |store_key, store_value|
        next unless store_key =~ store_key_regex
        child_key = dbs_to_s($')
        store_value.split(DB_SEP).each { |v|
          childs << build_entity(child_key, dbs_to_s(v), v)
        }
      }
    end
    
    key ? Entity.new(key, value, childs) : RootEntity.new(value, childs)
  end
end

class Hash
  
  # Checks if this hash contains another hash. If the intersection of this and
  # the other hash equals the other hash.
  #
  # === Parameters
  # * _hash_:: the other hash
  #
  def contains?(hash)
    contains = true
    hash.each { |k, v|
      next if self.key?(k) && self[k] == v
      contains = false
      break
    }
    contains
  end
end