#!/usr/bin/env ruby -w

require "rubygems"
require "bdb"
require "fileutils"
require "pp"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "root_entity"
require "entity"
require "attributes"

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
  # * _options_:: options hash (available keys: _:use_indexes_ (use inverted
  #            :: indexes) and _:use_all_indexes_ (use inverted indexes and
  #            :: additional indexes))
  #
  def initialize(bdb_path, options = {})
    
    @use_idx = @use_add_idx = options[:use_all_indexes]
    @use_idx ||= options[:use_indexes]
    
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
          remove_attribute_from_mappings v_idx_id_dbs, dbs_to_s(key_dbs), id
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
          remove_attribute_from_mappings idx1_id_dbs, dbs_to_s(key_dbs), id
        }
      }
    else
      # loop over +@store+
      db_each(@store) { |store_key, store_value|
        next unless store_key =~ @@DB_SEP_REGEX
        if db_remove_from_value @store, store_key, id_dbs
          remove_attribute_from_indexes $`, $', id_dbs
          remove_attribute_from_mappings $`, dbs_to_s($'), id
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
  # from all entity specific mappings containing it.
  #
  # === Parameters
  # * _id_:: identifier of the entity
  # * _key_:: name of the attribute (* to remove all attributes for _id_)
  # * _value_:: value of the attribute (* to remove all attributes with _key_)
  #
  # === Throws
  # * _NoEntityError_
  # * _NoAttributeError_
  #
  def delete_attribute(id, key, value)
    
    id_dbs = s_to_dbs(id)
    
    unless @store[id_dbs]
      raise NoEntityError.new id
    end
    
    key_dbs, value_dbs = s_to_dbs(key), s_to_dbs(value)
    
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
          remove_attribute_from_mappings id_dbs, dbs_to_s(k_dbs), value
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
          remove_attribute_from_mappings id_dbs, dbs_to_s(k_dbs), value
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
        remove_attribute_from_mappings id_dbs, key, dbs_to_s(v_dbs)
      }
      db_del @store, store_key
      
    else
      
      unless db_remove_from_value(@store, id_dbs + DB_SEP + key_dbs,
                                  value_dbs)
        raise NoAttributeError.new id, key, value
      end
      remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
      remove_attribute_from_mappings id_dbs, key, value
      
    end
  end

  # Adds a new mapping for one or multiple existing attributes. The mapping as
  # well can consist of one or multiple attribute name/value pairs. Mappings
  # can be created either for a specific entity or generic.
  #
  # === Parameters
  # * _id_:: identifier of the attributes' entity (or * to make the mapping
  #       :: generic)
  # * _attrib_:: +Attributes+ with the existing attribute name/value pairs
  # * _mapping_:: +Attributes+ with the mapping attribute name/value pairs
  #
  # === Throws
  # * _ArgumentError_:: if _attrib_ or _mapping_ is not an instance of
  #                  :: +Attributes+ or if one array is included in the other
  # * _NoEntityError_:: if the mapping is specific and the entity does not
  #                  :: exist
  # * _NoAttributeError_:: if an attribute does not exist
  # * _MappingExistsError_:: if the mapping already exists
  #
  def insert_attribute_mapping(id, attrib, mapping)

    id_dbs = s_to_dbs(id)

    unless id == Entity::ANY_VALUE || @store[id_dbs]
      raise NoEntityError.new id
    end

    unless attrib.instance_of?(Attributes) && mapping.instance_of?(Attributes)
      raise ArgumentError, "attrib and mapping must instances of Attributes"
    end
    
    if attrib.contains?(mapping) || mapping.contains?(attrib)
      raise ArgumentError, "one attribute set is included in the other, this mapping makes no sense"
    end
    
    unless id == Entity::ANY_VALUE
      attrib.pairs.each { |pair|
        unless db_value_contains? @store, id_dbs + DB_SEP + s_to_dbs(pair[0]),
                                  s_to_dbs(pair[1])
          raise NoAttributeError.new id, pair[0], pair[1]
        end
      }
    end
    
    attrib_dbs, mapping_dbs = array_to_dbs(attrib.pairs),
                              array_to_dbs(mapping.pairs)

    maps_key = id_dbs + DB_SEP + mapping_dbs
    if db_value_contains?(@maps, maps_key, attrib_dbs)
      raise MappingExistsError.new id, attrib, mapping
    end
    db_add_to_value @maps, maps_key, attrib_dbs
  end

  # Deletes an existing generic or entity specific mapping.
  #
  # === Parameters
  # * _id_:: identifier of the attributes' entity (or * for a generic mapping)
  # * _attrib_:: +Attributes+ with the existing attribute name/value pairs (or
  #           :: to remove all mappings with for _id_)
  # * _mapping_:: +Attributes+ with the mapping attribute name/value pairs (or
  #            :: to remove all mappings for _attrib_)
  #
  # === Throws
  # * _ArgumentError_:: if _attrib_ or _mapping_ is neither instance of
  #                  :: +Attributes+ nor *
  # * _NoEntityError_:: if the mappping is specific and the entity does not
  #                  :: exist
  # * _NoMappingError_:: if the mapping does not exist
  #
  def delete_attribute_mapping(id, attrib, mapping)

    id_dbs = s_to_dbs(id)

    unless id == Entity::ANY_VALUE || @store[id_dbs]
      raise NoEntityError.new id
    end

    unless (attrib.instance_of?(Attributes) || attrib == Entity::ANY_VALUE) &&
           (mapping.instance_of?(Attributes) || mapping == Entity::ANY_VALUE)
      raise ArgumentError, "attrib and mapping must be either instance of Attributes or *"
    end
    
    if attrib.instance_of?(Attributes) && mapping.instance_of?(Attributes)
      
      attrib_dbs, mapping_dbs = array_to_dbs(attrib.pairs),
                                array_to_dbs(mapping.pairs)
      unless db_remove_from_value(@maps, id_dbs + DB_SEP + mapping_dbs,
                                  attrib_dbs)
        raise NoMappingError.new id, attrib, mapping
      end
      
    else
      
      attrib_dbs = attrib.instance_of?(Attributes) ?
                   array_to_dbs(attrib.pairs) : nil
      removed_mapping = false
      maps_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
      # loop over +@maps+
      db_each(@maps) { |maps_key, maps_value|
        next unless maps_key =~ maps_key_regex
        if attrib.instance_of?(Attributes)
          removed_mapping = true if db_remove_from_value(@maps, maps_key,
                                                         attrib_dbs)
        else
          db_del @maps, maps_key
          removed_mapping = true
        end
      }
      unless removed_mapping
        raise NoMappingError.new id, attrib, mapping
      end
      
    end
  end

  # Performs a search specified by an +Entity+ object tree. Returns an array
  # of matching entity ids as result.
  #
  # === Parameters
  # * _query_:: +RootEntity+ object
  # * _options_:: options hash (available keys: _:use_mappings_ and _:verbose_
  #            :: (print debug output))
  #
  # === Returns
  # * Array of entity ids.
  #
  def search(query, options = {})
    
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
    results.delete_if { |r_id_dbs|
      # TODO enable variables among results
      vars = query.value =~ @@VAR_REGEX ? {$' => r_id_dbs} : {}
      if (options[:use_mappings] &&
          entity_complies_with_mappings?(r_id_dbs, query.children,
                                         vars, options[:verbose])) ||
          entity_complies?(r_id_dbs, query.children, vars, false,
                           options[:verbose])
        puts "TRUE #{r_id_dbs} complies" if options[:verbose]
        next
      end
      if options[:verbose]
        puts "FALSE #{r_id_dbs} doesn't comply"
        puts "=" * 60
      end
      true
    }
    
    results.map { |r_id_dbs| dbs_to_s(r_id_dbs) }
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

    def initialize(id, attrib, mapping)
      @id, @attrib, @mapping = id, attrib, mapping
      super "'#{@attrib}' are already mapped to '#{@mapping}' for entity '#{@id}'."
    end

    attr_reader :id, :attrib, :mapping
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

    def initialize(id, attrib, mapping)
      @id, @attrib, @mapping = id, attrib, mapping
      super "'#{@attrib}' are not mapped to '#{@mapping}' for entity '#{@id}'."
    end

    attr_reader :id, :attrib, :mapping
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
    
    unless s.instance_of?(String)
      raise ArgumentError, "s must be a string"
    end
    
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
    
    unless dbs.instance_of?(String)
      raise ArgumentError, "dbs must be a string"
    end
    
    dbs.gsub @@DB_INVALID_REGEX, DB_SEP
  end
  
  # Prepares an array for usage in the database by sorting, serializing and
  # replacing occurences of +DB_SEP+ in the serialization.
  #
  # === Parameters
  # * _array_:: array
  #
  def array_to_dbs(array)
    
    unless array.instance_of?(Array)
      raise ArgumentError, "array must be a array"
    end
    
    s_to_dbs Marshal.dump(array.sort)
  end
  
  # Reverts the modification done by +array_to_dbs+.
  #
  # === Parameters
  # * _dbs_:: hash modified by +array_to_dbs+
  #
  def dbs_to_array(dbs)
    
    unless dbs.instance_of?(String)
      raise ArgumentError, "dbs must be a string"
    end
    
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
          remove_attribute_from_mappings id_dbs, dbs_to_s(key_dbs),
                                         dbs_to_s(value_dbs)
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
          remove_attribute_from_mappings id_dbs, dbs_to_s(key_dbs),
                                         dbs_to_s(value_dbs)
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
  
  # Removes an attribute from the mappings.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _key_:: attribute name
  # * _value_:: attribute value
  #
  def remove_attribute_from_mappings(id_dbs, key, value)
    
    pair = [key, value]
    maps_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
    # loop over +@maps+
    db_each(@maps) { |maps_key, maps_value|
      next unless maps_key =~ maps_key_regex
      maps_value.split(DB_SEP).each { |attrib_dbs|
        attrib = dbs_to_array(attrib_dbs)
        next unless attrib.include?(pair)
        db_remove_from_value @maps, maps_key, attrib_dbs
        next if attrib.length == 1 # attrib to delete is the only attrib
        attrib.delete pair
        db_add_to_value @maps, maps_key, array_to_dbs(attrib)
      }
    }
  end
  
  # Checks if an entity fulfills the conditions expressed by +Entity+ objects
  # by also considering the stored attribute mappings. Generates all possible
  # partitions of the conditions and uses the stored mappings for the subsets
  # as alternatives.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _conditions_:: array of +Entity+ objects (trees)
  # * _vars_:: hash of query variables with their values
  # * _verb_:: print debug output
  #
  # === Returns
  # * +true+ if conditions fulfilled, +false+ if not
  #
  def entity_complies_with_mappings?(id_dbs, conditions, vars = {},
                                     verb = false)
                                     
    # has to comply for one partitioning
    partitioning_complies = false
    get_partitionings(conditions) { |partitioning|
      if verb
        s = "PARTITIONING: ["
        partitioning.each { |p|
          s += "["
          p.each { |g| s += g.to_s.chomp + ", " }
          s.chomp! ", "
          s += "]"
        }
        s += "]"
        puts s
      end
      # has to comply for all partitions
      partitions_comply = true
      partitioning.each { |partition|
        if verb
          s = "PARTITION: ["
          partition.each { |e| s += e.to_s.chomp + ", " }
          s.chomp! ", "
          s += "]"
          puts s
        end
        partition_array = []
        partition.each { |e|
          partition_array << [e.key, e.value]
        }
        if partition.length != partition_array.length && verb
          puts "ERROR translation to partition array went wrong"
        end
        alt_childs = [partition]
        # look for mappings
        partition_array_dbs = array_to_dbs(partition_array);
        { "SPECIFIC" => id_dbs, "GENERIC" => Entity::ANY_VALUE }.each { |map_type, map_id_dbs|
          mappings = @maps[map_id_dbs + DB_SEP + partition_array_dbs]
          unless mappings.nil?
            puts "NUM #{map_type} MAPPINGS FOUND: #{mappings.split(DB_SEP).length}" if verb
            mappings.split(DB_SEP).each { |mapping_dbs|
              childs = []
              dbs_to_array(mapping_dbs).each { |pair|
                childs << Entity.new(pair[0], pair[1])
              }
              alt_childs << childs
            }
          else
            puts "NO #{map_type} MAPPINGS FOUND" if verb
          end
        }
        # has to comply for one alternative
        alt_complies = false
        alt_childs.each { |childs|
          if verb
            s = "ALTERNATIVE: "
            childs.each { |e| s += e.to_s.chomp + ", " }
            s.chomp! ", "
            puts s
          end
          if entity_complies?(id_dbs, childs, vars, true, verb)
            puts "alternative complies" if verb
            alt_complies = true
            break
          end
          puts "alternative does not comply" if verb
        }
        if alt_complies
          puts "partition complies" if verb
          next
        end
        puts "partition does not comply" if verb
        partitions_comply = false
        break
      }  
      unless partitions_comply
        puts "partitioning does not comply" if verb
        next
      end
      puts "partitioning complies" if verb
      partitioning_complies = true
      break
    }
    return partitioning_complies
  end
  
  # Checks if an entity fulfills the conditions expressed by +Entity+ objects.
  # Runs recursively through the +Entity+ object trees.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  # * _conditions_:: array of +Entity+ objects (trees)
  # * _vars_:: hash of query variables with their values
  # * _use_maps_:: recurse through +entity_complies_with_mappings?+
  # * _verb_:: print debug output
  #
  # === Returns
  # * +true+ if conditions fulfilled, +false+ if not
  #
  def entity_complies?(id_dbs, conditions, vars = {}, use_maps = false,
                       verb = false)

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
            return false
          end
        
        end
        
        # recurse over all key and/or value values
        found_value = false
        if key_var
          key_dbs.each { |k_dbs, val_dbs|
            next if vars.value?(k_dbs) # vars must differ
            vars[key_var] = k_dbs # diff var value per run
            val_dbs.each { |v_dbs|
              next if value_var && vars.value?(v_dbs) # vars must differ
              puts "checking #{k_dbs} : #{v_dbs}" if verb
              vars[value_var] = v_dbs if value_var # diff var value per run
              unless (use_maps &&
                      entity_complies_with_mappings?(v_dbs, child.children,
                                                     vars, verb)) ||
                     entity_complies?(v_dbs, child.children, vars, false, 
                                      verb)
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
            unless (use_maps &&
                    entity_complies_with_mappings?(v_dbs, child.children,
                                                   vars, verb)) ||
                   entity_complies?(v_dbs, child.children, vars, false,
                                    verb)
              puts "FALSE #{v_dbs} doesn't comply" if verb
              next
            end
            found_value = true
            break
          }
        end
        unless found_value
          puts "FALSE no possible key and/or value value" if verb
          return false
        end
        
      elsif child.key == Entity::ANY_VALUE || key_var
        # any attribute must have the value
        
        key_dbs = []

        if @use_idx

          # lookup in +@idx2+
          unless idx2_value = @idx2[id_dbs + DB_SEP + value_dbs]
            puts "FALSE no @idx2[#{id_dbs + DB_SEP + key_dbs}]" if verb
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
            return false
          end

        end
        
        if key_var
          # recurse over all key values
          found_value = false
          key_dbs.each { |k_dbs|
            next if vars.value?(k_dbs) # vars must differ
            vars[key_var] = k_dbs # diff var value per run
            unless (use_maps &&
                    entity_complies_with_mappings?(value_dbs, child.children,
                                                   vars, verb)) ||
                   entity_complies?(value_dbs, child.children, vars, false,
                                    verb)
              puts "FALSE #{value_dbs} doesn't comply" if verb
              next
            end
            found_value = true
            break
          }
          unless found_value
            puts "FALSE no possible key value" if verb
            return false
          end
        else
          # recurse
          next if value_dbs =~ @@ATTRIB_STR_VALUE_REGEX
          unless (use_maps &&
                  entity_complies_with_mappings?(value_dbs, child.children,
                                                 vars, verb)) ||
                 entity_complies?(value_dbs, child.children, vars, false, 
                                  verb)
            puts "FALSE #{value_dbs} doesn't comply" if verb
            return false
          end
        end
        
      elsif child.value == Entity::ANY_VALUE || value_var
        # the attribute must exist with any value
        
        # lookup in +@store+
        unless store_value = @store[id_dbs + DB_SEP + key_dbs]
          puts "FALSE no @store[#{id_dbs + DB_SEP + key_dbs}]" if verb
          return false
        end
        
        # recurse over all value values
        found_value = false
        store_value.split(DB_SEP).each { |v_dbs|
          next if value_var && vars.value?(v_dbs) # vars must differ
          vars[value_var] = v_dbs if value_var # diff var value per run
          unless (use_maps &&
                  entity_complies_with_mappings?(v_dbs, child.children,
                                                 vars, verb)) ||
                 entity_complies?(v_dbs, child.children, vars, false,
                                  verb)
            puts "FALSE #{v_dbs} doesn't comply" if verb
            next
          end
          found_value = true
          break
        }  
        unless found_value
          puts "FALSE no possible value value" if verb
          return false
        end
        
      else
        # the attribute must exist and must have the value
        
        # lookup in +@store+
        store_key = id_dbs + DB_SEP + key_dbs
        unless db_value_contains? @store, store_key, value_dbs
          puts "FALSE #{value_dbs} not in @store[#{store_key}]" if verb
          return false
        end
        
        # recurse
        next if value_dbs =~ @@ATTRIB_STR_VALUE_REGEX
        unless (use_maps &&
                entity_complies_with_mappings?(value_dbs, child.children,
                                               vars, verb)) ||
                entity_complies?(value_dbs, child.children, vars, false,
                                 verb)
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
  
  # Recursively generates all possible partitionings of subsets for the
  # elements in an array.
  #
  # === Parameters
  # * _array_
  #
  def get_partitionings(array)
    yield [] if array.empty?
    (0 ... 2 ** array.length / 2).each { |i|
      parts = [[], []]
      array.each { |item|
        parts[i & 1] << item
        i >>= 1
      }
      get_partitionings(parts[1]) { |b|
        result = [parts[0]] + b
        result = result.reject { |e| e.empty? }
        yield result
      }
    }
  end
end