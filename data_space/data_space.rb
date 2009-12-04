#!/usr/bin/env ruby -w

require "rubygems"
require "bdb"
require "fileutils"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "root_entity"
require "entity"

# This class implements the data space in a flat way such that the entities
# and all their attributes are stored as key/value pairs in the database.
#
# A entity is saved in this way:
# * entity_id -> nil
# * entity_id///attrib1_key -> attrib1_value1
# * entity_id///attrib2_key -> attrib2_value1///attrib2_value2///...
# * ...
#
# If the +use_idx+ parameter is set to true, the data space uses an inverted
# indexes with the following structure:
# * attrib_value///attrib_key -> entity_id1///entity_id2///...
# * ...
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
  # * _use_idx_:: use inverted index
  # * _use_add_idx_:: use inverted index and additional indexes
  #
  def initialize(bdb_path, use_idx=true, use_add_idx=true)
    
    @use_idx = @use_add_idx = use_add_idx
    @use_idx ||= use_idx
    
    FileUtils::mkdir bdb_path unless File.exists? bdb_path
    
    @store = open_db(bdb_path, "store")
    
    return unless @use_idx
    
    @idx = open_db(bdb_path, "index")
    
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
    
    return unless @use_idx
    
    close_db @idx
    
    return unless @use_add_idx
    
    [@k_idx, @v_idx, @id_idx].each { |db| close_db db }
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
    # lookup solution:
    # val -> key1///key2///key2
    # FE: key in @k_idx
    # but val -> key index might be large (only use if needed in other places)
    #
    if @use_idx
      # loop over +@idx+
      idx_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
      db_each(@idx) { |idx_key, idx_value|
        next unless idx_key =~ idx_key_regex
        key_dbs = $'
        idx_value.split(DB_SEP).each { |idx_id_dbs|
          db_remove_from_value @store, idx_id_dbs + DB_SEP + key_dbs, id_dbs
          remove_attribute_from_indexes idx_id_dbs, key_dbs, id_dbs
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
    
    return unless @use_idx
    
    truncate_db @idx
    
    return unless @use_add_idx
    
    [@k_idx, @v_idx, @id_idx].each { |db| truncate_db db }
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
    #
    # (alternative: lookup in +@idx+ if +@store+ removed)
    # if db_value_contains?(@idx, value_dbs + DB_SEP + key_dbs, id_dbs)
    #
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

  # Removes an existing attribute from an entity.
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
    
    id_dbs = s_to_dbs(id)
    
    if key == Entity::ANY_VALUE
      remove_entity_attributes id_dbs
      return
    end
      
    key_dbs, value_dbs = s_to_dbs(key), s_to_dbs(value)
    store_key = id_dbs + DB_SEP + key_dbs
    
    store_value = @store[store_key]
    unless store_value &&
           (value == Entity::ANY_VALUE || store_value == value_dbs)
      raise NoAttributeError.new id, key, value
    end
    
    if value == Entity::ANY_VALUE
      @store[store_key].split(DB_SEP).each { |value_dbs|
        remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
      }
      db_del @store, store_key
    else
      db_remove_from_value(@store, store_key, value_dbs)
      remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
    end
  end

  # Performs a search specified by a +Entity+ object tree. Returns an array of
  # matching entity ids as result.
  #
  # === Parameters
  # * _query_:: +RootEntity+ object
  #
  # === Returns
  # * Array of entity ids.
  #
  def search(query)
    
    results = []
    
    # collect entities
    if query.value == Entity::ANY_VALUE
      # loop over +@store+
      db_each(@store) { |store_key, store_value|
        results << store_key unless store_key =~ @@DB_SEP_REGEX
      }
    else
      # lookup at +@store+
      id_dbs = s_to_dbs(query.value)
      results << id_dbs if @store[id_dbs]
    end
    
    # check entities
    results.delete_if { |id_dbs| !entity_complies?(id_dbs, query.children) }
    
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
    
    return unless @use_idx
    
    dump_db "index", @idx
    
    return unless @use_add_idx
    
    {
      "key index" => @k_idx,
      "value index" => @v_idx,
      "id index" => @id_idx
    }.each { |name, db| dump_db name, db }
  end
  
  class OpenDatabaseError < StandardError
    
    def initialize(db)
      super "Error opening database file '#{db}'."
    end
    
    attr_reader :id
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
    
    attr_reader :id, :key
  end

  private
    
  @@DB_SEP_ESC = Regexp.escape DB_SEP
  @@DB_SEP_REGEX = /#{@@DB_SEP_ESC}/
  @@DB_INVALID_REGEX = /#{Regexp.escape(DB_INVALID)}/
  @@ATTRIB_STR_VALUE_REGEX = /^".*"$/
  
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
    # @store.delete key
    db.del(nil, key_dbs, 0)
  end
  
  # Loops over a database. Call with block. Return +false+ from the block to
  # break out of the loop.
  #
  # === Parameters
  # * _db_:: database
  #
  def db_each(db)
    # @store.each { |k, v| yield k, v }
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
    /
      (^ | #{@@DB_SEP_ESC})
      #{Regexp.escape(dbs)}
      (#{@@DB_SEP_ESC} | $)
    /x
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
      db[key_dbs] = value_dbs =~ /#{dbs_esc}$/ ?
        value_dbs.chomp(DB_SEP + dbs) : value_dbs.sub(dbs + DB_SEP, "")
    end
    true
  end
  
  # Prepares entity id or attribute key for usage as a database key by
  # replacing occurences of +DB_SEP+.
  #
  # === Parameters
  # * _s_:: entity identifier, attribute name or attribute value
  #
  # === Throws
  # * _ArgumentError_:: if _str_ contains an occurance of DB_INVALID
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
  # * _s_dbs_:: entity identifier, attribute name or attribute value 
  #          :: modified by +s_to_dbs+
  #
  def dbs_to_s(s_dbs)
    s_dbs.gsub @@DB_INVALID_REGEX, DB_SEP
  end

  # Removes all attributes of an entity.
  #
  # === Parameters
  # * _id_dbs_:: entity identifier
  #
  def remove_entity_attributes(id_dbs)
    if @use_add_idx
      # lookup in +@id_idx+
      id_idx_value = @id_idx[id_dbs]
      return unless id_idx_value
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
      db_each(@store) { |store_key, store_value|
        next unless store_key =~ store_key_regex
        key_dbs = $'
        db_del @store, store_key
        store_value.split(DB_SEP).each { |value_dbs|
          remove_attribute_from_indexes id_dbs, key_dbs, value_dbs
        }
      }
    end
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
    
    db_add_to_value @idx, value_dbs + DB_SEP + key_dbs, id_dbs

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
    
    db_remove_from_value @idx, value_dbs + DB_SEP + key_dbs, id_dbs

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
  #
  # === Returns
  # * +true+ if conditions fulfilled, +false+ if not
  #
  def entity_complies?(id_dbs, conditions)

    return true if conditions.empty?

    conditions.each { |child|

      if child.key == Entity::ANY_VALUE && child.value == Entity::ANY_VALUE
        # this is not valid -- we silently stop traversing here
        # TODO check again, if valid we have to traverse
        next
      end  
        
      value_dbs = s_to_dbs(child.value)
      
      if child.key == Entity::ANY_VALUE
        # any attribute must have the value

        if @use_add_idx

          # lookup at +@v_idx+
          return false unless db_value_contains?(@v_idx, value_dbs, id_dbs)

        elsif @use_idx

          # loop over +@idx+ (better than loop over +@store+ as
          # +@idx.size+ < +@store.size+)
          idx_key_regex = /^#{Regexp.escape(value_dbs) + @@DB_SEP_ESC}/
          idx_value_regex = build_value_grep_regex(id_dbs)
          matched = false
          db_each(@idx) { |idx_key, idx_value|
            next if idx_key !~ idx_key_regex || idx_value !~ idx_value_regex
            matched = true
            false
          }
          return false unless matched

        else

          # loop over +@store+
          store_key_regex = /^#{Regexp.escape(id_dbs) + @@DB_SEP_ESC}/
          matched = false
          db_each(@store) { |store_key, store_value|
            next if store_key !~ store_key_regex || store_value != value_dbs
            matched = true
            false
          }
          return false unless matched

        end
        
      elsif child.value == Entity::ANY_VALUE
        # the attribute has to exist with any value
        
        # lookup at +@store+
        value_dbs = @store[id_dbs + DB_SEP + s_to_dbs(child.key)]
        return false unless value_dbs
        
      else
        # the attribute has to exist and must have the value
        
        # lookup at +@store+
        store_key = id_dbs + DB_SEP + s_to_dbs(child.key)
        return false unless db_value_contains? @store, store_key, value_dbs
      end  
        
      next if value_dbs =~ @@ATTRIB_STR_VALUE_REGEX

      return false unless entity_complies?(value_dbs, child.children)
    }      

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
          child_key = dbs_to_s(key_dbs);
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