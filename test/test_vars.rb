#!/usr/bin/env ruby -w

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "../data_space")
require "data_space"
require "root_entity"
require "entity"

class TestVars
  
  ID1 = '94MU# 8+z*9/797r:7p"iuaPp' + DataSpace::DB_SEP + 
        'tW ZwVA omyN=]9Kz+9TD&miYYcx' + DataSpace::DB_SEP
  ID2 = DataSpace::DB_SEP +
        'k%9Vhj7NE[9j s}H/294<:n PAf4vr ueFX(CL=hjAE6PK( Rs{27U'
  ID3 = '@ey4 {7WxTT(4Vs X7P2K&RUwH/Xi(i7GUHQ23Xi8v$Y$=AR#9xX' +
        DataSpace::DB_SEP
  KEY1 = ' ELXh7B}27e4JQP kv9t3kVy7cK^z+<W9[].P,s97PRs ' +
         DataSpace::DB_SEP + ':>womDY p'
  KEY2 = 'AaYAd xNM%$j94yFo w]H hmN46g' + DataSpace::DB_SEP +
         '&f3[9#4c Be3@7ch vV6dCs@A. '
  KEY3 = 'qk#9282<nQ8uJ34,Ycnoj9N,bsTg}v>wWVqmx.%ZtNyr)4v=6B'
  STR1 = '"3oEk b*7G{9kEAD 3e*kh7iq [ Dxm 9Dqq Ld8cH:P7 ]=?wH28N"'  
  STR2 = '"}p{63oP#*{E997pnE2NUoY  QHdD2ahY&2N rRXm$<N=Ns64U}AGt"'
  VAR1 = '$Cr%B){hNJ9Z2 o2KR8x^KsN2R 8krN9j:U' + DataSpace::DB_SEP + 
         'EzKq=N 9@PVw]7D{[4a'
  VAR2 = '$*cD$/]xgBa{b 3gxW9434y"' + DataSpace::DB_SEP +
         'CY?2w#9b4F+ ZHBZVUpE;HD4  2azdF'
  VAR3 = '$KpB(=8*H2JZ2LD4 PnNa.Y"2P+t8Yn' + DataSpace::DB_SEP +
         '3[eW/7  3EKVeBEVTo&)9ku'
  
  # ID1 = "this is " + DataSpace::DB_SEP + " id1"
  # ID2 = "this is " + DataSpace::DB_SEP + " id2"
  # ID3 = "this is " + DataSpace::DB_SEP + " id3"
  # KEY1 = "this is " + DataSpace::DB_SEP + " key1"
  # KEY2 = "this is " + DataSpace::DB_SEP + " key2"
  # KEY3 = "this is " + DataSpace::DB_SEP + " key3"
  # STR1 = '"this is ' + DataSpace::DB_SEP + ' str1"'
  # STR2 = '"this is ' + DataSpace::DB_SEP + ' str2"'
  # VAR1 = "$this is " + DataSpace::DB_SEP + " var1"
  # VAR2 = "$this is " + DataSpace::DB_SEP + " var2"
  # VAR3 = "$this is " + DataSpace::DB_SEP + " var3"
  
  # ID1 = "id1"
  # ID2 = "id2"
  # ID3 = "id3"
  # KEY1 = "key1"
  # KEY2 = "key2"
  # KEY3 = "key3"
  # STR1 = '"str1"'
  # STR2 = '"str2"'
  # VAR1 = "$var1"
  # VAR2 = "$var2"
  # VAR3 = "$var3"
  
  INVAL = 'y7cK^z +' + DataSpace::DB_INVALID + '7B }27'          
  ENTITY = RootEntity.new(ID1)
  ENTITY_ANY = RootEntity.new(Entity::ANY_VALUE)
  ENTITY_STR_ATTRIB = RootEntity.new ID1, [ Entity.new(KEY1, STR1) ]
  ENTITY_ID_ATTRIB = RootEntity.new ID1, [ Entity.new(KEY1, ID2) ]
  
end