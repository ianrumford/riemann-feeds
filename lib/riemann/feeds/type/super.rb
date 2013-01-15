# riemann feed super

# common methods for all feeder types

require 'potrubi/mixin/contract-recipes'

require_relative "../mixin/standard"

require 'riemann/client'

classContent = Class.new do

  ###include Potrubi::Mixin::Util
  include Riemann::Feeds::Mixin::Standard

  def self.attributes_specification_keys
    @attributes_specification_keys ||= [:include, :exclude, :definitions, :all]
  end

  def self.attributes_definition_keys
    @feed_attribute_definition_keys ||= [:map, :select, :metric, :pass_thru, :event_defaults]
  end

  def self.event_fields
    @event_fields ||= [:host, :service, :status, :time, :ttl,  :tags, :description, :metric]
  end

  def self.feed_configuration_keys
    @feed_configuration_keys ||= [:include_configuration, :event_defaults] # likely be subclassed
  end

  # Complex, simple and all attributes are hashes
  
  def self.complex_attributes
    @complex_attributes ||= {
      include_configuration: ->(k,v) {self.set_attributes_from_configuration_sources_list_or_croak(*v)},
    }
  end

  def self.simple_attributes
    @simple_attributes ||= Hash[*[:event_defaults, :riemann_host, :riemann_port, :riemann_interval].map {|a| [a, nil]}.flatten]
  end

  def self.all_attributes
    @all_attributes ||= complex_attributes.merge(simple_attributes)
  end
  
  attrAccessors = {
    riemann_host: :string,
    riemann_port: :fixnum,
    riemann_interval: :fixnum,
    event_defaults: {edit: {KEY_NAMES: 'self.class.event_fields'}, spec: :method_accessor_is_value_collection_with_keys},
    feed_configuration: {edit: {KEY_NAMES: 'self.class.all_attributes.keys'}, spec: :method_accessor_is_value_collection_with_keys},
  }

  Potrubi::Mixin::ContractRecipes.recipe_accessors(self, attrAccessors, :package_accessor_with_contract)

  mustbeContracts = {
    attributes_specification: {edit: {KEY_TYPE: :symbol, VALUE_TYPE: :any, KEY_NAMES: 'self.class.attributes_specification_keys'}, spec: :method_mustbe_is_value_typed_collection_with_keys},

    attributes_specification_all: {edit: {VALUE_TYPE: :string}, spec: :method_mustbe_is_value_typed_array},
    attributes_specification_include: {edit: {VALUE_TYPE: :string}, spec: :method_mustbe_is_value_typed_array},
    attributes_specification_exclude: {edit: {VALUE_TYPE: :string}, spec: :method_mustbe_is_value_typed_array},

    attributes_definitions: {edit: {KEY_TYPE: :string, VALUE_TYPE: :attributes_definition_map}, spec: :method_mustbe_is_value_typed_collection},
    attributes_definition_map: {edit: {KEY_NAMES: 'self.class.attributes_definition_keys'}, spec: :method_mustbe_is_value_collection_with_keys},
  }

  Potrubi::Mixin::ContractRecipes.recipe_mustbes(self, mustbeContracts)
  
  #STOPHEREAFTERSUPERCONTRACTS
  
  def initialize(initArgs=nil, &initBlok)
    eye = :'rfts i'

    $DEBUG && logger_me(eye, logger_fmt_kls_only(:initArgs => initArgs, :initBlok => initBlok))

    initArgs && import_configuration_or_croak(initArgs)

    #initArgs && mustbe_hash_or_croak(initArgs, eye).each {|k, v| __send__("#{k}=", v) }
    
    Kernel.block_given? && instance_eval(&initBlok)

  end

  def import_configuration_or_croak(feedConf=nil, &feedBlok)
    eye = :'rfts imp_cfg'

    $DEBUG && logger_me(eye, logger_fmt_kls(:feedConf => feedConf, :feedBlok => feedBlok))

    mustbe_feed_configuration_or_croak(feedConf, eye, "feedConf failed contract")
    
    set_attributes_using_specification_or_croak(self.class.all_attributes, feedConf)

    $DEBUG && logger_mx(eye, logger_fmt_kls_only(:feedConf => feedConf, :feedBlok => feedBlok))

    self
    
  end
  
  def to_event(*eventHashes)
    eventHash = potrubi_util_merge_hashes_or_croak(*eventHashes)
    mustbe_subset_or_croak(eventHash.keys, self.class.event_fields, eye, "unknown event keys / fields")
    eventHash
  end

  def merge_event_hashes(*eventHashes)

    mergeHash = eventHashes.inject({}) do | hashSumm, eventHash |

      case eventHash
      when NilClass then hashSumm # this is ok; convenient to allow nil
      when Hash

        ###print("MEREG EVENT HASHES hashSumm >#{hashSumm}< eventHash >#{eventHash}\n\n\n")
        
        r = hashSumm.merge(eventHash.select {|k,v| (k != :tags) })

        r[:tags] = merge_event_tags(hashSumm[:tags], eventHash[:tags])

        r

      else
        surprise_exception(eventHash, :m_evts_hshs, "eventHash not hash")
      end
      
    end

    $DEBUG && logger_ca(:m_evts_hshs, logger_fmt_kls(:mergeHash => mergeHash))
    
    mergeHash

    ###STOPMERGEHASHES
    
  end

  def merge_event_tags(*eventTags)
    mergeTags = eventTags.inject([]) do | tagsSumm, eventTags |

      case eventTags
      when NilClass then tagsSumm # this is ok; convenient to allow nil
      when Array then tagsSumm.concat(eventTags)
      else
        surprise_exception(eventTags, :m_evts_tags, "eventTags not array")
      end

    end

    $DEBUG && logger_ca(:m_evts_tags, logger_fmt_kls(:mergeTags => mergeTags))
    
    mergeTags
  end

    # These from Riemann Tools, modified

  def run(runArgs=nil, &runBlok)
    eye = 'rfms r'

    $DEBUG && logger_me(eye, logger_fmt_kls(:runArgs => runArgs, :runBlok => runBlok))
    
    ###mustbe_hash_or_nil_or_croak(runArgs, eye, "runArgs not hash")

    tickInterval = mustbe_fixnum_key_or_nil_or_croak(runArgs, :interval, eye, "run interval not fixnum") || find_riemann_interval_or_croak
    
    t0 = Time.now
    
    loop do
      
      begin
        riemann_tick
      rescue => e
        $stderr.puts "#{e.class} #{e}\n#{e.backtrace.join "\n"}"
        break
      end

      sleep(tickInterval - ((Time.now - t0) % tickInterval))  # Sleep. 
      
    end

    $DEBUG && logger_mx(eye, logger_fmt_kls(:runArgs => runArgs))

    self
    
  end

  def riemann_client
    @riemann_client ||= Riemann::Client.new(
                                            :host => find_riemann_host_or_croak,
                                            :port => find_riemann_port_or_croak,
                                            )
  end
  
  def riemann_tick

    r = riemann_client

    if (! $DEBUG)
      each_event {|e| e && (r << e) }  # send to riemann
    else
      
      #=begin
      t = Time.now.localtime
      #to_events.each_with_index do |e,i|
      to_enum(:each_event).each_with_index do |e, i|

        #i += 1
        
        puts("TICK #{t} NDX >#{i}< EVT e >#{e}<");

        # COMMNETED FOR TESTINGe && (r << e)
        
      end  # send to riemann #TESTING
      #=end
    end
  end

  # This method is passed the specification and returns
  # the consoldidated definitions collection
  
  def make_attributes_definitions_or_croak(attrArgs, &attrBlok)
    eye = :'m_attrs_defs'

    # Work with attribute as strings
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:attrArgs => attrArgs, :attrBlok => attrBlok))

    mustbe_attributes_specification_or_croak(attrArgs, eye, "attrArgs not attributes_specification")
    
    #STOPATTRARGSINSUPER
    
    #attrAll = mustbe_not_empty_or_croak(mustbe_array_key_or_nil_or_croak(attrArgs, :all, eye, "all attributes not array"), eye, "all attributes is empty").map(&:to_s)
    attrAll = mustbe_not_empty_or_croak(mustbe_attributes_specification_all_key_or_croak(attrArgs, :all, eye), eye, "all attributes is empty").map(&:to_s)
    

    #puts("\n\n\nATTR ALL >#{attrAll}<")

    #STOPMAKEATTRSPECSENTRY

    attrInc = mustbe_attributes_specification_include_key_or_nil_or_croak(attrArgs, :include, eye) # mustbe all strings
    #puts("ATTR INC >#{attrInc.class}< >#{attrInc}< >#{is_value_not_empty?(attrInc)}<")
    attrInc && mustbe_not_empty_or_croak(attrInc, eye, "include attributes is empty")

    attrExc = mustbe_attributes_specification_exclude_key_or_nil_or_croak(attrArgs, :exclude, eye)  || []
    
    attrMapNom = mustbe_attributes_definitions_key_or_nil_or_croak(attrArgs, :definitions, eye) || {}
    attrMap = attrMapNom && potrubi_util_map_hash_kv(attrMapNom) {|k,v| [k.to_s, v]} # keys all strings

    # Ensure all consistent
    
    attrInc && mustbe_subset_or_croak(attrInc, attrAll, eye, "include attributes contains unknown attributes")
    mustbe_subset_or_croak(attrExc, attrAll, eye, "exclude attributes contains unknown attributes")
    mustbe_subset_or_croak(attrMap.keys, attrAll, eye, "attribute map contains unknown attributes")
    
    attrUse = ((attrInc || attrAll) - attrExc).uniq  # list of unique attributes to report on

    # consolidate "faked up" attr specs with ones provided to get the composite attrSpecs
    
    attrDefsNom = potrubi_util_array_to_hash(attrUse).merge(attrMap.select {|k,v| attrUse.include?(k)}) # consolidated "faked up" attr specs with ones provided

    attrDefs = potrubi_util_map_hash_v(attrDefsNom) do | attrName, attrSpecNom|

      attrSpec =
        case attrSpecNom
        when NilClass then {}
        when Hash then
          attrSpecNom.each_with_object({}) do | (verbName, verbSpec), h1 |
          case verbName
          when :pass_thru then h1[:pass_thru] = verbSpec  # dont touch; just pass through
          when :event_defaults then # add these to pass_thru
            h1[:pass_thru] = (h1[:pass_thru] || {}).merge(verbName => verbSpec)
          when :map, :select, :metric then
            h1[verbName] = {
              :method_name => "#{verbName}_#{attrName}_#{rand(1000000)}", # make a unqiue name
              :method_spec => verbSpec   # spec must be valid to dynamic_define_methods
            }
          else
            logic_exception(verbName, eye, "attrName >#{attrName}<  verbName >#{verbName}< value should be impossible")
          end
        end
          
        else
          logic_exception(attrrSpecNom, eye, "attrSpecNom value should be impossible")
        end

      attrSpec
      
    end
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(:attrDefs => attrDefs))

    mustbe_attributes_definitions_or_croak(attrDefs, eye, "attrDefs failed contract")

    #STOPMAKEATTRSPECS
    
  end

  def collect_attributes_method_definitions_or_croak(attrSpecs, &attrBlok)
    eye = :'rfts col_attr_mtd_defs'

    $DEBUG && logger_me(eye, logger_fmt_kls(:attrSpecs => attrSpecs, :attrBlok => attrBlok))

    #STOPCOLLECTATTRSMTDS
    
    attrMths = attrSpecs.each_with_object({}) do | (attrName, attrSpec), h |

      $DEBUG && logger_beg(eye, 'ATTR MTH', logger_fmt_kls(:attrName => attrName, :attrSpec => attrSpec))

      attrSpec.select {|k,v| v.has_key?(:method_name) }.each {|k, v| h[v[:method_name]] = v[:method_spec] }
      
      $DEBUG && logger_fin(eye, 'ATTR MTH', logger_fmt_kls(:attrName => attrName, :attrSpec => attrSpec))

    end
    
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(attrMths: attrMths), logger_fmt_kls(attrSpec: attrSpecs))

    mustbe_hash_or_croak(attrMths, eye, "attrMths not hash")

    #STOPEHERECOLATTRMTDS

  end
  
end

klassConstant = Potrubi::Core.assign_class_constant_or_croak(classContent, :Riemann, :Feeds, :Type, :Super)

__END__

