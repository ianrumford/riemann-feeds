# riemann feed jmx

# Java Java Management Extensions (JMX) feed type

###require "potrubi/mixin/util"
require "potrubi/klass/syntax/braket"

# simple way to disable JMX access


JMX_LIVE_MODE = true
#JMX_LIVE_MODE = false

JMX_LIVE_MODE && (require "jmx4r") # does all the jmx heavy lifting

require_relative "super"
superClass = Riemann::Feeds::Type::Super

###puts "RIEMAN FEED TPOE JMX SUPER >#{superClass.class}< >#{superClass}<"

#stopHEREINJMX

classContent = Class.new(superClass) do

  ###include Potrubi::Mixin::Util

  def self.feed_configuration_keys
    @feed_configuration_keys ||= super.concat([:beans])
  end

  def self.bean_specification_keys
    @bean_spec_keys ||= [:event_defaults, :attributes, :bean_name]
  end
  
  def self.complex_attributes
    #@complex_attributes ||= super.merge(beans: ->(k,v) {puts("\n\n\nBEAN IMPORT k >#{k}< v >#{v}<"); import_config_beans_or_croak(read_maybe_configuration_sources_or_croak({k => v}).values.first)})
    @complex_attributes ||= super.merge(beans: ->(k,v) {import_config_beans_or_croak(read_maybe_configuration_sources_or_croak({k => v}).values.first)})
  end

  def self.simple_attributes
    @simple_attributes ||= super.merge(Hash[*[:jmx_user, :jmx_pass, :jmx_host, :jmx_port, :jmx_connection].map {|a| [a, nil]}.flatten])
  end
  
  # create the accessors with associated contracts
  
  attrAccessors = {
    jmx_user: :string,
    jmx_pass: :string,
    jmx_host: :string,
    ###:jmx_port => :string,
    jmx_port: :fixnum,
    #:jmx_connection => nil,
    jmx_connection: 'JMX::MBeanServerConnectionProxy',

    #beans_attributes: :hash,
    beans_attributes: {edit: {KEY_TYPE: :string, VALUE_TYPE: :attributes_definitions}, spec: :method_accessor_is_value_typed_collection},

    #beans_specifications: :hash,
    beans_specifications: {edit: {KEY_TYPE: :string, VALUE_TYPE: :bean_specification, VALUE_IS_NIL_RESULT: 'true'}, spec: :method_accessor_is_value_typed_collection},

    
  }

  Potrubi::Mixin::ContractRecipes.recipe_accessors(self, attrAccessors, :package_accessor_with_contract)

  #STOPHEREJMXATTS
  
  mustbeContracts = {
    beans_container: :array,  # just the container
    bean_name: :string,
    #bean_spec: :hash,
    bean_specification: {edit: {KEY_NAMES: 'self.class.bean_specification_keys'}, spec: :method_mustbe_is_value_collection_with_keys},
    ###bean_attributes: :hash,
    bean_attributes_with_metrics: :hash,
    jmx_bean: 'JMX::MBean',
    bean_attributes_specification: :hash,
    jmx_bean_attributes: {edit: {KEY_TYPE: :string, VALUE_TYPE: :string}, spec: :method_mustbe_is_value_typed_collection},
    jmx_bean_attributes_with_metrics: {edit: {KEY_TYPE: :string, VALUE_TYPE: :string}, spec: :method_mustbe_is_value_typed_collection},
  }

  Potrubi::Mixin::ContractRecipes.recipe_mustbes(self, mustbeContracts)

  #STOPHEREJMXMUSTBES

 # def self.simple_attributes
 #   @simple_attributes ||= super.concat([:event_defaults])
 # end

  def jmx_connection # override the one made by the recipe
    @jmx_connection ||=  open_jmx_connection_or_croak
  end
  
  def import_config_beans_or_croak(beanArgs, &beanBlok)
    eye = :'rftj imp_cfg_beans'

    $DEBUG && logger_me(eye, logger_fmt_kls(beanArgs: beanArgs, beanBlok: beanBlok))

    #STOPBEENARAGSB4VALIDATION

    mustbe_beans_container_or_croak(beanArgs, eye) # jsut an array right now

    #puts("\n\n\n\nBEAN ARGS >#{beanArgs.class}< >#{beanArgs}<")
    
    # resolved external configuration
    # afterwards, treat bare strings as bean name and gfake up a hash
    
    #beansHashes = read_maybe_configuration_sources_list_or_croak(*beanArgs).map {|h| puts("\n\n\n\n\n\nIMPCFGBEAN h >#{h.class}< >#{h}<"); h.is_a?(String) ? {bean_name: h} : h}
    beansHashes = read_maybe_configuration_sources_list_or_croak(*beanArgs).map {|h| h.is_a?(String) ? {bean_name: h} : h}
    mustbe_bean_specifications_or_croak(*beansHashes)  

    beansSpecs = self.beans_specifications = potrubi_util_array_to_hash(beansHashes) {|v| [mustbe_bean_name_key_or_croak(v, :bean_name), read_maybe_configuration_sources_or_croak(v)] }
    
    #STOPBEENARAGSAFVALIDATION
    
    beanObjs = []

    #beanSpecDefault = {}
    ##beanSpecDefault = nil
    
    self.beans_attributes = beansAttrs = make_beans_attributes_definitions_or_croak(beansSpecs, &beanBlok)  # preserve full atributes map

    beansMths = make_beans_attributes_methods_or_croak(beansAttrs)
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(beansMths: beansMths))

    beansMths
    
  end
  
  def make_beans_attributes_definitions_or_croak(beansSpecs, &beanBlok)
    eye = :'m_beans_attrs_defs'
    eyeTale = 'BEAN ATTRS DEFS'
    
    $DEBUG && logger_me(eye, eyeTale, logger_fmt_kls(beansSpecs: beansSpecs, beanBlok: beanBlok))

    mustbe_beans_specifications_or_croak(beansSpecs, eye, "beansSpecs not hash")

    #STOPHEREBEANSATTRSSPECS

    beanSpecDefault = nil  # what to do / use this for?

    beansAttrs = potrubi_util_map_hash_v(beansSpecs) do | beanName, beanSpecNom |

      $DEBUG && logger_beg(eye, eyeTale, logger_fmt_kls(beanName: beanName, :beanSpecNom => beanSpecNom))
      
      beanSpec = potrubi_util_merge_hashes_or_croak(beanSpecDefault, beanSpecNom) # any defaults?

      ###attrSpec = make_bean_attrs_specs_or_croak(:bean_name => beanName, :bean_spec => beanSpec)

      attrDefsNom = mustbe_hash_key_or_nil_or_croak(beanSpec, :attributes, eye, "attribute not hash")

      $DEBUG && logger_ms(eye, eyeTale, 'ATTRS NOM', logger_fmt_kls(beanName: beanName, :attrDefsNom => attrDefsNom))

      ###STOPHEREATARRTDEFSNOM
      
      JMX_LIVE_MODE && (attrAll = find_jmx_bean_attributes_with_metrics_or_croak(beanName).keys)
      JMX_LIVE_MODE || (attrAll = mustbe_hash_key_or_nil_or_croak(attrDefsNom, :definitions, eye, "TESTING attributes map not hash").keys)

      attrDefsNrm = potrubi_util_merge_hashes_or_croak({:all => attrAll}, attrDefsNom)

      $DEBUG && logger_ms(eye, eyeTale, 'ATTRS NRM', logger_fmt_kls(beanName: beanName, :attrDefsNrm => attrDefsNrm))
      
      attrsDefs  = make_attributes_definitions_or_croak(attrDefsNrm)

      $DEBUG && logger_fin(eye, eyeTale, logger_fmt_kls(beanName: beanName, :attrsDefs => attrsDefs))

      attrsDefs
    end
    
    $DEBUG && logger_mx(eye, eyeTale, logger_fmt_kls(beansSpecs: beansSpecs, beansAttrs: beansAttrs))

    mustbe_hash_or_croak(beansAttrs, eye, "beansAttrs not hash")

    #STOPMAKEBEENASATTARSDEFS
    
  end

  def make_beans_attributes_methods_or_croak(beansAttrs, &beanBlok)
    eye = :'rftj m_beans_attrs_mths'

    $DEBUG && logger_me(eye, logger_fmt_kls(:beansAttrs => beansAttrs, beanBlok: beanBlok))

    #STOPHEREBEASNATTRSMTHS
    
    mustbe_beans_attributes_or_croak(beansAttrs, eye, "beansAttrs failed contract")

    #STOPMAKEBEASNATTRSMTHS
    
    beansMths = make_syntax_beans_methods_or_croak(beansAttrs)
    
    $DEBUG && logger_ms(eye, 'BEANS MTHS', logger_fmt_kls(beansMths: beansMths))

    ##STOPHEREBEANSMTHS

    beansDyns = beansMths.values.inject({}) {|s,h| s.merge(h) }  # flatten values

    $DEBUG && logger_ms(eye, 'BEANS DYNS', logger_fmt_kls(beansDyns: beansDyns))
    
    # get the singleton class

    singletonKls = class << self; self; end

    Potrubi::Core.dynamic_define_methods(singletonKls, beansDyns) # make beans methods in singleton class
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(beansDyns: beansDyns))

    beansDyns
    
  end
  
  # Method Syntax
  # #############

  def make_syntax_beans_methods_or_croak(beansAttrs, &beanBlok)
    eye = :'rftj ms_beans_mths'

    $DEBUG && logger_me(eye, logger_fmt_kls(beansAttrs: beansAttrs, beanBlok: beanBlok))

    mustbe_hash_or_croak(beansAttrs, eye, "beansAttrs not hash")

    beansMths = potrubi_util_map_hash_v(beansAttrs) { | beanName, beanAttrs | collect_attributes_method_definitions_or_croak(beanAttrs) }
    
    #mtdSpec = {:bean_name => beanName, :bean_spec => beanAttrs}

    beansMths[:each_event] = {each_event: make_syntax_each_event_method_or_croak(beansAttrs).to_s}
    
    $DEBUG && logger_ms(eye, logger_fmt_kls(beansAttrs: beansAttrs), logger_fmt_kls_only(beansMths: beansMths))

    mustbe_hash_or_croak(beansMths, eye, "beansMthds not hash")

  end
  

  def make_syntax_each_event_method_or_croak(beansAttrs, &mthBlok)
    eye = :'rftj ms_each_evt_mtd'
    eyeTale = 'EACH_EVENT'

    $DEBUG && logger_me(eye, eyeTale, logger_fmt_kls(beansAttrs: beansAttrs, :mthBlok => mthBlok))

    mustbe_beans_attributes_or_croak(beansAttrs, eye, "beansAttrs not a beans_attributes")
    
    braketKls = Potrubi::Klass::Syntax::Braket # class to simplify method text construction

    braketMethod = braketKls.new_method  # will hold to_events method's syntax
    
    braketMethod.cons_head('def each_event',
                           ####JMX_LIVE_MODE && 'jmxConn = find_jmx_connection_or_croak',
                           ###JMX_LIVE_MODE && 'jmxConn = find_jmx_connection_or_croak',
                           ###'beansAttrs = beans_attributes',
                           ###'evntIter = Enumerator.new do | enum |',  # make a lazy enumerator
                           )
    
    braketMethod.push_tail(###'end',
                           ####JMX_LIVE_MODE && 'close_jmx_connection_or_croak(jmxConn)',
                           ###'$DEBUG && logger_ca(:to_evnts, "evntIter >#{evntIter.class}<")',
                           ###'evntIter',
                           'self',
                           'end')
    hostDefaults = {host: jmx_host} #  need to specify source of the events
    eventDefaults = event_defaults
    beansSpecifications = find_beans_specifications_or_croak
    
    braketsBeans = beansAttrs.each_with_object([]) do | (beanName, attrsSpecs), a |

      $DEBUG && logger_beg(eye, eyeTale, 'BEAN', logger_fmt_kls(beanName: beanName, :attrsSpecs => attrsSpecs))

      beaneventDefaults = mustbe_event_defaults_key_or_nil_or_croak(beansSpecifications[beanName], :event_defaults, eye, "beanName >#{beanName}< event defaults not hash")

      mustbe_bean_attributes_specification_or_croak(attrsSpecs, eye, "attrsSpecs not hash")

      braketBean = braketKls.new_stanza  # will hold syntax for this bean

      braketBean.cons(JMX_LIVE_MODE && "beanInst = find_jmx_bean_or_croak('#{beanName}')")
      
      attrsSpecs.each do | attrName, attrSpec |

        $DEBUG && logger_beg(eye, eyeTale, 'BEAN ATTR', logger_fmt_kls(beanName: beanName, :attrName => attrName, :attrSpec => attrSpec))

        attrPassThru = mustbe_hash_key_or_nil_or_croak(attrSpec, :pass_thru, eye, "pass thru not hash")

        attreventDefaults = mustbe_event_defaults_key_or_nil_or_croak(attrPassThru, :event_defaults, eye, "beanName >#{beanName}< attrName >#{attrName}< event defaults not hash")

        $DEBUG && logger_ms(eye, eyeTale, 'BEAN ATTR PASS & DEFS', logger_fmt_kls(:passThru => attrPassThru, :attrDefaulst => attreventDefaults))
  
        defaultFields = merge_event_hashes(hostDefaults, eventDefaults, beaneventDefaults, attreventDefaults)

        #  Build the event hash with the wanted attribute (event) fields
        
        attrDefs = {metric: "beanInst.#{attrName}", description: "'#{attrName}'"}
        JMX_LIVE_MODE || (attrDefs = {:metric => "2.97", :description => "'#{attrName}'"}) # TESING

        # Create the syntax for the attributes in attrDefs, with normalisation calls if any
        
        attrFields = attrDefs.each_with_object({}) do | (fieldName, fieldDef), h |
          fieldBraket = braketKls.new_statement.push(fieldDef) # add the field def 
          (fieldSpec = attrSpec[fieldName]) && fieldBraket.cons_head(fieldSpec[:method_name], '(').push_tail(')')                                      
          h[fieldName] = fieldBraket.to_s
        end
        
        eventFields = merge_event_hashes(defaultFields, attrSpec[:event_defaults], attrFields)

        # Convert the event to text representation - needs custom logic
        
        braketEvent = eventFields.inject(braketKls.new_statement) do | bkt, (fieldName, fieldValue) |
          r = case fieldName
              when :metric, :description then fieldValue
              when :tags then fieldValue  # array to.s is fine
              else
                "'#{fieldValue}'"  # enclose in single quotes
              end
          bkt.push(fieldName, ': ', r.to_s, ', ') # short form symbol key syntax 
        end

        braketEvent.pop  # get rid of last comma
        
        braketEvent.cons_head('{').push_tail('}') # hash syntax

        ###braketEvent.raw_debug("BRAKET FIELDS")
        ###puts("BRAKETFIELDS >#{braketEvent}<")
        
        ###STOPHEREAFTERATTRFIELDS

        # Is the event hash to be transformed (mapped)?

        braketTransform = case (transformSpec = attrSpec[:map])
                          when NilClass then braketEvent
                          else
                            #bkt = braketKls.new_statement
                            #bkt.cons_head(transformSpec[:method_name], '(').push_tail(')')
                            #bkt.push(braketEvent)
                            #bkt
                            braketKls.new_statement.cons_head(transformSpec[:method_name], '(').push_tail(')').push(braketEvent)
                          end

        # Construct the yield

        ###braketYield = braketKls.new_statement.cons_head('enum.yield(').push_tail(')')
        braketYield = braketKls.new_statement.cons_head('yield(').push_tail(')')

        # Is event to be filtered (selected)?
        
        braketAttr = case (filterSpec = attrSpec[:select])
                     when NilClass then braketYield.push(braketTransform) # no filter
                     else
                       bkt = braketKls.new_statement
                       bkt.push('(r = ', braketTransform, ')')
                       bkt.push(' && ', filterSpec[:method_name], '(r)')
                       bkt.push(' && ', braketYield.push('r'))
                       bkt
                     end

        braketBean.push(braketAttr)
        
        $DEBUG && logger_fin(eye, eyeTale, 'BEAN ATTR', logger_fmt_kls(:attrName => attrName, :attrSpec => attrSpec))

      end

      a << braketBean

      $DEBUG && logger_fin(eye, eyeTale, 'BEAN', logger_fmt_kls(beanName: beanName, :attrsSpecs => attrsSpecs))

    end

    braketMethod.push(braketsBeans)
    
    ###braketMethod.raw_debug('TO_EVENT METHOD')
    $DEBUG && puts("EACH_EVENT METHOD \n>#{braketMethod.to_s}<")
    
    ###STOPHERETOEVENTS

    $DEBUG && logger_me(eye, eyeTale, logger_fmt_kls_only(beansAttrs: beansAttrs, :braketMethod => braketMethod))

    braketMethod


  end

  


  # Bean Finders
  # ############
  
  def find_jmx_bean_attributes_or_croak(beanName, &beanBlok)
    eye = :'rftj f_bean_attrs'
    beanAttrs = find_jmx_bean_or_croak(beanName).attributes
    $DEBUG && logger_ca(eye, logger_fmt_kls(beanName: beanName, beanAttrs: beanAttrs))
    mustbe_jmx_bean_attributes_or_croak(beanAttrs, eye, "beanAttrs failed contract")
  end

  def find_jmx_bean_attributes_with_metrics_or_croak(beanName, &beanBlok)
    eye = :'rftj f_bean_attrs_w_metrics'
    metricsRegex = Regexp.new('\Atag\.')
    beanAttrsWithMetrics = find_jmx_bean_attributes_or_croak(beanName).select {|k,v| ! k.match(metricsRegex) }
    $DEBUG && logger_ca(eye, logger_fmt_kls(beanName: beanName, beanAttrsWithMetrics: beanAttrsWithMetrics))
    mustbe_jmx_bean_attributes_with_metrics_or_croak(beanAttrsWithMetrics, eye, "beanAttrsWithMetrics failed contract")
  end

  def find_jmx_bean_or_croak(beanName, &beanBlok)
    eye = :'rftj f_bean'
    $DEBUG && logger_me(eye, logger_fmt_kls(beanName: beanName, beanBlok: beanBlok))
    beanConn = find_jmx_connection_or_croak
    beanInst = JMX::MBean.find_by_name(beanName, :connection => beanConn)
    $DEBUG && logger_mx(eye, logger_fmt_kls(beanName: beanName, :beinInst => beanInst, :beanConn => beanConn))
    mustbe_jmx_bean_or_croak(beanInst, eye, 'jmx bean find failed')
  end

  # JMX Connections
  # ###############

  
  def open_jmx_connection_or_croak(jmxArgs={}, &jmxBlok)
    eye = :'rftj o_jmx_conn'

    jmxHost = mustbe_jmx_host_key_or_nil_or_croak(jmxArgs, :jmx_host, eye) || find_jmx_host_or_croak
    jmxPort = mustbe_jmx_port_key_or_nil_or_croak(jmxArgs, :jmx_port, eye) || find_jmx_port_or_croak
    jmxUser = mustbe_jmx_user_key_or_nil_or_croak(jmxArgs, :jmx_user, eye) || jmx_user
    jmxPass = mustbe_jmx_pass_key_or_nil_or_croak(jmxArgs, :jmx_pass, eye) || jmx_pass

    jmxConn = begin

                 case
                 when jmxUser.nil? then JMX::MBean.establish_connection(:host => jmxHost, :port => jmxPort)
                 else
                   JMX::MBean.establish_connection(:host => jmxHost, :port => jmxPort, :username => jmxUser, :password => jmxPass)
                 end
                 
               rescue Exception => e
                 errorText = "Connection open error: jmxArgs >#{jmxArgs}< jmxHost >#{jmxHost}< jmxPort >#{jmxPort}< jmxUser >#{jmxUser}< error >#{e.message}"
                 #riemann_alert
                 connection_exception(e, errorText)
               end
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(:jmxArgs => jmxArgs, :jmxConn => jmxConn))

    self.jmx_connection = jmxConn # contract will validate it

  end  
  

  def close_jmx_connection_or_croak(jmxArgs={}, &jmxBlok)
    eye = :'rftj cl_jmx_conn'

    jmxConn = mustbe_jmx_connection_key_or_nil_or_croak(jmxArgs, :jmx_connection, eye) || (jmxConnAccessor = jmx_connection)

    jmxConn && begin
                 jmxConn.close
                 (jmxConn == jmxConnAccessor) && reset_jmx_connection # reset accessor if same
               rescue Exception => e
                 errorText = "Connection close error: jmxArgs >#{jmxArgs}< jmxHost >#{jmxHost}< jmxPort >#{jmxPort}< jmxUser >#{jmxUser}< error >#{e.message}"
                 #riemann_alert
                 connection_exception(e, errorText)
               end
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(:jmxArgs => jmxArgs, :jmxConn => jmxConn))

    self

  end  
  
end

klassConstant = Potrubi::Core.assign_class_constant_or_croak(classContent, :Riemann, :Feeds, :Type, :JMX)

###puts "RIEMANN FEED TYPE JMX >#{klassConstant.class}< >#{klassConstant}<"

__END__






  def zzzmake_syntax_bean_methods_or_croak(beanArgs, &beanBlok)
    eye = :'rftj ms_bean_mths'

    $DEBUG && logger_me(eye, logger_fmt_kls(beanArgs: beanArgs, beanBlok: beanBlok))

    beanName = mustbe_string_key_or_croak(beanArgs, :bean_name, eye, "beanName not string")
    beanAttrs = mustbe_hash_key_or_croak(beanArgs, :bean_attrs, eye, "beanAttrs not hash")

    beanMths = collect_attribute_method_definitions_or_croak(beanAttrs)

    $DEBUG && logger_mx(eye, logger_fmt_kls(beanArgs: beanArgs), logger_fmt_kls(:beanMtds => beanMths))

    mustbe_hash_or_croak(beanMths, eye, "beanMths not hash")

    ##STOPEHEREATTR

  end
  
jmxConstants = klassConstant.constants(false)

puts "RIEMANN FEED TYPE JMX CONSTANTS >#{jmxConstants.class}< >#{jmxConstants}<"

jmxConstants.each do |c|
  v = klassConstant.const_get(c, false)
  puts "RIEMANN FEED TYPE JMX CONSTANT c >#{c}< v >#{v.class}< >#{v}<"
end


  def make_bean_attrs_specs_or_croak(beanArgs, &beanBlok)
    eye = :'rftj m_bean_attrs_specs'

    $DEBUG && logger_me(eye, logger_fmt_kls(beanArgs: beanArgs, beanBlok: beanBlok))

    STOPMAKEBEANATTRSSPECS

    
    beanSpec = mustbe_bean_spec_or_nil_or_croak(beanArgs[:bean_spec], eye, "beanSpec failed contract") || {}
    beanName = mustbe_bean_name_or_croak(beanArgs[:bean_name], eye, "beanName failed contract")
    
    $DEBUG && logger_ms(eye, logger_fmt_kls(beanName: beanName, :beanSpec => beanSpec))
    
    attrDefsNom = mustbe_hash_key_or_nil_or_croak(beanSpec, :attributes, eye, "attribuste not hash")

    $DEBUG && logger_ms(eye, 'BEAN ATTRS NOM', logger_fmt_kls(:attrDefsNom => attrDefsNom))

    ###STOPHEREATARRTDEFSNOM
    
    JMX_LIVE_MODE && (attrAll = find_jmx_bean_attributes_with_metrics_or_croak(beanName).keys)
    JMX_LIVE_MODE || (attrAll = mustbe_hash_key_or_nil_or_croak(attrDefsNom, :definitions, eye, "TESTING attributes map not hash").keys)

    attrDefsNrm = potrubi_util_merge_hashes_or_croak({:all => attrAll}, attrDefsNom)

    $DEBUG && logger_ms(eye, 'BEAN ATTRS NRM', logger_fmt_kls(:attrDefsNrm => attrDefsNrm))
    
    beanAttrs = make_attribute_specs_or_croak(attrDefsNrm)

    ##SOPTPOSMAKEATTRSPECS
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(beanName: beanName, :beanAttrs => beanAttrs))

    mustbe_hash_or_croak(beanAttrs, "beanName >#{beanName}< beanAttrs not hash")
    
  end
