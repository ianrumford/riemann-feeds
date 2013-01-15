
# riemann feed handler

# manages feeds

require_relative  "mixin/standard"
require 'potrubi/mixin/contract-recipes'

klassContent = Class.new do
  
  include Riemann::Feeds::Mixin::Standard
  
  # create the accessors with associated contracts

  def self.feed_specification_keys
    @feed_spec_keys ||= [:name, :type, :configuration]
  end
  
  attrAccessors = {
    ###:type => :string,
    ###:name => :string,
    ##feed: :hash,
    feed_specification: {edit: {KEY_NAMES: 'self.class.feed_specification_keys'}, spec: :method_accessor_is_value_collection_with_keys},

    #feed_specification_type: :symbol,
    #feed_specification_name: :string,
    #feed_specification_configuration: :hash,
    
    watch: :hash,
    feed_instance: 'Riemann::Feeds::Type::Super', # all feeds must have this as superclass

  }

  Potrubi::Mixin::ContractRecipes.recipe_accessors(self, attrAccessors, :package_accessor_with_contract)

  mustbeContracts = {
    feed_name: :string,
    feed_type: :symbol,
    feed_config: :hash,
  }

  Potrubi::Mixin::ContractRecipes.recipe_mustbes(self, mustbeContracts)
  
  def initialize(feedArgs=nil, &feedBlok)
    eye = 'rie::feed-hndl i'
    
    $DEBUG && logger_me(eye, "FEED HNDL INIT",  logger_fmt_kls(:feedArgs => feedArgs, :feedBlok => feedBlok))

    super
      
    $DEBUG && logger_mx(eye, 'FEED HNDL INIT')
    
  end
  

  def make_watch_or_croak(watchArgs=nil, &watchBlok)
    eye = :'rie::feed-hndl m_watch'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(watchArgs: watchArgs),  logger_fmt_kls(watchBlok: watchBlok) )

    ###mustbe_watch_handler_or_croak(watchArgs, eye, "watchArgs not really a watch handler")

    ##watchWatch = watch
    
    ###mustbe_hash_or_croak(watchArgs, eye, "watchArgs not hash")

    STOPBEFORMAKEWATCHRETURN
    
    nil
  end

    
  def import_feed_or_croak(feedArgs)
    eye = :'rie::feed-hndl imp_feed'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedArgs => feedArgs))

    feedSpec = set_feed_specification(read_maybe_configuration_sources_list_or_croak(feedArgs).first) # set will apply contract

    #STOPIMPFEED
    
    ###mustbe_subset_or_croak(feedSpec.keys, self.class.feed_specification_keys, eye) # check allowed keys
    
    feedName = mustbe_feed_name_key_or_croak(feedSpec, :name)
    feedType = mustbe_feed_type_key_or_croak(feedSpec, :type)
    feedConf = mustbe_feed_config_key_or_croak(feedSpec, :configuration)
    
    $DEBUG && logger_ms(eye, 'FEED', logger_fmt_kls(feedName: feedName, feedType: feedType, feedConf: feedConf))

    feedKlass = find_feed_class_or_croak(feedType)
    
    feedInst = feedKlass.new(feedConf)

    $DEBUG && logger_mx(eye, 'FEED', logger_fmt_kls(:feedName => feedName, :feedInst => feedInst))

    set_feed_instance(feedInst)

  end
  

  def find_feed_class_or_croak(feedType)
    eye = :'rie::feed-hndl f_feed_kls'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedType => feedType))

    feedKlass = case feedType
                        when Class then feedType
                        when Symbol then find_or_require_class_constant_or_croak("Riemann::Feeds::Type::#{feedType}")
                        when String then find_or_require_class_constant_or_croak(feedType)
                        else
                          surprise_exception(feedType, eye, "feedType is what?")
                        end
    
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(:feedType => feedType, :feedKlass => feedKlass))

    mustbe_class_or_croak(feedKlass)
    
  end
    
    
  def run_feed(runArgs=nil, &runBlok)
    eye =  :'rie::feed-hndl r_feed'
    eyeTale = 'RUN'
    
    $DEBUG && logger_me(eye, eyeTale,  logger_fmt_kls(:runArgs => runArgs, :runBlok => runBlok))

    feedInst = find_feed_instance_or_croak
    
    feedInst.run(runArgs, &runBlok)

    $DEBUG && logger_mx(eye, eyeTale,  logger_fmt_kls(:runArgs => runArgs, :runBlok => runBlok))
    
    self
    
  end

end


klassConstant = Potrubi::Core.assign_class_constant_or_croak(klassContent, :Riemann, :Feeds, :FeedHandler)

__END__


