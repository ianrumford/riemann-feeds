
# rieman feed builder

# consolidates configuration and build one or more feed yaml files


###require_relative  "mixin/standard"
require_relative 'feed-handler'


klassContent = Class.new do
  
  include Riemann::Feeds::Mixin::Standard

  
  # create the accessors with associated contracts
  
  attrAccessors = {
    :type => nil, # validation later
    :name => :string,
    :config => :hash,
    :feeds => :hash,
    :templates => :hash,
  }

  Potrubi::Mixin::ContractRecipes.recipe_accessors(self, attrAccessors, :attr_accessor_with_contract)

  # create some contracts
  
  mustbeContracts = {
    :feed_handler => 'Riemann::Feeds::FeedHandler',
  }

  Potrubi::Mixin::ContractRecipes.recipe_mustbes(self, mustbeContracts)

  ###STOPFEEDBUILDEPOTSCONTRACTS
  
  def initialize(feedArgs=nil, &feedBlok)
    eye = 'rie::feed-bldr i'
    
    $DEBUG && logger_me(eye, "Hi there",  logger_fmt_kls(:feedArgs => feedArgs, :feedBlok => feedBlok))
    ##super

    ###STOPHEREININIT
    
    feedArgs &&  build_feeds_or_croak(feedArgs)
    Kernel.block_given? && instance_eval(&feedBlok)

    ###super
      
    $DEBUG && logger_mx(eye, 'BYE BYE')

    
  end
  



    
  def import_feed_spec_or_croak(feedSpec)
    eye = :'rie::feed-bldr imp_feed_spec'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedSpec => feedSpec))

    mustbe_hash_or_croak(feedSpec, eye, "feedSpec not hash")

    set_attributes_or_croak(feedSpec)

    ###STOPPOSTATTRS
    
    $DEBUG && logger_mx(eye, logger_fmt_kls(:feedSpec => feedSpec))
    
    self
  end
  
  def build_feeds_or_croak(feedArgs)
    eye = :'rie::feed-bldr bld_feeds'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedArgs => feedArgs))
    
    feedDefs = build_feeds_definitions_or_croak(feedArgs)

    watchSpec = {
      watch_directory: './watch'
    }

    feedHndls = make_feeds_handlers_or_croak(feedDefs, watchSpec)

    

    feedHndls.map do | feedHndl |


      feedWatch = feedHndl.make_watch_or_croak
      
      ###make_watch_or_croak(feedHndl)
      
    end
    

    

    $DEBUG && logger_mx(eye, logger_fmt_kls(:feedArgs => feedArgs))
    
    self
    
  end


   
  def make_feeds_handlers_or_croak(feedDefs, watchSpec, &feedBlok)
    eye = :'rie::feed-bldr m_feeds_handlers'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(feedDefs: feedDefs),  logger_fmt_kls(watchSpec: feedDefs), logger_fmt_kls(feedBlok: feedBlok) )

    mustbe_hash_or_croak(feedDefs, eye, "feedDefs not hash")
    mustbe_hash_or_croak(watchSpec, eye, "watchSpec not hash")

    feedHandlerKls = Riemann::Feeds::FeedHandler

    ###watchSpec = {}
    
    feedHndls = feedDefs.map do |(feedName, feedDef)|

      ###mustbe_hash_or_croak(feedDef, eye, "feedDef not hash")
      feedHandlerKls.new(feed: feedDef, watch: watchSpec)

      ###STOPPOSTHNDLCREATE
    end
    
    $DEBUG && logger_mx(eye, logger_fmt_kls_only(:feedDefs => feedDefs),logger_fmt_kls(:feedHndls => feedHndls) )

    mustbe_array_or_croak(feedHndls, eye, "feedHndls not array")

  end
  
    
  def build_feeds_definitions_or_croak(feedArgs)
    eye = :'rie::feed-bldr bld_feeds_defs'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedArgs => feedArgs))

    ###import_feed_spec_or_croak(feedArgs)

    #  Top level common configuration - applies to all feeds
    
    commonConfig = mustbe_hash_key_or_nil_or_croak(feedArgs, :config, eye, "feed config failed contract")

    # Type of feed e.g. JMX, etc
    
    feedType = mustbe_type_key_or_croak(feedArgs, :type, eye, "feed type failed contract")
    ###feedKlass = find_feed_class_or_croak(feedType) # for validation; not used

    # The feed definitions, with *:config key* templates as optional
    
    feedInstances = mustbe_feeds_key_or_croak(feedArgs, :feeds, eye, "feeds failed contract")
    feedTemplates = mustbe_templates_key_or_nil_or_croak(feedArgs, :templates, eye, "feed templates failed contract") || {}

    # Create the feed envelope by deselecting the builder-related keys
    
    commonEnvelop = feedArgs.select {|k,v| ! [:config, :feeds, :templates].include?(k) } 

    # Create a flat has of all the feed definitions
    
    feedsDefs = feedInstances.each_with_object({}) do | (feedName, feedSpec), h |

      $DEBUG && logger_beg(eye, 'MAKE FEED', logger_fmt_kls(:feedName => feedName, :feedSpec => feedSpec))

      # does this feed have a template configuration?
      
      feedTemplate = mustbe_hash_key_or_nil_or_croak(feedTemplates, feedName, eye, "feedName >#{feedName}< template not hash")

      # Make an array, even if onpy one (hash)
      feedSpecs = case feedSpec
                  when Array then feedSpec
                  when Hash then [feedSpec]
                  else
                    surpise_exception(feedSpec, eye, "feedSpec not hash or arry")
                  end

      mustbe_hashs_or_croak(*feedSpecs)  # validate

      feedSpecsMax = feedSpecs.size - 1 

      feedDefs = feedSpecs.map.with_index do | feedSpec, feedNdx |

        # create the feed "envelope" (:config will be supplied later)
        
        feedEnvelop = potrubi_util_merge_hashes_or_croak(commonEnvelop, feedSpec)
        feedEnvelop.delete(:config)
        
        $DEBUG && logger_ms(eye, 'FEED ENVELOP', logger_fmt_kls(:feedName => feedName, :feedNdx => feedNdx, :feedEnvelop => feedEnvelop))
        
        feedConfigNom = mustbe_hash_key_or_nil_or_croak(feedSpec, :config, eye, "feedName >#{feedName}< feedNdx > #{feedNdx}< feedSpec config not hash")

        # Create the consolidated configuration (:config)
        # Note template is a *:config*
        
        
        feedConfig = merge_hashes_with_array_values_or_croak(commonConfig, feedTemplate, feedConfig) # order is important

        $DEBUG && logger_ms(eye, 'FEED CONFIG', logger_fmt_kls(:feedName => feedName, :feedNdx => feedNdx, :feedConfig => feedConfig))

        ##STOPHEREBEFOREFULLENVENLOP
        
        # And make the feed instance with the correct configuration
        
        nil # TESTING RETRUN
        #feedKlass.new(feedSpecAll)
        #feedObj = feedKlass.new

        # does this feed need a uniq name?

        feedSpecName = (feedNdx == feedSpecsMax) ? feedName : "#{feedName}#{feedNdx}"

        is_value_key?(h, feedSpecName) && logic_exception(feedSpecName, eye, "feedSpecName already seen")
        
 #h[feedSpecName] = feedEnvelop.merge({:name => feedSpecName, :feeds => {feedSpecName => {:config => feedConfig}}})  # complete feed specifcation
 h[feedSpecName] = feedEnvelop.merge(:name => feedSpecName, :config => feedConfig) # complete feed specifcation
                
      end
      
      
      $DEBUG && logger_fin(eye, 'MADE FEED', logger_fmt_kls(:feedName => feedName, :feedDefs => feedDefs))

      ###STOPFEEDOBJS
      
      ###h[feedName] = feedDefs

    end
    
    
    ####mustbe_hash_or_croak(feedSpec, eye, "feedSpec not hash")

    ###STOPPOSTATTRS
    
    #feedSpecs = feedSpec.flatten(1)
    #feedSpecs = feedSpec
    
    ###feedObjs = make_feed_objects_or_croak(feedSpecs)
    
    ###self.feeders = feedObjs

    $DEBUG && logger_mx(eye, logger_fmt_kls(:feedsDefs => feedsDefs))
    
    mustbe_hash_or_croak(feedsDefs, eye, "feedsDefs not hash")

    ##STOPAFTERFEEDDEFS
    
  end
  

end


klassConstant = Potrubi::Core.assign_class_constant_or_croak(klassContent, :Riemann, :Feeds, :FeedBuilder)

puts "FEEDER class constant >#{klassConstant}<"

__END__


















































































  def import_feed_configuration_or_croak(confArgs, &confBlok)
    eye = :'rie::feed imp_cfg'
    
    $DEBUG && logger_me(eye, "IMP CONF",  logger_fmt_kls(:confArgs => confArgs, :confBlok => confBlok))

    mustbe_array_or_croak(confArgs, eye, "confARgs (ARGV) not array")


    importArgs.each do | argName, argMethod |

      argPath = mustbe_file_key_or_nil_or_croak(confOpts, argName, eye, "#{argName} key not file or nil")

      argPath && begin
                   argData = read_yaml_file_or_croak(argPath)
                   self.__send__(argMethod, argData)
                 end
      
    end
    
    ###[:'config-file'].each {|a| mustbe_file_key_or_nil_or_croak(confOpts, a, eye, "#{a} not string or nil") }
    
    $DEBUG && logger_mx(eye, "IMP CONF",  logger_fmt_kls(:confOpts => confOpts, :confBlok => confBlok))
    
  end
  
  def import_configuration_or_croak(confArgs, &confBlok)
    eye = :'rie::feed-bldr imp_cfg'
    
    $DEBUG && logger_me(eye, "IMP CONF",  logger_fmt_kls(:confArgs => confArgs, :confBlok => confBlok))

    mustbe_array_or_croak(confArgs, eye, "confARgs (ARGV) not array")
    
    confOpts = Trollop::options(confArgs) do
      version "Riemann Feeds (c) 2012 Ian Rumford"
      banner <<-EOS

Riemann Feeds is a framework to feed Riemann with event
from differnt type of event sources.

The only event source today is a Java JMX souce

Current usage is to monitor Hadoop and HBase

Usage:
       test [options] <filenames>+

where [options] are:

puts the parms defs here

EOS

      #opt :ignore, "Ignore incorrect values"
      
      opt :'config-file', "YAML configuration file with feed definitiosn", :type => String

      ###opt :jmx_credentials, "YAML file with JMX credentials", :type => String

      #opt :volume, "Volume level", :default => 3.0

      #opt :iters, "Number of iterations", :default => 5
    end

    $DEBUG && logger_ms(eye, "confOpts", logger_fmt_kls(:confOpts => confOpts))

    # Process the YAML config files
    
    importArgs = {
      :'config-file' => 'import_feed_specs_or_croak'
    }

    importArgs.each do | argName, argMethod |

      argPath = mustbe_file_key_or_nil_or_croak(confOpts, argName, eye, "#{argName} key not file or nil")

      argPath && begin
                   argData = read_yaml_file_or_croak(argPath)
                   self.__send__(argMethod, argData)
                 end
      
    end
    
    ###[:'config-file'].each {|a| mustbe_file_key_or_nil_or_croak(confOpts, a, eye, "#{a} not string or nil") }
    
    $DEBUG && logger_mx(eye, "IMP CONF",  logger_fmt_kls(:confOpts => confOpts, :confBlok => confBlok))
    
  end
  
  def make_feed_objects_or_croak(feedSpecs)
    eye = :'rie::feed-bldr m_feed_objs'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedSpecs => feedSpecs))
    mustbe_array_or_croak(feedSpecs, eye)

    feedObjs = feedSpecs.each_with_object([]) do | feedSpec, a |

      mustbe_hash_or_croak(feedSpec, eye, "feedSpec not hash")

      feedName = feedSpec[:name] || :anon
      ###feedName = :anon
      
      $DEBUG && logger_beg(eye, 'FEED', logger_fmt_kls(:feedName => feedName, :feedSpec => feedSpec))

      mustbe_empty_or_croak(feedSpec.keys - [:name, :class, :config, :credentials]) # check allowed keys
      
      feedClass = mustbe_not_nil_or_croak(feedSpec[:class])

      feedClassConstant = case feedClass
                          when Class then feedClass
                          when String then find_or_require_class_constant_or_croak(feedClass)
                          else
                            surprise_exception(feedClass, eye, "feedClass is what?")
                          end
      
      feedInst = feedClassConstant.new(feedSpec[:config])

      ###feedInst.credentials = feedSpec[:credentials]  # leave up to feed object to make sense

      $DEBUG && logger_fin(eye, 'FEED', logger_fmt_kls(:feedName => feedName, :feedInst => feedInst))

      feedInst
      
      
      ###h[feedName] = feedInsts

      a << feedInst
      
    end

    $DEBUG && logger_mx(eye, logger_fmt_kls(:feedObjs => feedObjs))

    mustbe_array_or_croak(feedObjs)
    
    
  end

  def run_feeds(runArgs=nil, &runBlok)
    eye = 'rie::feed-bldr r_feeds'
    eyeTale = 'RUN'
    
    $DEBUG && logger_me(eye, eyeTale,  logger_fmt_kls(:runArgs => runArgs, :runBlok => runBlok))

    feedObjs = mustbe_array_or_croak(feeders, eye, "feeders not array")

    feedObjs.each do | feedInst |

      $DEBUG && logger_beg(eye, 'FEED', eyeTale,  logger_fmt_kls(:feedInst => feedInst))

      feedInst.run(runArgs, &runBlok)

      $DEBUG && logger_fin(eye, 'FEED', eyeTale,  logger_fmt_kls(:feedInst => feedInst))
      
    end

    $DEBUG && logger_mx(eye, eyeTale,  logger_fmt_kls(:runArgs => runArgs, :runBlok => runBlok))
    
    self
    
  end


  def initialize(*feedConf, &feedBlok)
    eye = 'rie::feed-bldr i'
    
    $DEBUG && logger_me(eye, "Hi there",  logger_fmt_kls(:feedConf => feedConf, :feedBlok => feedBlok))
    ##super

    ###self.feeders = feedObjs = make_feed_objects_or_croak(feedConf)

    feedConf.empty? || import_configuration_or_croak(*feedConf)

    Kernel.block_given? && instance_eval(&feedBlok)
      
    $DEBUG && logger_mx(eye)

    
  end
  












 def make_feed_objects_or_croak(feedConfig)
    eye = :'rie::feed-bldr m_feed_objs'
    
    $DEBUG && logger_me(eye, logger_fmt_kls(:feedConfig => feedConfig))
    mustbe_hash_or_croak(feedConfig, eye)

    feedSumm = feedConfig.each_with_object({}) do |  feedSpecNom, h |

      ###mustbe_hash_or_croak(feedSpecNom, eye, "feedSpecNom not hash")

      
      ###feedName = feedSpecNom[:name] || :anon
      feedName = :anon
      
      
      logger_debug(eye, 'BEG', logger_fmt_kls(:feedName => feedName, :feedSpec => feedSpecNom))

      #mustbe_hash_or_croak(feedSpec, 'feedSpec not hash')
      feedSpecs = case feedSpecNom
                  when Hash then [feedSpecNom]
                  w###hen Array then feedSpecNom
                  else
                    surpise_exception(feedSpec, eye, "feedSpec is what?")
                  end

      mustbe_hashs_or_croak(*feedSpecs)

      feedInsts = feedSpecs.map do | feedSpec |

        mustbe_empty_or_croak(feedSpec.keys - [:class, :config]) # check allowed keys
        
        feedClass = mustbe_not_nil_or_croak(feedSpec[:class])

        feedClassConstant = case feedClass
                            when Class then feedClass
                            when String then find_or_require_class_constant_or_croak(feedClass)
                            else
                              surprise_exception(feedClass, eye, "feedClass is what?")
                            end
        
        feedInst = feedClassConstant.new(feedSpec[:config])

        logger_debug(eye, 'FIN', logger_fmt_kls(:feedName => feedName, :feedInst => feedInst))

        feedInst
      end
      
      h[feedName] = feedInsts
      
    end

