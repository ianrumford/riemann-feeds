
# riemann feed mixin standard

# includes all the usual mixins

require 'potrubi/core'

requireList = %w(initialize util persistence configuration)
requireList.each {|r| require "potrubi/mixin/#{r}"}

###["logger", "exceptions", "contracts"].each { |m| require_relative m }

mixinContent = Module.new do

  include Potrubi::Core
  include Potrubi::Mixin::Initialize
  include Potrubi::Mixin::Persistence
  include Potrubi::Mixin::Configuration
  include Potrubi::Mixin::Util

  def initialize(initArgs=nil, &initBlok)
    eye = :'rfms i'

    $DEBUG && logger_me(eye, logger_fmt_kls_only(:initArgs => initArgs, :initBlok => initBlok))

    super
    
    $DEBUG && logger_mx(eye, logger_fmt_kls_only(:initArgs => initArgs, :initBlok => initBlok))
    
  end

end

mixinConstant = Potrubi::Core.assign_module_constant_or_croak(mixinContent, :Riemann, :Feeds, :Mixin, :Standard)

__END__

