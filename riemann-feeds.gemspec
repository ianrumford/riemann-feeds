# -*- encoding: utf-8 -*-

$LOAD_PATH.push File.expand_path("../lib", __FILE__)

###puts "LOAD PATH >#{$LOAD_PATH}<"

require "riemann/feeds/version"

selectMatches = [
                 Regexp.new('\.rb\Z'),
                 Regexp.new('README'),
                 Regexp.new('LICENCE'),
                ]

rejectMatches = [
                 Regexp.new('HOLD'),
                 Regexp.new('lsfiles.rb'),
                 Regexp.new('Rakefile'),
                 Regexp.new('Gemfile'),
                 Regexp.new('.gemspec\Z'),
                 ]

Gem::Specification.new do |s|

  s.name        = "riemann-feeds"
  s.version     = Riemann::Feeds::VERSION
  s.authors     = ["Ian Rumford"]
  s.email       = ["ian@rumford.name"]
  s.homepage    = "https://github.com/ianrumford/riemann-feeds"
  s.summary     = %q{Riemann-Feeds: Riemann event collectors}
  s.description = %q{Riemann-Feeds: a framework for Riemann event collectors}

  s.rubyforge_project = "riemann-feeds"

  s.files         = `git ls-files`.split("\n").select {|f| selectMatches.any? {|r| r.match(f)} && (! rejectMatches.any? {|r| r.match(f) })  }

  s.executables = []
  
  s.require_paths = ["lib"]

  s.add_runtime_dependency "riemann-client"
  s.add_runtime_dependency "potrubi", '>= 0.0.2'

  #s.add_runtime_dependency "jmx4r", '>= 0.1.4'

  s.required_ruby_version = '>= 1.9.3'
  
end
