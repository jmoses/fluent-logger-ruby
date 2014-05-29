# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'fluent/logger/version'

Gem::Specification.new do |gem|
  unless File.exist?("vendor/fluentd/Gemfile")
    puts "git submodule update -i"
    system("git submodule update -i")
  end

  gem.name        = %q{jmoses_fluent-logger}
  gem.version     = Fluent::Logger::VERSION
  # gem.platform  = Gem::Platform::RUBY
  gem.authors     = ["Jon Moses", "Sadayuki Furuhashi"]
  gem.email       = %q{jon@burningbush.us frsyuki@gmail.com}
  gem.homepage    = %q{https://github.com/jmoses/fluent-logger-ruby}
  gem.description = %q{fluent logger for ruby}
  gem.summary     = gem.description + " (fork by jmoses)"

  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency 'yajl-ruby', '~> 1.0'
  gem.add_dependency "msgpack", [">= 0.4.4", "!= 0.5.0", "!= 0.5.1", "!= 0.5.2", "!= 0.5.3", "< 0.6.0"]
  gem.add_development_dependency 'rake', '>= 0.9.2'
  gem.add_development_dependency 'rspec', '>= 2.7.0'
  gem.add_development_dependency 'simplecov', '>= 0.5.4'
  gem.add_development_dependency 'timecop', '>= 0.3.0'
end
