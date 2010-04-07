$:.unshift(File.dirname(__FILE__)) unless
  $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

module ReliableMsg
  PACKAGE = 'reliable-msg'
  
  # Version number.
  module Version
    version = Gem::Specification.load(File.expand_path("../reliable-msg.gemspec", File.dirname(__FILE__))).version.to_s.split(".").map { |i| i.to_i }
    MAJOR = version[0]
    MINOR = version[1]
    PATCH = version[2]
    STRING = "#{MAJOR}.#{MINOR}.#{PATCH}"
  end

  VERSION = Version::STRING
end

require "reliable-msg/queue"
require "reliable-msg/topic"
require "reliable-msg/cli"
begin
  require "reliable-msg/rails"
rescue LoadError
end
