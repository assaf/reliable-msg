$:.unshift(File.dirname(__FILE__)) unless
  $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

module ReliableMsg
  PACKAGE = 'reliable-msg'
  VERSION = '1.1.1'
end

require "reliable-msg/queue"
require "reliable-msg/topic"
require "reliable-msg/cli"
begin
  require "reliable-msg/rails"
rescue LoadError
end
