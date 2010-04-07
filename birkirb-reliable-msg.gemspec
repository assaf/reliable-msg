Gem::Specification.new do |spec|
  spec.name = 'birkirb-reliable-msg'
  spec.version = '1.1.2'
  spec.summary = "Reliable messaging and persistent queues for building asynchronous applications in Ruby"
  spec.description = <<-EOF
    This package provides reliable messaging and persistent queues for
    building asynchronous applications in Ruby.

    It supports transaction processing, message selectors, priorities,
    delivery semantics, remote queue managers, disk-based and MySQL message
    stores and more.
  EOF
  spec.author = 'Birkir A. Barkarson'
  spec.email = 'birkirb@stoicviking.net'
  spec.homepage = 'http://github.com/birkirb/reliable-msg'
  spec.rubyforge_project = 'reliable-msg'

  spec.files = Dir['{bin,test,lib,docs}/**/*', 'README.rdoc', 'MIT-LICENSE', 'Rakefile', 'changelog.txt']
  spec.require_path = 'lib'
  spec.bindir = 'bin'
  spec.executables = ['queues']
  spec.default_executable = 'queues'

  spec.has_rdoc = true
  spec.rdoc_options << '--main' << 'README.rdoc' << '--title' <<  "Reliable Messaging for Ruby"
  spec.extra_rdoc_files = ['README.rdoc']
  spec.add_dependency 'uuid', '~>2.0'
end
