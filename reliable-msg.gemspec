Gem::Specification.new do |spec|
  spec.name = 'reliable-msg'
  spec.version = '1.2.0'
  spec.summary = "Reliable messaging and persistent queues for building asynchronous applications in Ruby"
  spec.description = <<-EOF
    This package provides reliable messaging and persistent queues for
    building asynchronous applications in Ruby.

    It supports transaction processing, message selectors, priorities,
    delivery semantics, remote queue managers, disk-based and MySQL message
    stores and more.
  EOF
  spec.author = 'Assaf Arkin'
  spec.email = 'assaf@labnotes.org'
  spec.homepage = 'http://github.com/assaf/reliable-msg'
  spec.rubyforge_project = 'reliable-msg'

  spec.files = Dir['{bin,test,lib,docs}/**/*', 'README.rdoc', 'MIT-LICENSE', 'Rakefile', 'CHANGELOG', 'reliable-msg.gemspec']
  spec.require_path = 'lib'
  spec.bindir = 'bin'
  spec.executables = ['queues']
  spec.default_executable = 'queues'

  spec.has_rdoc = true
  spec.rdoc_options << '--main' << 'README.rdoc' << '--title' <<  "Reliable Messaging for Ruby"
  spec.extra_rdoc_files = ['README.rdoc']
  spec.add_dependency 'uuid', '~>2.0'
end
