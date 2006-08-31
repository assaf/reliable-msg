# Adapted from the rake Rakefile.

require "rubygems"
Gem::manage_gems
require "rake/testtask"
require "rake/rdoctask"
require "rake/gempackagetask"


desc "Default Task"
task :default => [:tests, :rdoc]


desc "Run test case for Queue API"
Rake::TestTask.new :test_queue do |test|
  test.verbose = true
  test.test_files = ["test/test-queue.rb"]
end
desc "Run test case for Topic API"
Rake::TestTask.new :test_topic do |test|
  test.verbose = true
  test.test_files = ["test/test-topic.rb"]
end
desc "Run test case for Rails integration"
Rake::TestTask.new :test_rails do |test|
  test.verbose = true
  test.test_files = ["test/test-rails.rb"]
end
desc "Run all test cases"
Rake::TestTask.new :tests do |test|
  test.verbose = true
  test.test_files = ["test/*.rb"]
  #test.warning = true
end


# Create the documentation.
Rake::RDocTask.new do |rdoc|
  rdoc.main = "README"
  rdoc.rdoc_files.include("README", "lib/**/*.rb")
  rdoc.title = "Reliable Messaging"
end

# Handle version number.
class Version

  PATTERN = /(\s*)VERSION.*(\d+\.\d+\.\d+)/

  def initialize file, new_version
    @file = file
    @version = File.open @file, "r" do |file|
      version = nil
      file.each_line do |line|
        match = line.match PATTERN
        if match
          version = match[2]
          break
        end
      end
    version
    end
    fail "Can't determine version number" unless @version
    @new_version = new_version || @version
  end

  def changed?
    @version != @new_version
  end

  def number
    @version
  end

  def next
    @new_version
  end

  def update
    puts "Updating to version #{@new_version}"
    copy = "#{@file}.new"
    open @file, "r" do |input|
      open copy, "w" do |output|
        input.each_line do |line|
          match = line.match PATTERN
          if match
            output.puts "#{match[1]}VERSION = \"#{@new_version}\""
          else
            output.puts line
          end
        end
      end
    end
    mv copy, @file
    @version = @new_version
  end

end
version = Version.new "lib/reliable-msg.rb", ENV["version"]


# Create the GEM package.
gem_spec = Gem::Specification.new do |spec|
  spec.name = "reliable-msg"
  spec.version = version.next
  spec.summary = "Reliable messaging and persistent queues for building asynchronous applications in Ruby"
  spec.description = <<-EOF
    This package provides reliable messaging and persistent queues for
    building asynchronous applications in Ruby.

    It supports transaction processing, message selectors, priorities,
    delivery semantics, remote queue managers, disk-based and MySQL message
    stores and more.
  EOF
  spec.author = "Assaf Arkin"
  spec.email = "assaf@labnotes.org"
  spec.homepage = "http://trac.labnotes.org/cgi-bin/trac.cgi/wiki/Ruby/ReliableMessaging"

  spec.files = FileList["{bin,test,lib,docs}/**/*", "README", "MIT-LICENSE", "Rakefile", "changelog.txt"].to_a
  spec.require_path = "lib"
  spec.autorequire = "reliable-msg.rb"
  spec.bindir = "bin"
  spec.executables = ["queues"]
  spec.default_executable = "queues"
  spec.requirements << "MySQL for database store, otherwise uses the file system"
  spec.has_rdoc = true
  spec.rdoc_options << "--main" << "README" << "--title" <<  "Reliable Messaging for Ruby" << "--line-numbers"
  spec.extra_rdoc_files = ["README"]
  spec.rubyforge_project = "reliable-msg"
  spec.add_dependency(%q<uuid>, [">= 1.0.0"])
end

gem = Rake::GemPackageTask.new(gem_spec) do |pkg|
  pkg.need_tar = true
  pkg.need_zip = true
end


desc "Look for TODO and FIXME tags in the code"
task :todo do
  FileList["**/*.rb"].egrep /#.*(FIXME|TODO|TBD)/
end


# --------------------------------------------------------------------
# Creating a release

desc "Make a new release"
task :release => [:tests, :prerelease, :clobber, :update_version, :package] do
  puts
  puts "**************************************************************"
  puts "* Release #{version.number} Complete."
  puts "* Packages ready to upload."
  puts "**************************************************************"
  puts
end

task :prerelease do
  if !version.changed? && ENV["reuse"] != version.number
    fail "Current version is #{version.number}, must specify reuse=ver to reuse existing version"
  end
end

task :update_version => [:prerelease] do
  if !version.changed?
    puts "No version change ... skipping version update"
  else
    version.update
  end
end

