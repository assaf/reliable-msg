# Adapted from the rake Rakefile.

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
  rdoc.main = "README.rdoc"
  rdoc.rdoc_files.include("README.rdoc", "lib/**/*.rb")
  rdoc.title = "Reliable Messaging"
end


# Create the GEM package.
gem_spec = Gem::Specification.load('reliable-msg.gemspec')

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
task :release => [:tests, :clobber, :package] do
  puts
  puts "**************************************************************"
  puts "* Release #{spec.version} Complete."
  puts "* Packages ready to upload."
  puts "**************************************************************"
  puts
end
