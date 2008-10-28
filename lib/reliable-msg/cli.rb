#
# = cli.rb - Reliable messaging command-line interface
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++


require "drb"
require "optparse"
#require "rdoc/usage"
require "reliable-msg/queue-manager"

module ReliableMsg

  class CLI #:nodoc:

    USAGE = <<-EOF
usage:  queues command [args] [options]

To see list of available commands and options
  queues help
    EOF

    HELP = <<-EOF
usage:  queues command [args] [options]

Reliable messaging queue manager, version #{VERSION}

Available commands:

  help
    Display this help message.

  manager start
    Start the queue manager as a standalone server

  manager stop
    Stop a running queue manager.

  list <queue>
    List headers of messages in the named queue.

  empty <queue>
    Empty (delete all messages from) the named queue.

  install disk [<path>]
    Configure queue manager to use disk-based message store
    using the specified directory. Uses 'queues' by default.

  install mysql <host> <username> <password> <database> [options]
                [--port <port>] [--socket <socket>] [--prefix <prefix>]
    Configure queue manager to use MySQL for message store,
    using the specified connection properties. Updates database
    schema.

  --port  Port to connect to
  --socket  Socket to connect to
  --prefix  Prefix for table names


Available options:

  -v --version
    Show version number.

  -c --config <path>
    Points to the queue manager configuration file.

    EOF

    MAX_STRING = 50

    class InvalidUsage  < Exception #:nodoc:
    end

    def initialize()
    end

    def run()
      begin
        config_file = nil
        opts = OptionParser.new
        opts.on("-c FILE", "--config FILE", String) { |value| config_file = value }
        opts.on("-v", "--version") do
          puts "Reliable messaging queue manager, version #{VERSION}"
          exit
        end
        opts.on("-h", "--help") do
          puts HELP
          exit
        end

        args = opts.parse(ARGV)

        raise InvalidUsage if args.length < 1
        case args[0]
        when 'help'
          puts HELP

        when 'manager'
          case args[1]
          when 'start', nil
            manager = QueueManager.new({:config=>config_file})
            manager.start
            begin
              while manager.alive?
                sleep 3
              end
            rescue Interrupt
              manager.stop
            end
          when 'stop'
            begin
              queue_manager(config_file).stop
            rescue DRb::DRbConnError =>error
              puts "Cannot access queue manager: is it running?"
            end
          else
            raise InvalidUsage
          end

        when 'install'
          config = Config.new config_file, nil
          case args[1]
          when 'disk'
            store = MessageStore::Disk.new({}, nil)
            config.store = store.configuration
            if config.create_if_none
              store.setup
              puts "Created queues configuration file: #{config.path}"
            else
              puts "Found existing queues configuration file: #{config.path}"
              puts "No changes made"
            end
          when 'mysql'
            host, username, password, database = args[2], args[3], args[4], args[5]
            raise InvalidUsage unless host && database && username && password
            conn = { "host"=>host, "username"=>username, "password"=>password, "database"=>database }
            store = MessageStore::MySQL.new(conn, nil)
            config.store = store.configuration
            if config.create_if_none
              puts "Created queues configuration file: #{config.path}"
              if store.setup
                puts "Created queue manager tables in database '#{database}'"
              end
            else
              puts "Found existing queues configuration file: #{config.path}"
              puts "No changes made"
            end
          else
            raise InvalidUsage
          end

        when 'list'
          queue = args[1]
          raise InvalidUsage unless queue
          begin
            qm = queue_manager(config_file)
            list = qm.list(:queue=>queue)
            puts "Found #{list.size} messages in queue #{queue}"
            list.each do |headers|
              puts "Message #{headers[:id]}"
              headers.each do |name, value|
                unless name==:id
                  case value
                  when String
                    value = value[0..MAX_STRING - 3] << "..." if value.length > MAX_STRING
                    value = '"' << value.gsub('"', '\"') << '"'
                  when Symbol
                    value = ":#{value.to_s}"
                  end
                  puts "  :#{name} => #{value}"
                end
              end
            end
          rescue DRb::DRbConnError =>error
            puts "Cannot access queue manager: is it running?"
          end

        when 'empty'
          queue = args[1]
          raise InvalidUsage unless queue
          begin
            qm = queue_manager(config_file)
            while msg = qm.dequeue(:queue=>queue)
            end
          rescue DRb::DRbConnError =>error
            puts "Cannot access queue manager: is it running?"
          end

        else
          raise InvalidUsage
      end
      rescue InvalidUsage
        puts USAGE
      end
    end

    def queue_manager(config_file)
      if config_file
        config = Config.new config_file, nil
        unless config.load_no_create || config_file.nil?
          puts "Could not find configuration file #{config.path}"
          exit
        end
        drb = Config::DEFAULT_DRB
        drb.merge(config.drb) if config.drb
        drb_uri = "druby://localhost:#{drb['port']}"
      else
        drb_uri = Queue::DEFAULT_DRB_URI
      end
      DRbObject.new(nil, drb_uri)
    end

  end

end

if __FILE__ == $0
  ReliableMsg::CLI.new.run
end
