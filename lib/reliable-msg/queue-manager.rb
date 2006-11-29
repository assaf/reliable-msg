#
# = queue-manager.rb - Queue manager
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Documentation:: http://trac.labnotes.org/cgi-bin/trac.cgi/wiki/Ruby/ReliableMessaging
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require "singleton"
require "drb"
require "drb/acl"
require "thread"
require "yaml"
begin
  require "uuid"
rescue LoadError
  require "rubygems"
  require_gem "uuid"
end
require "reliable-msg/client"
require "reliable-msg/message-store"

module ReliableMsg

  class Config #:nodoc:

    CONFIG_FILE = "queues.cfg"

    DEFAULT_STORE = MessageStore::Disk::DEFAULT_CONFIG

    DEFAULT_DRB = {
      "port"=>Client::DRB_PORT,
      "acl"=>"allow localhost"
    }

    INFO_LOADED_CONFIG  = "Loaded queues configuration from: %s" #:nodoc:
    INFO_CREATED_CONFIG = "Created queues configuration file in: %s" #:nodoc:

    def initialize(file, logger = nil)
      @logger = logger
      # If no file specified, attempt to look for file in current directory.
      # If not found in current directory, look for file in Gem directory.
      unless file
        file = if File.exist?(CONFIG_FILE)
          CONFIG_FILE
        else
          file = File.expand_path(File.join(File.dirname(__FILE__), '..'))
          File.basename(file) == 'lib' ? File.join(file, '..', CONFIG_FILE) : File.join(file, CONFIG_FILE)
        end
      end
      @file = File.expand_path(file)
      @config = {}
    end

    def load_no_create()
      if File.exist?(@file)
        @config= {}
        File.open @file, "r" do |input|
          YAML.load_documents input do |doc|
            @config.merge! doc
          end
        end
        true
      end
    end

    def load_or_create()
      if File.exist?(@file)
        @config= {}
        File.open @file, "r" do |input|
          YAML.load_documents input do |doc|
            @config.merge! doc
          end
        end
        @logger.info format(INFO_LOADED_CONFIG, @file)
      else
        @config = {
          "store" => DEFAULT_STORE,
          "drb" => DEFAULT_DRB
        }
        save
        @logger.info format(INFO_CREATED_CONFIG, @file)
      end
    end

    def create_if_none()
      if File.exist?(@file)
        false
      else
        @config = {
          "store" => DEFAULT_STORE,
          "drb" => DEFAULT_DRB
        }.merge(@config)
        save
        true
      end
    end

    def exist?()
      File.exist?(@file)
    end

    def path()
      @file
    end

    def save()
      File.open @file, "w" do |output|
        YAML::dump @config, output
      end
    end

    def method_missing(symbol, *args)
      if symbol.to_s[-1] == ?=
        @config[symbol.to_s[0...-1]] = *args
      else
        @config[symbol.to_s]
      end
    end

  end

  # The QueueManager handles message storage and delivery. Applications connect to the QueueManager
  # either locally or remotely using the client API objects Queue and Topic.
  #
  # You can start a QueueManager from the command line using
  #   queues manager start
  # Or from code using
  #   qm = QueueManager.new
  #   qm.start
  #
  # A Ruby process can only allow one active QueueManager at any given time. Do not run more than
  # one QueueManager connected to the same database or file system storage, as this will cause the
  # queue managers to operate on different messages and queues. Instead, use a single QueueManager
  # and connect to it remotely using DRb.
  #
  # The client API (Queue and Topic) will automatically connect to any QueueManager running in the
  # same Ruby process, or if not found, to a QueueManager running in a different process using DRb.
  class QueueManager

    TX_TIMEOUT_CHECK_EVERY = 30

    ERROR_SEND_MISSING_QUEUE = "You must specify a destination queue for the message" #:nodoc:
    ERROR_RECEIVE_MISSING_QUEUE = "You must specify a queue to retrieve the message from" #:nodoc:
    ERROR_PUBLISH_MISSING_TOPIC = "You must specify a destination topic for the message" #:nodoc:
    ERROR_RETRIEVE_MISSING_TOPIC = "You must specify a topic to retrieve the message from" #:nodoc:
    ERROR_INVALID_HEADER_NAME = "Invalid header '%s': expecting the name to be a symbol, found object of type %s" #:nodoc:
    ERROR_INVALID_HEADER_VALUE = "Invalid header '%s': expecting the value to be %s, found object of type %s" #:nodoc:
    ERROR_NO_TRANSACTION = "Transaction %s has completed, or was aborted" #:nodoc:
    ERROR_QM_STARTED = "Queue manager already started for this process: stop the other queue manager before starting a new one" #:nodoc:
    ERROR_QM_NOT_STARTED = "Queue manager not active" #:nodoc:
    INFO_MESSAGE_STORE = "Using message store: %s" #:nodoc:
    INFO_ACCEPTING_DRB = "Accepting requests at: %s" #:nodoc:
    INFO_QM_STOPPED = "Stopped queue manager at: %s" #:nodoc:
    WARN_TRANSACTION_TIMEOUT = "Timeout: aborting transaction %s" #:nodoc:
    WARN_TRANSACTION_ABORTED = "Transaction %s aborted by client" #:nodoc:

    @@active = nil #:nodoc:

    # Create a new QueueManager with the specified options. Once created, you can
    # start the QueueManager with QueueManager.start.
    #
    # Accepted options are:
    # * <tt>:logger</tt> -- The logger to use. If not specified, will log messages
    #   to STDOUT.
    # * <tt>:config</tt> -- The configuration file to use. If not specified, will
    #   use <tt>queues.cfg</tt>.
    def initialize(options = nil)
      options ||= {}
      # Locks prevent two transactions from seeing the same message. We use a mutex
      # to ensure that each transaction can determine the state of a lock before
      # setting it.
      @mutex = Mutex.new
      @locks = {}
      # Transactions use this hash to hold all inserted messages (:inserts), deleted
      # messages (:deletes) and the transaction timeout (:timeout) until completion.
      @transactions = {}
      @logger = options[:logger] || Logger.new(STDOUT)
      @config = Config.new options[:config], @logger
      @config.load_or_create
    end

    # Starts the QueueManager. This method will block until the QueueManager has
    # successfully started, and raise an exception if the QueueManager fails to start
    # or if another QueueManager was already started in this process.
    def start()
      @mutex.synchronize do
        return if @@active == self
        Thread.critical = true
        if @@active.nil?
          @@active = self
        else
          Thread.critical = false
          raise RuntimeError, ERROR_QM_STARTED
        end
        Thread.critical = false

        begin
          # Get the message store based on the configuration, or default store.
          @store = MessageStore::Base.configure(@config.store || Config::DEFAULT_STORE, @logger)
          @logger.info format(INFO_MESSAGE_STORE, @store.type)
          @store.activate

          # Get the DRb URI from the configuration, or use the default. Create a DRb server.
          drb = Config::DEFAULT_DRB.merge(@config.drb || {})
          drb_uri = "druby://localhost:#{drb['port']}"
          @drb_server = DRb::DRbServer.new drb_uri, self, :tcp_acl=>ACL.new(drb["acl"].split(" "), ACL::ALLOW_DENY)
          @logger.info format(INFO_ACCEPTING_DRB, drb_uri)

          # Create a background thread to stop timed-out transactions.
          @timeout_thread = Thread.new do
            begin
              while true
                time = Time.new.to_i
                @transactions.each_pair do |tid, tx|
                  if tx[:timeout] <= time
                    begin
                      @logger.warn format(WARN_TRANSACTION_TIMEOUT, tid)
                      abort tid
                    rescue
                    end
                  end
                end
                sleep TX_TIMEOUT_CHECK_EVERY
              end
            rescue Exception=>error
              retry
            end
          end

          # Associate this queue manager with the local Queue class, instead of using DRb.
          Client.send :qm=, self
          nil
        rescue Exception=>error
          @@active = nil if @@active == self
          raise error
        end
      end
    end

    # Stops the QueueManager. Once stopped, you can start the same QueueManager again,
    # or start a different QueueManager.
    def stop()
      @mutex.synchronize do
        raise RuntimeError, ERROR_QM_NOT_STARTED unless @@active == self
        # Prevent transactions from timing out while we take down the server.
        @timeout_thread.terminate
        # Shutdown DRb server to prevent new requests from being processed.\
        Client.send :qm=, nil
        drb_uri = @drb_server.uri
        @drb_server.stop_service
        # Deactivate the message store.
        @store.deactivate
        @store = nil
        @drb_server = @store = @timeout_thread = nil
        @logger.info format(INFO_QM_STOPPED, drb_uri)
        @@active = nil
      end
      true
    end

    # Returns true if the QueueManager is receiving remote requests.
    def alive?()
      @drb_server && @drb_server.alive?
    end

    # Called by client to enqueue a message.
    def enqueue(args)
      # Get the arguments of this call.
      message, headers, queue, tid = args[:message], args[:headers], args[:queue].downcase, args[:tid]
      raise ArgumentError, ERROR_SEND_MISSING_QUEUE unless queue && queue.instance_of?(String) && !queue.empty?
      time = Time.new.to_i
      # TODO: change this to support the RM delivery protocol.
      id = args[:id] || UUID.new
      created = args[:created] || time

      # Validate and freeze the headers. The cloning ensures that the headers we hold in memory
      # are not modified by the caller. The validation ensures that the headers we hold in memory
      # can be persisted safely. Basic types like string and integer are allowed, but application types
      # may prevent us from restoring the index. Strings are cloned since strings may be replaced.
      headers = if headers
        copy = {}
        headers.each_pair do |name, value|
          raise ArgumentError, format(ERROR_INVALID_HEADER_NAME, name, name.class) unless name.instance_of?(Symbol)
          case value
            when String, Numeric, Symbol, true, false, nil
              copy[name] = value.freeze
            else
              raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, name, "a string, numeric, symbol, true/false or nil", value.class)
          end
        end
        copy
      else
        {}
      end

      # Set the message headers controlled by the queue.
      headers[:id] = id
      headers[:created] = time
      headers[:delivery] ||= :best_effort
      headers[:max_deliveries] = integer headers[:max_deliveries], 1, Queue::DEFAULT_MAX_DELIVERIES
      headers[:priority] = integer headers[:priority], 0, 0
      if expires_at = headers[:expires_at]
        raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, :expires_at, "an integer", expires_at.class) unless expires_at.is_a?(Integer)
      elsif expires = headers[:expires]
        raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, :expires, "an integer", expires.class) unless expires.is_a?(Integer)
        headers[:expires_at] = Time.now.to_i + expires if expires > 0
      end
      # Create an insertion record for the new message.
      insert = {:id=>id, :queue=>queue, :headers=>headers, :message=>message}
      if tid
        tx = @transactions[tid]
        raise RuntimeError, format(ERROR_NO_TRANSACTION, tid) unless tx
        tx[:inserts] << insert
      else
        @store.transaction do |inserts, deletes, dlqs|
          inserts << insert
        end
      end
      # Return the message identifier.
      id
    end

    # Called by client to list queue headers.
    def list(args)
      # Get the arguments of this call.
      queue = args[:queue].downcase
      raise ArgumentError, ERROR_SEND_MISSING_QUEUE unless queue && queue.instance_of?(String) && !queue.empty?

      return @mutex.synchronize do
        list = @store.get_headers queue
        now = Time.now.to_i
        list.inject([]) do |list, headers|
          if queue != Client::DLQ && ((headers[:expires_at] && headers[:expires_at] < now) || (headers[:redelivery] && headers[:redelivery] >= headers[:max_deliveries]))
            expired = {:id=>headers[:id], :queue=>queue, :headers=>headers}
            if headers[:delivery] == :once || headers[:delivery] == :repeated
              @store.transaction { |inserts, deletes, dlqs| dlqs << expired }
            else # :best_effort
              @store.transaction { |inserts, deletes, dlqs| deletes << expired }
            end
          else
            # Need to clone headers (shallow, values are frozen) when passing in same process.
            list << headers.clone
          end
          list
        end
      end
    end

    # Called by client to dequeue message.
    def dequeue(args)
      # Get the arguments of this call.
      queue, selector, tid = args[:queue].downcase, args[:selector], args[:tid]
      id, headers = nil, nil
      raise ArgumentError, ERROR_RECEIVE_MISSING_QUEUE unless queue && queue.instance_of?(String) && !queue.empty?

      # We need to lock the selected message, before deleting, otherwise,
      # we allow another transaction to see the message we're about to delete.
      # This is true whether we delete the message inside or outside a client
      # transaction. We can wrap everything with a mutex, but it's faster to
      # release the locks mutex as fast as possibe.
      message = @mutex.synchronize do
        message = @store.get_message queue do |headers|
          not @locks.has_key?(headers[:id]) and case selector
            when nil
              true
            when String
              headers[:id] == selector
            when Hash
              selector.all? { |name, value| headers[name] == value }
            else
              raise RuntimeError, "Internal error"
          end
        end
        if message
          @locks[message[:id]] = true
          message
        end
      end
      # Nothing to do if no message found.
      return unless message

      # If the message has expired, or maximum delivery count elapsed, we either
      # discard the message, or send it to the DLQ. Since we're out of a message,
      # we call to get a new one. (This can be changed to repeat instead of recurse).
      headers = message[:headers]
      if queue != Client::DLQ && ((headers[:expires_at] && headers[:expires_at] < Time.now.to_i) || (headers[:redelivery] && headers[:redelivery] >= headers[:max_deliveries]))
        expired = {:id=>message[:id], :queue=>queue, :headers=>headers}
        if headers[:delivery] == :once || headers[:delivery] == :repeated
          @store.transaction { |inserts, deletes, dlqs| dlqs << expired }
        else # :best_effort
          @store.transaction { |inserts, deletes, dlqs| deletes << expired }
        end
        @mutex.synchronize { @locks.delete message[:id] }
        return dequeue(args)
      end

      delete = {:id=>message[:id], :queue=>queue, :headers=>headers}
      begin
        if tid
          tx = @transactions[tid]
          raise RuntimeError, format(ERROR_NO_TRANSACTION, tid) unless tx
          if queue != Client::DLQ && headers[:delivery] == :once
            # Exactly once delivery: immediately move message to DLQ, so if
            # transaction aborts, message is not retrieved again. Do not
            # release lock here, to prevent message retrieved from DLQ.
            # Change delete record so message removed from DLQ on commit.
            @store.transaction { |inserts, deletes, dlqs| dlqs << delete }
            delete[:queue] = Client::DLQ
            tx[:deletes] << delete
          else
            # At most once delivery: delete message if transaction commits.
            # Best effort: we don't need to delete on commit, but it's more
            # efficient this way.
            # Exactly once: message never gets to expire in DLQ.
            tx[:deletes] << delete
          end
        else
          @store.transaction { |inserts, deletes, dlqs| deletes << delete }
          @mutex.synchronize { @locks.delete message[:id] }
        end
      rescue Exception=>error
        # Because errors do happen.
        @mutex.synchronize { @locks.delete message[:id] }
        raise error
      end

      # To prevent a transaction from modifying a message and then returning it to the
      # queue by aborting, we instead clone the message by de-serializing (this happens
      # in Queue, see there). The headers are also cloned (shallow, all values are frozen).
      return :id=>message[:id], :headers=>message[:headers].clone, :message=>message[:message]
    end

    # Called by client to publish message.
    def publish(args)
      # Get the arguments of this call.
      message, headers, topic, tid = args[:message], args[:headers], args[:topic].downcase, args[:tid]
      raise ArgumentError, ERROR_PUBLISH_MISSING_TOPIC unless topic and topic.instance_of?(String) and !topic.empty?
      time = Time.new.to_i
      id = args[:id] || UUID.new
      created = args[:created] || time

      # Validate and freeze the headers. The cloning ensures that the headers we hold in memory
      # are not modified by the caller. The validation ensures that the headers we hold in memory
      # can be persisted safely. Basic types like string and integer are allowed, but application types
      # may prevent us from restoring the index. Strings are cloned since strings may be replaced.
      headers = if headers
        copy = {}
        headers.each_pair do |name, value|
          raise ArgumentError, format(ERROR_INVALID_HEADER_NAME, name, name.class) unless name.instance_of?(Symbol)
          case value
          when String, Numeric, Symbol, true, false, nil
            copy[name] = value.freeze
          else
            raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, name, "a string, numeric, symbol, true/false or nil", value.class)
          end
        end
        copy
      else
        {}
      end

      # Set the message headers controlled by the topic.
      headers[:id] = id
      headers[:created] = time
      if expires_at = headers[:expires_at]
        raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, :expires_at, "an integer", expires_at.class) unless expires_at.is_a?(Integer)
      elsif expires = headers[:expires]
        raise ArgumentError, format(ERROR_INVALID_HEADER_VALUE, :expires, "an integer", expires.class) unless expires.is_a?(Integer)
        headers[:expires_at] = Time.now.to_i + expires if expires > 0
      end
      # Create an insertion record for the new message.
      insert = {:id=>id, :topic=>topic, :headers=>headers, :message=>message}
      if tid
        tx = @transactions[tid]
        raise RuntimeError, format(ERROR_NO_TRANSACTION, tid) unless tx
        tx[:inserts] << insert
      else
        @store.transaction do |inserts, deletes, dlqs|
          inserts << insert
        end
      end
    end

    # Called by client to retrieve message from topic.
    def retrieve(args)
      # Get the arguments of this call.
      seen, topic, selector, tid = args[:seen], args[:topic].downcase, args[:selector], args[:tid]
      id, headers = nil, nil
      raise ArgumentError, ERROR_RETRIEVE_MISSING_TOPIC unless topic && topic.instance_of?(String) && !topic.empty?

      # Very simple, we really only select one message and nothing to lock.
      message = @store.get_last topic, seen do |headers|
        case selector
        when nil
          true
        when Hash
          selector.all? { |name, value| headers[name] == value }
        else
          raise RuntimeError, "Internal error"
        end
      end
      # Nothing to do if no message found.
      return unless message

      # If the message has expired, we discard the message. This being the most recent
      # message on the topic, we simply return nil.
      headers = message[:headers]
      if (headers[:expires_at] && headers[:expires_at] < Time.now.to_i)
        expired = {:id=>message[:id], :topic=>topic, :headers=>headers}
        @store.transaction { |inserts, deletes, dlqs| deletes << expired }
        return nil
      end
      return :id=>message[:id], :headers=>message[:headers].clone, :message=>message[:message]
    end

    # Called by client to begin a transaction.
    def begin(timeout)
      tid = UUID.new
      @transactions[tid] = {:inserts=>[], :deletes=>[], :timeout=>Time.new.to_i + timeout}
      tid
    end

    # Called by client to commit a transaction.
    def commit(tid)
      tx = @transactions[tid]
      raise RuntimeError, format(ERROR_NO_TRANSACTION, tid) unless tx
      begin
        @store.transaction do |inserts, deletes, dlqs|
          inserts.concat tx[:inserts]
          deletes.concat tx[:deletes]
        end
        # Release locks here, otherwise we expose messages before the
        # transaction gets the chance to delete them from the queue.
        @mutex.synchronize do
          tx[:deletes].each { |delete| @locks.delete delete[:id] }
        end
        @transactions.delete tid
      rescue Exception=>error
        abort tid
        raise error
      end
    end

    # Called by client to abort a transaction.
    def abort(tid)
      tx = @transactions[tid]
      raise RuntimeError, format(ERROR_NO_TRANSACTION, tid) unless tx
      # Release locks here because we are no longer in posession of any
      # retrieved messages.
      @mutex.synchronize do
        tx[:deletes].each do |delete|
          @locks.delete delete[:id]
          delete[:headers][:redelivery] = (delete[:headers][:redelivery] || 0) + 1
          # TODO: move to DLQ if delivery count or expires
        end
      end
      @transactions.delete tid
      @logger.warn format(WARN_TRANSACTION_ABORTED, tid)
    end

  private

    def integer(value, minimum, default)
      return default unless value
      value = value.to_i
      value > minimum ? value : minimum
    end

  end

end
