#
# = message-store.rb - Queue manager storage adapters
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require "thread"
require "reliable-msg/client"

module ReliableMsg

  module MessageStore

    ERROR_INVALID_MESSAGE_STORE = "No message store '%s' available (note: case is not important)" #:nodoc:

    # Base class for message store.
    class Base

      ERROR_INVALID_MESSAGE_STORE = "No message store '%s' available (note: case is not important)" #:nodoc:

      @@stores = {} #:nodoc:

      def initialize(logger)
        @logger = logger
      end

      # Returns the message store type name.
      #
      # :call-seq:
      #   store.type -> string
      #
      def type
        raise RuntimeException, "Not implemented"
      end

      # Set up the message store. Create files, database tables, etc.
      #
      # :call-seq:
      #   store.setup
      #
      def setup()
      end

      # Returns the message store configuration as a hash.
      #
      # :call-seq:
      #   store.configuration -> hash
      #
      def configuration
        raise RuntimeException, "Not implemented"
      end

      # Activates the message store. Call this method before using the
      # message store.
      #
      # :call-seq:
      #   store.activate
      #
      def activate()
        @mutex = Mutex.new
        @queues = {Queue::DLQ=>[]}
        @topics = {}
        @cache = {}
        # TODO: add recovery logic
      end

      # Deactivates the message store. Call this method when done using
      # the message store.
      #
      # :call-seq:
      #   store.deactivate
      #
      def deactivate()
        @mutex = @queues = @topics = @cache = nil
      end

      def transaction(&block)
        result = block.call inserts = [], deletes = [], dlqs= []
        begin
          update inserts, deletes, dlqs unless inserts.empty? && deletes.empty? && dlqs.empty?
        rescue Exception=>error
          @logger.error error
          # If an error occurs, the queue may be out of synch with the store.
          # Empty the cache and reload the queue, before raising the error.
          @cache = {}
          @queues = {Queue::DLQ=>[]}
          @topics = {}
          load_index
          raise error
        end
        result
      end

      def get_message(queue, &block)
        messages = @queues[queue]
        return nil unless messages
        messages.each do |headers|
          if block.call(headers)
            id = headers[:id]
            message = @cache[id] || load(id, :queue, queue)
            return {:id=>id, :headers=>headers, :message=>message}
          end
        end
        return nil
      end

      def get_headers(queue)
        return @queues[queue] || []
      end

      def get_last(topic, seen, &block)
        headers = @topics[topic]
        return nil if headers.nil? || headers[:id] == seen
        if block.call(headers)
          id = headers[:id]
          message = @cache[id] || load(id, :topic, topic)
          {:id=>id, :headers=>headers, :message=>message}
        end
      end

      # Returns a message store from the specified configuration (previously
      # created with configure).
      #
      # :call-seq:
      #   Base::configure(config, logger) -> store
      #
      def self.configure(config, logger)
        type = config["type"].downcase
        cls = @@stores[type]
        raise RuntimeError, format(ERROR_INVALID_MESSAGE_STORE, type) unless cls
        cls.new config, logger
      end

    protected

      def update(inserts, deletes, dlqs)
        @mutex.synchronize do
          inserts.each do |insert|
            if insert[:queue]
              queue = @queues[insert[:queue]] ||= []
              headers = insert[:headers]
              # Add element based on priority, higher priority comes first.
              priority = headers[:priority]
              added = false
              queue.each_index do |idx|
                if queue[idx][:priority] < priority
                  queue[idx, 0] = headers
                  added = true
                  break
                end
              end
              queue << headers unless added
              @cache[insert[:id]] = insert[:message]
            elsif insert[:topic]
              @topics[insert[:topic]] = insert[:headers]
            end
          end
          deletes.each do |delete|
            if delete[:queue]
              queue = @queues[delete[:queue]]
              id = delete[:id]
              queue.delete_if { |headers| headers[:id] == id }
              @cache.delete id
            elsif delete[:topic]
              @topics.delete delete[:topic]
            end
          end
          dlqs.each do |dlq|
            queue = @queues[dlq[:queue]]
            id = dlq[:id]
            queue.delete_if do |headers|
              if headers[:id] == id
                @queues[Queue::DLQ] << headers
                true
              end
            end
            @cache.delete id
          end
        end
      end

    end

    class Disk < Base #:nodoc:

      TYPE = self.name.split('::').last.downcase

      @@stores[TYPE] = self

      # Default path where index and messages are stored.
      DEFAULT_PATH = 'queues'

      # Maximum number of open files.
      MAX_OPEN_FILES = 20

      DEFAULT_CONFIG = {
        "type"=>TYPE,
        "path"=>DEFAULT_PATH
      }

      ERROR_PATH_NOT_DIR        = "The path %s is not a directory" #:nodoc:
      ERROR_FAILED_WRITE_MASTER = "Cannot write to master index file %s" #:nodoc:

      def initialize(config, logger)
        super logger
        @fsync = config['fsync']
        # file_map maps messages (by ID) to files. The value is a two-item array: the file
        # name and, if opened, the File object. file_free keeps a list of all currently
        # unused files, using the same two-item arrays.
        @file_map = {}
        @file_free = []
        # Make sure the path points to the queue directory, and the master index is writeable.
        @path = File.expand_path(config['path'] || DEFAULT_PATH)
      end

      def type()
        TYPE
      end

      def setup()
        if File.exist?(@path)
          raise RuntimeError, format(ERROR_PATH_NOT_DIR, @path) unless File.directory?(@path)
          false
        else
          Dir.mkdir @path
          true
        end
      end

      def configuration()
        { "type"=>TYPE, "path"=>@path }
      end

      def activate()
        super
        Dir.mkdir @path unless File.exist?(@path)
        raise RuntimeError, format(ERROR_PATH_NOT_DIR, @path) unless File.directory?(@path)
        index = "#{@path}/master.idx"
        if File.exist? index
          raise RuntimeError, format(ERROR_FAILED_WRITE_MASTER, index) unless File.writable?(index)
          @file = File.open index, "r+"
          @file.flock File::LOCK_EX
          @file.binmode # Things break if you forget binmode on Windows.
          load_index
        else
          @file = File.open index, "w"
          @file.flock File::LOCK_EX
          @file.binmode # Things break if you forget binmode on Windows.
          @last_block = @last_block_end = 8
          # Save. This just prevents us from starting with an empty file, and to
          # enable load_index().
          update [], [], []
        end
      end

      def deactivate()
        @file.flock File::LOCK_UN
        @file.close
        @file_map.each_pair do |id, map|
          map[1].close if map[1]
        end
        @file_free.each do |map|
          map[1].close if map[1]
        end
        @file_map = @file_free = nil
        super
      end

    protected

      def update(inserts, deletes, dlqs)
        inserts.each do |insert|
          # Get an available open file, if none available, create a new file.
          # This allows us to reuse previously opened files that no longer hold
          # any referenced message to store new messages. The File object exists
          # if the file was opened before, otherwise, we need to open it again.
          free = @mutex.synchronize { @file_free.shift }
          name = free ? free[0] : "#{@path}/#{UUID.new}.msg"
          file = if free && free[1]
            free[1]
          else
            file = File.open name, "w+"
            file.binmode
          end
          # Store the message in the file, map the message to the file
          # (message and file have different IDs).
          file.sysseek 0, IO::SEEK_SET
          file.syswrite insert[:message]
          file.flush
          file.truncate insert[:message].length
          file.flush
          @mutex.synchronize { @file_map[insert[:id]] = [name, file] }
        end
        super
        @save = true
        @mutex.synchronize do
          deletes.each do |delete|
            # Instead of deleting the file, we delete the mapping between the message
            # and file and return the file (name and File) to the free list. But we
            # only keep so many open files.
            if @file_free.length < MAX_OPEN_FILES
              @file_free << @file_map.delete(delete[:id])
            else
              free = @file_map.delete(delete[:id])
              free[1].close
              File.delete free[0]
            end
          end
        end
        @mutex.synchronize do
          if @save
            # Create an image of the index. We need that image in order to determine
            # its length and therefore positioning in the file. The image contains the
            # message map/free file map, but without the File objects.
            file_map = {}
            @file_map.each_pair { |id, name_file| file_map[id] = name_file[0] }
            file_free = @file_free.collect { |name_file| name_file[0] }
            image = Marshal::dump({:queues=>@queues, :topics=>@topics, :file_map=>file_map, :file_free=>file_free})
            length = image.length
            # Determine if we can store the new image before the last one (must have
            # enough space from header), or append it to the end of the last one.
            next_block = length + 16 > @last_block ? @last_block_end : 8
            # Seek to new position in file, write image length followed by image.
            @file.sysseek next_block, IO::SEEK_SET
            @file.syswrite sprintf("%08x", length)
            @file.syswrite image
            # Seek to beginning of file, write position of last block. Flush the
            # updates to the O/S.
            @file.sysseek 0, IO::SEEK_SET
            @file.syswrite sprintf("%08x", next_block)
            #@file.flush
            @file.fsync if @fsync
            # Note: the paranoids add fsync here
            @last_block, @last_block_end = next_block, next_block + length + 8
            @save = false
          end
        end
      end

      def load_index()
        @file.sysseek 0, IO::SEEK_SET
        last_block = @file.sysread(8).hex
        if last_block
          # Seek to last block written, read length of image stored
          # there and read that many bytes into memory, restoring the
          # master index.
          @file.sysseek last_block, IO::SEEK_SET
          length = @file.sysread(8).hex
          # Load the index image and create the queues, topics, file_free
          # and file_map structures.
          image = Marshal::load @file.sysread(length)
          @queues = image[:queues]
          @topics = image[:topics]
          image[:file_free].each { |name| @file_free << [name, nil] }
          image[:file_map].each_pair { |id, name| @file_map[id] = [name, nil] }
          @last_block, @last_block_end = last_block, last_block + length + 8
        end
      end

      def load(id, type, queue)
        # Find the file from the message/file mapping.
        map = @file_map[id]
        return nil unless map # TODO: Error?
        file = if map[1]
          map[1]
        else
          file = File.open map[0], "r+"
          file.binmode
          map[1] = file
        end
        file.sysseek 0, IO::SEEK_SET
        file.sysread file.stat.size
      end

    end

    begin

      # Make sure we have a MySQL library before creating this class,
      # worst case we end up with a disk-based message store. Try the
      # native MySQL library, followed by the Rails MySQL library.
      begin
        require "mysql"
      rescue LoadError
        require "active_record/vendor/mysql"
      end

      class MySQL < Base #:nodoc:

        TYPE = self.name.split('::').last.downcase

        @@stores[TYPE] = self

        # Default prefix for tables in the database.
        DEFAULT_PREFIX = 'reliable_msg_';

        # Reference to an open MySQL connection held in the current thread.
        THREAD_CURRENT_MYSQL = :reliable_msg_mysql #:nodoc:

        def initialize(config, logger)
          super logger
          @config = { :host=>config['host'], :username=>config['username'], :password=>config['password'],
                      :database=>config['database'], :port=>config['port'], :socket=>config['socket'] }
          @prefix = config['prefix'] || DEFAULT_PREFIX
          @queues_table = "#{@prefix}queues"
          @topics_table = "#{@prefix}topics"
        end

        def type()
          TYPE
        end

        def setup()
          mysql = connection
          requires = 2 # Number of tables used by reliable-msg.
          mysql.query "SHOW TABLES" do |result|
            while row = result.fetch_row
              requires -= 1 if row[0] == @queues_table || row[0] == @topics_table
            end
          end
          if requires > 0
            sql = File.open File.join(File.dirname(__FILE__), "mysql.sql"), "r" do |input|
              input.readlines.join
            end
            sql.gsub! DEFAULT_PREFIX, @prefix
            mysql.query sql
            true
          end
        end

        def configuration()
          config = { "type"=>TYPE, "host"=>@config[:host], "username"=>@config[:username],
                     "password"=>@config[:password], "database"=>@config[:database] }
          config["port"] = @config[:port] if @config[:port]
          config["socket"] = @config[:socket] if @config[:socket]
          config["prefix"] = @config[:prefix] if @config[:prefix]
          config
        end

        def activate()
          super
          load_index
        end

        def deactivate()
          Thread.list.each do |thread|
            if conn = thread[THREAD_CURRENT_MYSQL]
              thread[THREAD_CURRENT_MYSQL] = nil
              conn.close
            end
          end
          super
        end


      protected

        def update(inserts, deletes, dlqs)
          mysql = connection
          mysql.query "BEGIN"
          begin
            inserts.each do |insert|
              if insert[:queue]
                mysql.query "INSERT INTO `#{@queues_table}` (id,queue,headers,object) VALUES('#{connection.quote  insert[:id]}','#{connection.quote insert[:queue]}',BINARY '#{connection.quote Marshal::dump(insert[:headers])}',BINARY '#{connection.quote insert[:message]}')"
              else
                mysql.query "REPLACE `#{@topics_table}` (topic,headers,object) VALUES('#{connection.quote insert[:topic]}',BINARY '#{connection.quote Marshal::dump(insert[:headers])}',BINARY '#{connection.quote insert[:message]}')"
              end
            end
            ids = deletes.inject([]) do |array, delete|
              delete[:queue] ? array << "'#{delete[:id]}'" : array
            end
            if !ids.empty?
              mysql.query "DELETE FROM `#{@queues_table}` WHERE id IN (#{ids.join ','})"
            end
            dlqs.each do |dlq|
              mysql.query "UPDATE `#{@queues_table}` SET queue='#{Queue::DLQ}' WHERE id='#{connection.quote dlq[:id]}'"
            end
            mysql.query "COMMIT"
          rescue Exception=>error
            mysql.query "ROLLBACK"
            raise error
          end
          super
        end

        def load_index()
          connection.query "SELECT id,queue,headers FROM `#{@queues_table}`" do |result|
            while row = result.fetch_row
              queue = @queues[row[1]] ||= []
              headers = Marshal::load row[2]
              # Add element based on priority, higher priority comes first.
              priority = headers[:priority]
              added = false
              queue.each_index do |idx|
                if queue[idx][:priority] < priority
                  queue[idx, 0] = headers
                  added = true
                  break
                end
              end
              queue << headers unless added
            end
          end
          connection.query "SELECT topic,headers FROM `#{@topics_table}`" do |result|
            while row = result.fetch_row
              @topics[row[0]] = Marshal::load row[1]
            end
          end
        end

        def load(id, type, queue_or_topic)
          message = nil
          if type == :queue
            connection.query "SELECT object FROM `#{@queues_table}` WHERE id='#{id}'" do |result|
              message = if row = result.fetch_row
                row[0]
              end
            end
          else
            connection.query "SELECT object FROM `#{@topics_table}` WHERE topic='#{queue_or_topic}'" do |result|
              message = if row = result.fetch_row
                row[0]
              end
            end
          end
          message
        end

        def connection
          Thread.current[THREAD_CURRENT_MYSQL] ||= Mysql.new @config[:host], @config[:username], @config[:password],
              @config[:database], @config[:port], @config[:socket]
        end

      end

    rescue LoadError
      # do nothing
    end

  end

end
