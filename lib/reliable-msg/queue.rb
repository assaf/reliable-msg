#
# = queue.rb - Reliable queue client API
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require "drb"
require "reliable-msg/client"
require "reliable-msg/selector"


module ReliableMsg

  # == Queue client API
  #
  # Use the Queue object to put messages in queues, or get messages from queues.
  #
  # You can create a Queue object that connects to a single queue by passing the
  # queue name to the initialized. You can also access other queues by specifying
  # the destination queue when putting a message.
  #
  # For example:
  #   queue = Queue.new 'my-queue'
  #   # Put a message in the queue with priority 2, expiring in 30 seconds.
  #   msg = 'lorem ipsum'
  #   mid = queue.put msg, :priority=>2, :expires=>30
  #   # Retrieve and process a message from the queue.
  #   queue.get do |msg|
  #     if msg.id == mid
  #       print "Retrieved same message"
  #     end
  #     print "Message text: #{msg.object}"
  #   end
  #
  # See Queue.get and Queue.put for more examples.
  class Queue < Client

    # Caches queue headers locally. Used by queues that retrieve a list of
    # headers for their selectors, and can be shared by queue/selector
    # objects operating on the same queue.
    @@headers_cache = {} #:nodoc:

    # Default number of delivery attempts.
    DEFAULT_MAX_DELIVERIES = 5;

    INIT_OPTIONS = [:expires, :delivery, :priority, :max_deliveries, :drb_uri, :tx_timeout, :connect_count]

    # The optional argument +queue+ specifies the queue name. The application can
    # still put messages in other queues by specifying the destination queue
    # name in the header.
    #
    # The following options can be passed to the initializer:
    # * <tt>:expires</tt> -- Message expiration in seconds. Default for new messages.
    # * <tt>:delivery</tt> -- The message delivery mode. Default for new messages.
    # * <tt>:priority</tt> -- The message priority. Default for new messages.
    # * <tt>:max_deliveries</tt> -- Maximum number of attempts to deliver message.
    #   Default for new messages.
    # * <tt>:drb_uri</tt> -- DRb URI for connecting to the queue manager. Only
    #   required when using a remote queue manager, or different port.
    # * <tt>:tx_timeout</tt> -- Transaction timeout. See tx_timeout.
    # * <tt>:connect_count</tt> -- Connection attempts. See connect_count.
    #
    # :call-seq:
    #   Queue.new([name [,options]])    -> queue
    #
    def initialize(queue = nil, options = nil)
      options.each do |name, value|
        raise RuntimeError, format(ERROR_INVALID_OPTION, name) unless INIT_OPTIONS.include?(name)
        instance_variable_set "@#{name.to_s}".to_sym, value
      end if options
      @queue = queue
    end

    # Put a message in the queue.
    #
    # The +message+ argument is required, but may be +nil+
    #
    # Headers are optional. Headers are used to provide the application with additional
    # information about the message, and can be used to retrieve messages (see Queue.get
    # for discussion of selectors). Some headers are used to handle message processing
    # internally (e.g. <tt>:priority</tt>, <tt>:expires</tt>).
    #
    # Each header uses a symbol for its name. The value may be string, numeric, true/false
    # or nil. No other objects are allowed. To improve performance, keep headers as small
    # as possible.
    #
    # The following headers have special meaning:
    # * <tt>:delivery</tt> -- The message delivery mode.
    # * <tt>:queue</tt> -- Puts the message in the named queue. Otherwise, uses the queue
    #   specified when creating this Queue object.
    # * <tt>:priority</tt> -- The message priority. Messages with higher priority are
    #   retrieved first.
    # * <tt>:expires</tt> -- Message expiration in seconds. Messages do not expire unless
    #   specified. Zero or +nil+ means no expiration.
    # * <tt>:expires_at</tt> -- Specifies when the message expires (timestamp). Alternative
    #   to <tt>:expires</tt>.
    # * <tt>:max_deliveries</tt> -- Maximum number of attempts to deliver message, afterwhich
    #   message moves to the DLQ. Minimum is 1 (deliver only once), default is 5 (deliver
    #   up to 5 times).
    #
    # Headers can be set on a per-queue basis when the Queue is created. This only affects
    # messages put through that Queue object.
    #
    # Messages can be delivered using one of three delivery modes:
    # * <tt>:best_effort</tt> -- Attempt to deliver the message once. If the message expires or
    #   cannot be delivered, discard the message. The is the default delivery mode.
    # * <tt>:repeated</tt> -- Attempt to deliver until message expires, or up to maximum
    #   delivery attempts (see <tt>:max_deliveries</tt>). Afterwards, move message to
    #   dead-letter queue.
    # * <tt>:once</tt> -- Attempt to deliver message exactly once. If message expires, or
    #   first delivery attempt fails, move message to dead-letter queue.
    #
    # For example:
    #   queue.put request
    #   queue.put notice, :expires=>10
    #   queue.put object, :queue=>'other-queue'
    #
    # :call-seq:
    #   queue.put(message[, headers]) -> id
    #
    def put(message, headers = nil)
      tx = Thread.current[THREAD_CURRENT_TX]
      # Use headers supplied by callers, or defaults for this queue.
      defaults = {
        :priority => @priority || 0,
        :expires => @expires,
        :max_deliveries => @max_deliveries || DEFAULT_MAX_DELIVERIES,
        :delivery => @delivery || :best_effort
      }
      headers = headers ? defaults.merge(headers) : defaults
      # Serialize the message before sending to queue manager. We need the
      # message to be serialized for storage, this just saves duplicate
      # serialization when using DRb.
      message = Marshal::dump message
      # If inside a transaction, always send to the same queue manager, otherwise,
      # allow repeated() to try and access multiple queue managers.
      if tx
        return tx[:qm].enqueue(:message=>message, :headers=>headers, :queue=>(headers[:queue] || @queue), :tid=>tx[:tid])
      else
        return repeated { |qm| qm.enqueue :message=>message, :headers=>headers, :queue=>(headers[:queue] || @queue) }
      end
    end

    # Get a message from the queue.
    #
    # Call with no arguments to retrieve the next message in the queue. Call with a message
    # identifier to retrieve that message. Call with selectors to retrieve the first message
    # that matches.
    #
    # Selectors specify which headers to match. For example, to retrieve all messages in the
    # with priority 2:
    #   msg = queue.get :priority=>2
    # To put and get the same message:
    #   mid = queue.put obj
    #   msg = queue.get mid # or queue.get :id=>mid
    #   assert(msg.obj == obj)
    #
    # More complex selector expressions can be generated using Queue.selector. For example,
    # to retrieve the next message with priority 2 or higher, created in the last 60 seconds:
    #   selector = Queue.selector { priority >= 2 && created > now - 60 }
    #   msg = queue.get selector
    #
    # The following headers have special meaning:
    # * <tt>:id</tt> -- The message identifier.
    # * <tt>:queue</tt> -- Select a message originally delivered to the named queue. Only used
    #   when retrieving messages from the dead-letter queue.
    # * <tt>:redelivery</tt> -- Specifies the re-delivery count for this message. Nil if the
    #   message is delivered (get) for the first time, one on the first attempt to re-deliver,
    #   and incremented once for each subsequent attempt.
    # * <tt>:created</tt> -- Indicates timestamp (in seconds) when the message was created.
    # * <tt>:expires_at</tt> -- Indicates timestamp (in seconds) when the message will expire,
    #   +nil+ if the message does not expire.
    #
    # Call this method without a block to return the message. The returned object is of type
    # Message, or +nil+ if no message is found.
    #
    # Call this method in a block to retrieve and process the message. The block is called with
    # the Message object, returning the result of the block. Returns +nil+ if no message is found.
    #
    # All operations performed on the queue inside the block are part of the same transaction.
    # The transaction commits when the block completes. However, if the block raises an exception,
    # the transaction aborts: the message along with any message retrieved through that Queue object
    # are returned to the queue; messages put through that Queue object are discarded. You cannot
    # put and get the same message inside a transaction.
    #
    # For example:
    #   queue.put obj
    #   while queue.get do |msg|  # called for each message in the queue,
    #                             # until the queue is empty
    #     ... do something with msg ...
    #     queue.put obj           # puts another message in the queue
    #     true
    #   end
    # This loop will only complete if it raises an exception, since it gets one message from
    # the queue and puts another message in its place. After an exception, there will be at
    # least one message in the queue.
    #
    # Each attempt to process a message increases its retry count. When the retry count
    # (<tt>:retry</tt>) reaches the maximum allowed (<tt>:max_retry</tt>), the message is
    # moved to the dead-letter queue.
    #
    # This method does not block and returns immediately if there is no message in the queue.
    # To continue processing all messages in the queue:
    #   while true  # repeat forever
    #     while true
    #       break unless queue.get do |msg|
    #         ... do something with msg ...
    #         true
    #       end
    #     end
    #     sleep 5     # no messages, wait
    #   end
    #
    # :call-seq:
    #   queue.get([selector]) -> msg or nil
    #   queue.get([selector]) {|msg| ... } -> obj
    #
    # See: Message
    #
    def get(selector = nil, &block)
      tx = old_tx = Thread.current[THREAD_CURRENT_TX]
      # If block, begin a new transaction.
      if block
        tx = {:qm=>qm}
        tx[:tid] = tx[:qm].begin tx_timeout
        Thread.current[THREAD_CURRENT_TX] = tx
      end
      result = begin
        # Validate the selector: nil, string or hash.
        selector = case selector
          when String
            {:id=>selector}
          when Hash, Selector, nil
            selector
          else
            raise ArgumentError, ERROR_INVALID_SELECTOR
        end
        # If using selector object, obtain a list of all message headers
        # for the queue (shared by all Queue/Selector objects accessing
        # the same queue) and run the selector on that list. Pick one
        # message and switch to an :id selector to retrieve it.
        if selector.is_a?(Selector)
          cached = @@headers_cache[@queue] ||= CachedHeaders.new
          id = cached.next(selector) do
            if tx
              tx[:qm].list :queue=>@queue, :tid=>tx[:tid]
            else
              repeated { |qm| qm.list :queue=>@queue }
            end
          end
          return nil unless id
          selector = {:id=>id}
        end
        # If inside a transaction, always retrieve from the same queue manager,
        # otherwise, allow repeated() to try and access multiple queue managers.
        message = if tx
          tx[:qm].dequeue :queue=>@queue, :selector=>selector, :tid=>tx[:tid]
        else
          repeated { |qm| qm.dequeue :queue=>@queue, :selector=>selector }
        end
        # Result is either message, or result from processing block. Note that
        # calling block may raise an exception. We deserialize the message here
        # for two reasons:
        # 1. It creates a distinct copy, so changing the message object and returning
        #    it to the queue (abort) does not affect other consumers.
        # 2. The message may rely on classes known to the client but not available
        #    to the queue manager.
        result = if message
          message = Message.new(message[:id], message[:headers], Marshal::load(message[:message]))
          block ? block.call(message) : message
        end
      rescue Exception=>error
        # Abort the transaction if we started it. Propagate error.
        qm.abort(tx[:tid]) if block
        raise error
      ensure
        # Resume the old transaction.
        Thread.current[THREAD_CURRENT_TX] = old_tx if block
      end
      # Commit the transaction and return the result. We do this outside the main
      # block, since we don't abort in case of error (commit is one-phase) and we
      # don't retain the transaction association, it completes by definition.
      qm.commit(tx[:tid]) if block
      result
    end

    # Returns the queue name.
    def name
      @queue
    end

  end

  # Locally cached headers for a queue. Used with the Selector object to
  # retrieve the headers once, and share them. This effectively acts as a
  # cursor into the queue, and saves I/O by retrieving a new list only
  # when it's empty.
  class CachedHeaders #:nodoc:

    def initialize
      @list = nil
      @mutex = Mutex.new
    end

    # Find the next matching message in the queue based on the
    # selector. The argument is a Selector object which filters out
    # messages. The block is called to load a list of headers from
    # the queue manager, returning an Array of headers (Hash).
    # Returns the identifier of the first message found.
    #
    # :call-seq:
    #   obj.next(selector) { } -> id or nil
    #
    def next selector, &block
      load = false
      @mutex.synchronize do
        load ||= (@list.nil? || @list.empty?)
        @list = block.call() if load
        @list.each_with_index do |headers, idx|
          if selector.match headers
            @list.delete_at idx
            return headers[:id]
          end
        end
        unless load
          load = true
          retry
        end
      end
      return nil
    end

  end

end

