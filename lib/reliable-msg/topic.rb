#
# = topic.rb - Publish to topic API
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Documentation:: http://trac.labnotes.org/cgi-bin/trac.cgi/wiki/Ruby/ReliableMessaging
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require "drb"
require "reliable-msg/client"
require "reliable-msg/selector"


module ReliableMsg

  # == Pub/Sub Topic API
  #
  # Use the Topic object to publish a message on a topic, get messages from topics.
  #
  # You can create a Topic object that connects to a single topic by passing the
  # topic name to the initialized. You can also access other topics by specifying
  # the destination topic when putting a message.
  #
  # For example:
  #   topic = Topic.new 'my-topic'
  #   # Publish a message on the topic, expiring in 30 seconds.
  #   msg = 'lorem ipsum'
  #   mid = topic.put msg, :expires=>30
  #   # Retrieve and process a message on the topic.
  #   topic.get do |msg|
  #     if msg.id == mid
  #       print "Retrieved same message"
  #     end
  #     print "Message text: #{msg.object}"
  #   end
  #
  # See Topic.get and Topic.put for more examples.
  class Topic < Client

    INIT_OPTIONS = [:expires, :drb_uri, :tx_timeout, :connect_count]

    # The optional argument +topic+ specifies the topic name. The application can
    # still publish messages on other topics by specifying the destination topics
    # name in the header.
    #
    # The following options can be passed to the initializer:
    # * <tt>:expires</tt> -- Message expiration in seconds. Default for new messages.
    # * <tt>:drb_uri</tt> -- DRb URI for connecting to the queue manager. Only
    #   required when using a remote queue manager, or different port.
    # * <tt>:tx_timeout</tt> -- Transaction timeout. See tx_timeout.
    # * <tt>:connect_count</tt> -- Connection attempts. See connect_count.
    #
    # :call-seq:
    #   Topic.new([name [,options]])    -> topic
    #
    def initialize(topic = nil, options = nil)
      options.each do |name, value|
        raise RuntimeError, format(ERROR_INVALID_OPTION, name) unless INIT_OPTIONS.include?(name)
        instance_variable_set "@#{name.to_s}".to_sym, value
      end if options
      @topic = topic
      @seen = nil
    end


    # Publish a message on the topic.
    #
    # The +message+ argument is required, but may be +nil+
    #
    # Headers are optional. Headers are used to provide the application with additional
    # information about the message, and can be used to retrieve messages (see Topic.get
    # for discussion of selectors). Some headers are used to handle message processing
    # internally (e.g. <tt>:expires</tt>).
    #
    # Each header uses a symbol for its name. The value may be string, numeric, true/false
    # or nil. No other objects are allowed. To improve performance, keep headers as small
    # as possible.
    #
    # The following headers have special meaning:
    # * <tt>:topic</tt> -- Publish the onn the named topic. Otherwise, uses the topic
    #   specified when creating this Topic object.
    # * <tt>:expires</tt> -- Message expiration in seconds. Messages do not expire unless
    #   specified. Zero or +nil+ means no expiration.
    # * <tt>:expires_at</tt> -- Specifies when the message expires (timestamp). Alternative
    #   to <tt>:expires</tt>.
    #
    # Headers can be set on a per-topic basis when the Topic is created. This only affects
    # messages put through that Topic object.
    #
    # For example:
    #   topic.put updates
    #   topic.put notice, :expires=>10
    #   topic.put object, :topic=>'other-topic'
    #
    # :call-seq:
    #   topic.put(message[, headers])
    #
    def put(message, headers = nil)
      tx = Thread.current[THREAD_CURRENT_TX]
      # Use headers supplied by callers, or defaults for this topic.
      defaults = { :expires=> @expires }
      headers = headers ? defaults.merge(headers) : defaults
      # Serialize the message before sending to queue manager. We need the
      # message to be serialized for storage, this just saves duplicate
      # serialization when using DRb.
      message = Marshal::dump message
      # If inside a transaction, always send to the same queue manager, otherwise,
      # allow repeated() to try and access multiple queue managers.
      if tx
        tx[:qm].publish(:message=>message, :headers=>headers, :topic=>(headers[:topic] || @topic), :tid=>tx[:tid])
      else
        repeated { |qm| qm.publish :message=>message, :headers=>headers, :topic=>(headers[:topic] || @topic) }
      end
    end

    # Get a message on the topic.
    #
    # Call with no arguments to retrieve the last message published on the topic. Call with
    # selectors to retrieve only matching messages. See also Queue.get.
    #
    # The following headers have special meaning:
    # * <tt>:id</tt> -- The message identifier.
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
    # All operations performed on the topic inside the block are part of the same transaction.
    # See Queue.get for discussion about transactions. Note that retry counts and delivery modes
    # do not apply to Topics. A message remains published on the topic until replaced by another
    # message.
    #
    # :call-seq:
    #   topic.get([selector]) -> msg or nil
    #   topic.get([selector]) {|msg| ... } -> obj
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
        # Validate the selector: nil or hash.
        selector = case selector
          when Hash, Selector, nil
            selector
          else
            raise ArgumentError, ERROR_INVALID_SELECTOR
        end
        # If inside a transaction, always retrieve from the same queue manager,
        # otherwise, allow repeated() to try and access multiple queue managers.
        message = if tx
          tx[:qm].retrieve :seen=>@seen, :topic=>@topic, :selector=>(selector.is_a?(Selector) ? nil : selector), :tid=>tx[:tid]
        else
          repeated { |qm| qm.retrieve :seen=>@seen, :topic=>@topic, :selector=>(selector.is_a?(Selector) ? nil : selector) }
        end
        # Result is either message, or result from processing block. Note that
        # calling block may raise an exception. We deserialize the message here
        # for two reasons:
        # 1. It creates a distinct copy, so changing the message object and returning
        #    it to the queue (abort) does not affect other consumers.
        # 2. The message may rely on classes known to the client but not available
        #    to the queue manager.
        result = if message
          # Do not process message unless selector matches. Do not mark
          # message as seen either, since we may retrieve it if the selector
          # changes.
          if selector.is_a?(Selector)
            return nil unless selector.match message[:headers]
          end
          @seen = message[:id]
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

    # Returns the topic name.
    def name()
      @topic
    end

  end

end

