require "action_controller"

module ActionController #:nodoc:

  class Base

    # Convenience method for accessing queues from your Rails controller.
    #
    # Use this method in your controller class to create an attribute for accessing
    # the named queue. The method can be called in one of three ways:
    # * With a +Symbol+. Adds the named attribute to access a queue with the same name.
    # * With a +String+. Adds the attribute <tt>queue</tt> to access the named queue.
    # * With a +Symbol+ and a +String+. Adds the named attribute to access the named
    #   queue.
    #
    # For example
    #  queue 'default'
    #
    #  def index
    #    queue.put "some message"
    #    render :text => "added message to queue 'default'"
    #  end
    #
    # :call-seq:
    #   queue symbol
    #   queue symbol, name
    #   queue name
    #
    def self.queue(*args)
      raise ArgumentError, "Expecting a Symbol specifying the attribute name, a String specifying the queue name, or both" unless args.length > 0 && args.length <= 2
      attr = name = nil
      case args[0]
      when String
        raise ArgumentError, "When first argument is a String specifying the queue name, expecting to find only one argument" unless args.length == 1
        attr = :queue
        name = args[0]
      when Symbol
        attr = args[0]
        if args.length == 1
          name = args[0].to_s
        else
          raise ArgumetnError, "When first argument is a Symbol specifying the attribute name, expecting the second argument to be a String specifying the queue name or absent" unless args[1].instance_of?(String)
          name = args[1]
        end
      else
        raise ArgumentError, "Expecting first argument to be a Symbol or a String"
      end
      quoted = "\"" << name.gsub("\"", "\\\"") << "\""

      module_eval(<<-EOS, __FILE__, __LINE__ + 1)
        @@queue_#{attr.to_s} = ReliableMsg::Queue.new(#{quoted})
        def #{attr.to_s}(*args)
          raise ArgumentError, "Attribute #{attr} is accessed without any arguments, e.g. #{attr.to_s}.put(msg)" unless args.length == 0
          @@queue_#{attr.to_s}
        end
      EOS
    end


    # Convenience method for accessing topics from your Rails controller.
    #
    # Use this method in your controller class to create an attribute for accessing
    # the named topic. The method can be called in one of three ways:
    # * With a +Symbol+. Adds the named attribute to access a topic with the same name.
    # * With a +String+. Adds the attribute <tt>topic</tt> to access the named topic.
    # * With a +Symbol+ and a +String+. Adds the named attribute to access the named
    #   topic.
    #
    # For example
    #  topic :notification
    #
    #  def index
    #    :notification.put "something new"
    #    render :text => "added message to topic 'notification'"
    #  end
    #
    # :call-seq:
    #   topic symbol
    #   topic symbol, name
    #   topic name
    #
    def self.topic(*args)
      raise ArgumentError, "Expecting a Symbol specifying the attribute name, a String specifying the topic name, or both" unless args.length > 0 && args.length <= 2
      attr = name = nil
      case args[0]
      when String
        raise ArgumentError, "When first argument is a String specifying the topic name, expecting to find only one argument" unless args.length == 1
        attr = :topic
        name = args[0]
      when Symbol
        attr = args[0]
        if args.length == 1
          name = args[0].to_s
        else
          raise ArgumetnError, "When first argument is a Symbol specifying the attribute name, expecting the second argument to be a String specifying the topic name or absent" unless args[1].instance_of?(String)
          name = args[1]
        end
      else
        raise ArgumentError, "Expecting first argument to be a Symbol or a String"
      end
      quoted = "\"" << name.gsub("\"", "\\\"") << "\""

      module_eval(<<-EOS, __FILE__, __LINE__ + 1)
        @@topic_#{attr.to_s} = ReliableMsg::Topic.new(#{quoted})
        def #{attr.to_s}(*args)
          raise ArgumentError, "Attribute #{attr} is accessed without any arguments, e.g. #{attr.to_s}.put(msg)" unless args.length == 0
          @@topic_#{attr.to_s}
        end
      EOS
    end

  end

end
