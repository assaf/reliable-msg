#
# = selector.rb - Deferred expression evaluation selector
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
# Credit to Jim Weirich who provided the influence. Ideas borrowed from his
# presentation on domain specific languages, and the BlankSlate source code.
#
#--
#++

module ReliableMsg #:nodoc:

  # A message selector is used to retrieve specific messages from the queue
  # by matching the message headers.
  #
  # The selector matches messages by calling the block for each potential
  # message. The block can access (read-only) message headers by calling
  # methods with the same name, or using <tt>[:symbol]</tt>. It returns true
  # if a match is found.
  #
  # The following three examples are equivalent:
  #   selector = Queue::selector { priority > 2 }
  #   selector = queue.selector { priority > 2 }
  #   selector = Selector.new { [:priority] > 2 }
  #
  # The new function is always available and evaluates to the current time
  # (in seconds from the Epoch).
  #
  # This example uses the delivery count and message creation date/time to
  # implement a simple retry with back-out:
  #
  #   MINUTE = 60
  #   HOUR = MINUTE * 60
  #   BACKOUT = [ 5 * MINUTE, HOUR, 4 * HOUR, 12 * HOUR ]
  #
  #   selector = Queue::selector { delivered == 0 || BACKOUT[delivered - 1] + created <= now }
  class Selector

    ERROR_INVALID_SELECTOR_BLOCK = "Selector must be created with a block accepting no arguments" #:nodoc:

    # Create a new selector that evaluates by calling the block.
    #
    # :call-seq:
    #   Selector.new { |headers| ... } -> selector
    #
    def initialize(&block)
      raise ArgumentError, ERROR_INVALID_SELECTOR_BLOCK unless block && block.arity < 1
      @block = block
    end

    # Matches the message headers with the selectors. Returns true
    # if a match is made, false otherwise. May raise an error if
    # there's an error in the expression.
    #
    # :call-seq:
    #   selector.match(headers) -> boolean
    #
    def match(headers) #:nodoc:
      context = EvaluationContext.new headers
      context.instance_eval(&@block)
    end

  end

  class EvaluationContext #:nodoc:

    instance_methods.each { |name| undef_method name unless name =~ /^(__.*__)|instance_eval$/ }

    def initialize(headers)
      @headers = headers
    end

    def now()
      Time.now.to_i
    end

    def [](symbol)
      @headers[symbol]
    end

    def method_missing(symbol, *args, &block)
      raise ArgumentError, "Wrong number of arguments (#{args.length} for 0)" unless args.empty?
      @headers[symbol]
    end

  end

end
