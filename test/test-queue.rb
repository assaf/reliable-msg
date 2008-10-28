#
# = test-queue.rb - Queue API test cases
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Copyright:: Copyright (c) 2005,2006 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require 'test/unit'
require 'reliable-msg'

class TestQueue < Test::Unit::TestCase

  class AbortTransaction < Exception
  end

  def setup
    @queue = ReliableMsg::Queue.new 'test-queue'
    @dlq = ReliableMsg::Queue.new ReliableMsg::Queue::DLQ
    @manager = ReliableMsg::QueueManager.new
    @manager.start
    clear false
    @restart = proc do
      @manager.stop
      @manager.start
    end
  end

  def teardown
    clear true
    @manager.stop
  end

  def test_order
    # Run test case without restrating queue manager (from cache).
    _test_order
    # Run test case by restarting queue manager (test recovery).
    _test_order @restart
  end

  def _test_order restart = nil
    # Put two messages, test that they are retrieved in order.
    id1 = @queue.put 'first test message'
    id2 = @queue.put 'second test message'
    restart.call if restart
    # TODO: order is not really guaranteed here.
    msg = @queue.get
    assert msg && (msg.id == id1 || msg.id == id2), "Failed to retrieve message in order"
    seen = msg.id
    msg = @queue.get
    assert msg && (msg.id == id1 || msg.id == id2), "Failed to retrieve message in order"
    assert msg.id != seen, "Retrieved the same message twice"
    assert @queue.get.nil?, "Phantom message in queue"

    # Put three messages with priority, test that they are retrieved in order.
    id1 = @queue.put 'priority one message', :priority=>1
    id2 = @queue.put 'priority three message', :priority=>3
    id3 = @queue.put 'priority two message', :priority=>2
    restart.call if restart
    msg = @queue.get
    assert msg && msg.id == id2, "Failed to retrieve message in order"
    msg = @queue.get
    assert msg && msg.id == id3, "Failed to retrieve message in order"
    msg = @queue.get
    assert msg && msg.id == id1, "Failed to retrieve message in order"
    assert @queue.get.nil?, "Phantom message in queue"
  end

  def test_selector
    # Run test case without restrating queue manager (from cache).
    _test_selector
    # Run test case by restarting queue manager (test recovery).
    _test_selector @restart
  end

  def _test_selector restart = nil
    # Test that we can retrieve message based on specific header value,
    # contrary to queue order.
    id1 = @queue.put 'first test message', :name=>"foo"
    id2 = @queue.put 'second test message', :name=>"bar"
    restart.call if restart
    msg = @queue.get(ReliableMsg::Queue.selector { name == 'bar' })
    assert msg && msg.id == id2, "Failed to retrieve message by selector"
    msg = @queue.get(ReliableMsg::Queue.selector { name == 'baz' })
    assert msg.nil?, "Retrieved non-existent message" # Tests reloading selector
    msg = @queue.get(ReliableMsg::Queue.selector { name == 'foo' })
    assert msg && msg.id == id1, "Failed to retrieve message by selector"
    msg = @queue.get(ReliableMsg::Queue.selector { name == 'baz' })
    assert msg.nil?, "Retrieved non-existent message" # Tests reloading selector
    assert @queue.get.nil?, "Phantom message in queue"
  end

  def test_non_delivered
    # Run test case without restrating queue manager (from cache).
    _test_non_delivered
    # Run test case by restarting queue manager (test recovery).
    _test_non_delivered @restart
  end

  def _test_non_delivered restart = nil
    # Test that we can receive message that has not yet expired (30 second delay),
    # but cannot receive message that has expires (1 second, we wait for 2), and
    # that message has been moved to the DLQ.
    id1 = @queue.put 'first test message', :expires=>30, :delivery=>:repeated
    id2 = @queue.put 'second test message', :expires=>1, :delivery=>:repeated
    restart.call if restart
    msg = @queue.get :id=>id1
    assert msg, "Failed to retrieve message that did not expire"
    sleep 2
    msg = @queue.get :id=>id2
    assert msg.nil?, "Incorrectly retrieved expired message"
    msg = @dlq.get
    assert msg && msg.id == id2, "Message not moved to DLQ"

    # Test that we can receive message more than once, but once we try more than
    # max_deliveries, the message moves to the DLQ.
    id1 = @queue.put 'test message', :max_deliveries=>2, :delivery=>:repeated
    restart.call if restart
    begin
      @queue.get do |msg|
        assert msg && msg.id == id1, "Block called without the message"
        raise AbortTransaction
      end
        flunk "Message not found in queue, or exception not propagated"
      rescue AbortTransaction
    end
    begin
      @queue.get do |msg|
        assert msg && msg.id == id1, "Block called without the message"
        raise AbortTransaction
      end
      flunk "Message not found in queue, or exception not propagated"
    rescue AbortTransaction
    end
    assert @queue.get.nil?, "Incorrectly retrieved expired message"
    msg = @dlq.get
    assert msg && msg.id == id1, "Message not moved to DLQ"

    # Test that message discarded when delivery mode is best_effort.
    id1 = @queue.put 'test message', :max_deliveries=>1, :delivery=>:best_effort
    restart.call if restart
    begin
      @queue.get do |msg|
        assert msg && msg.id == id1, "Block called without the message"
        raise AbortTransaction
      end
      flunk "Message not found in queue, or exception not propagated"
    rescue AbortTransaction
    end
    assert @queue.get.nil?, "Incorrectly retrieved expired message"
    assert @dlq.get.nil?, "Message incorrectly moved to DLQ"

    # Test that message is moved to DLQ when delivery mode is exactly_once.
    id1 = @queue.put 'test message', :max_deliveries=>2, :delivery=>:once
    restart.call if restart
    begin
      @queue.get do |msg|
        assert msg && msg.id == id1, "Block called without the message"
        assert @dlq.get.nil?, "Message prematurely found in DLQ"
        raise AbortTransaction
      end
      flunk "Message not found in queue, or exception not propagated"
    rescue AbortTransaction
    end
    assert @queue.get.nil?, "Incorrectly retrieved expired message"
    msg = @dlq.get
    assert msg && msg.id == id1, "Message not moved to DLQ"
  end


  def test_backout
    @queue.put "backout test", :delivery=>:repeated
    backout = [ 1, 2, 3, 4 ]
    redelivery = nil
    done = false
    while !done
      selector = ReliableMsg::Queue::selector { redelivery.nil? || backout[redelivery - 1] + created <= now }
      begin
        more = @queue.get selector do |msg|
          assert redelivery == msg.headers[:redelivery], "Unexpected redelivery header"
          redelivery = (redelivery || 0) + 1
          raise "Reject message"
          true
        end
      rescue Exception=>error
        assert error.message == "Reject message", "Unexpected ... exception"
        more = true
      end while more
      while msg = @dlq.get
        done = true
      end
      sleep 2
    end
  end

private

  def clear complain
    # Empty test queue and DLQ.
    while @queue.get
      flunk "Found message in queue during clear" if complain
    end
    while @dlq.get
      flunk "Found message in DLQ queue during clear" if complain
    end
  end

end

