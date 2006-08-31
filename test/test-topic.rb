#
# = test-topic.rb - Topic API test cases
#
# Author:: Assaf Arkin  assaf@labnotes.org
# Documentation:: http://trac.labnotes.org/cgi-bin/trac.cgi/wiki/Ruby/ReliableMessaging
# Copyright:: Copyright (c) 2005 Assaf Arkin
# License:: MIT and/or Creative Commons Attribution-ShareAlike
#
#--
#++

require 'test/unit'
require 'reliable-msg'

class TestTopic < Test::Unit::TestCase

    class AbortTransaction < Exception
    end

    def setup
        @topic = ReliableMsg::Topic.new 'test-topic'
        @manager = ReliableMsg::QueueManager.new
        @manager.start
        @restart = proc do
            @manager.stop
            @manager.start
        end
    end

    def teardown
        @manager.stop
    end

    def test_single
        # Run test case without restrating queue manager (from cache).
        _test_single
        # Run test case by restarting queue manager (test recovery).
        _test_single @restart
    end

    def _test_single restart = nil
        # Put one message, check that we can retrieve it. Check that we
        # can only retrieve it once. Then put another message, repeat.
        msg1 = UUID.new
        msg2 = UUID.new
        @topic.put msg1
        restart.call if restart
        msg = @topic.get
        assert msg && msg.object == msg1, "Failed to retrieve last message on topic"
        assert @topic.get.nil?, "Retrieved last message on topic twice"
        @topic.put msg2
        restart.call if restart
        msg = @topic.get
        assert msg && msg.object == msg2, "Failed to retrieve last message on topic"
        assert @topic.get.nil?, "Retrieved last message on topic twice"
        # Test that someone else can retrieve the message.
        new_topic = ReliableMsg::Topic.new @topic.name
        msg = new_topic.get
        assert msg && msg.object == msg2, "Failed to retrieve last message on topic"
    end

    def test_selector
        # Run test case without restrating queue manager (from cache).
        _test_selector
        # Run test case by restarting queue manager (test recovery).
        _test_selector @restart
    end

    def _test_selector restart = nil
        msg1 = UUID.new
        @topic.put msg1, :name=>"foo"
        restart.call if restart
        msg = @topic.get(ReliableMsg::Queue.selector { name == 'bar' })
        assert msg.nil?, "Retrieve message with non-matching selector"
        msg = @topic.get(ReliableMsg::Queue.selector { name == 'foo' })
        assert msg && msg.object == msg1, "Failed to retrieve message with selector"
    end

    def test_non_expires
        # Run test case without restrating queue manager (from cache).
        _test_non_expires
        # Run test case by restarting queue manager (test recovery).
        _test_non_expires @restart
    end

    def _test_non_expires restart = nil
        # Test that we can receive message that has not yet expired (30 second delay),
        # but cannot receive message that has expires (1 second, we wait for 2).
        msg1 = UUID.new
        msg2 = UUID.new
        @topic.put msg1, :expires=>30
        restart.call if restart
        msg = @topic.get
        assert msg && msg.object == msg1, "Failed to retrieve message"
        @topic.put msg2, :expires=>1
        restart.call if restart
        sleep 2
        msg = @topic.get
        assert msg.nil?, "Incorrectly retrieved expired message"
    end
end

