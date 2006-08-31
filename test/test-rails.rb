#
# = test-rails.rb - Rails integration test cases
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

class TestRails < Test::Unit::TestCase

    begin
        class TestController < ActionController::Base
            queue :queue1
            queue :queue2, "queue2"
            queue "queue3"

            topic :topic1
            topic :topic2, "topic2"
            topic "topic3"
        end


        def setup
            @controller = TestController.new
        end

        def test_queue
            queue = @controller.queue1
            assert queue.instance_of?(ReliableMsg::Queue) && queue.name == "queue1", "Queue1 not set propertly"
            queue = @controller.queue2
            assert queue.instance_of?(ReliableMsg::Queue) && queue.name == "queue2", "Queue2 not set propertly"
            queue = @controller.queue
            assert queue.instance_of?(ReliableMsg::Queue) && queue.name == "queue3", "Queue3 not set propertly"
        end

        def test_topic
            topic = @controller.topic1
            assert topic.instance_of?(ReliableMsg::Topic) && topic.name == "topic1", "Topic1 not set propertly"
            topic = @controller.topic2
            assert topic.instance_of?(ReliableMsg::Topic) && topic.name == "topic2", "Topic2 not set propertly"
            topic = @controller.topic
            assert topic.instance_of?(ReliableMsg::Topic) && topic.name == "topic3", "Topic3 not set propertly"
        end
    rescue NameError => error
        raise error unless error.name == :ActionController

        def test_nothing
            puts "ActionController not found: Rails integration test not run"
        end
    end

end

