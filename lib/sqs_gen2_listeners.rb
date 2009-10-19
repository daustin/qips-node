#--
# Copyright (c) 2007-2009, John Mettraux, jmettraux@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Made in Japan.
#++


require 'yaml'
require 'base64'
require 'thread'

require 'rubygems'
require 'right_aws'
require 'openwfe/service'
require 'openwfe/listeners/listener'

# require 'rufus/sqs' # gem 'rufus-sqs'


module OpenWFE
module Extras

  #
  # Polls an Amazon SQS queue for workitems
  #
  # Workitems can be instances of InFlowWorkItem or LaunchItem.
  #
  #   require 'openwfe/extras/listeners/sqslisteners'
  #
  #   note that application_context must contain aws_access_key_id and 
  #   aws_secret_access_key in order to connect to aws:
  #
  #   ql = OpenWFE::SqsGen2Listener("workqueue1", engine.application_context.merge({:aws_access_key_id => 'KEYKEY', :aws_secret_access_key => 'SIudfhsidu'} ))
  #
  #   engine.add_workitem_listener(ql, "2m30s")
  #     #
  #     # thus, the engine will poll our "workqueue1" SQS queue
  #     # every 2 minutes and 30 seconds
  #
  class SqsGen2Listener < Service

    include WorkItemListener
   
    #
    # The name of the Amazon SQS whom this listener cares for, aws stuff
    #
    attr_reader :queue_name, :aws_access_key_id, :aws_secret_access_key, :pid

    def initialize (service_name, opts)

      @mutex = Mutex.new

      @queue_name = opts[:queue_name] || service_name
      @aws_access_key_id = opts[:aws_access_key_id]
      @aws_secret_access_key = opts[:aws_secret_access_key]
      @pid = opts[:pid]

      super(service_name, opts)

      linfo { "new() queue is '#{@queue_name}'" }
    end

    #
    # polls the SQS for incoming messages
    #
    def trigger (params)
      @mutex.synchronize do
        # making sure executions do not overlap

        linfo {"connecting to AWS..."}

        sqs = RightAws::SqsGen2.new(@aws_access_key_id, @aws_secret_access_key)

        q = sqs.queue(@queue_name)
          # just to be sure it is there

        loop do

          l = q.receive_messages(10,0)
          linfo "got #{l.length} messages."
          break if l.length < 1

          l.each do |msg|
            
            o = decode_message(msg.to_s)
            #now we look for our special pid
            if o.pid == @pid
              handle_item(o)
              msg.delete
              linfo { "trigger() handled successfully msg #{msg.message_id}" }
              break
            end
          end
        end
      end
    end

    #
    # Extracts a workitem from the message's body.
    #
    # By default, this listeners assumes the workitem is stored in
    # its "hash form" (not directly as a Ruby InFlowWorkItem instance).
    #
    # LaunchItem instances (as hash as well) are also accepted.
    #
    def decode_message (message)

      o = Base64.decode64(message)
      o = YAML.load(o)
      o = OpenWFE::workitem_from_h(o)
      o
    end
  end

end
end

