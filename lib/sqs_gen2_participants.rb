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

require 'rubygems'
require 'right_aws'
# require 'rufus/sqs' # gem 'rufus-sqs'

require 'openwfe/participants/participant'


module OpenWFE
module Extras

  #
  # This participant dispatches its workitem to an Amazon SQS queue.
  #
  # If the queue doesn't exist, the participant will create it.
  #
  # a small example :
  #
  #   # ...
  #   engine.register_participant(:sqs0, SqsGen2Participant.new('AWSKEYHERE', 'SECRETKEYHERE', "workqueue0"))
  #   # ...
  #
  # For more details about SQS :
  # http://aws.amazon.com
  #
  class SqsGen2Participant
    include LocalParticipant

    attr_reader :queue, :sqs, :aws_access_key_id, :aws_secret_access_key

    #
    # Builds an SqsParticipant instance pointing to a given queue.
    # (Refer to the SQS service on how to set up AWS key ids).
    #
    # params is an optional hash to be passed along to the right_aws interface.  see right_aws rdoc for options
    #
    def initialize (aws_access_key_id, aws_secret_access_key, queue_name, params = {})

      @aws_access_key_id = aws_access_key_id
      @aws_secret_access_key = aws_secret_access_key
      @queue_name = queue_name

      @sqs = RightAws::SqsGen2.new(aws_access_key_id, aws_secret_access_key, params)

      @queue = @sqs.queue @queue_name
    end

    #
    # The method called by the engine when it has a workitem for this
    # participant.
    #
    def consume (workitem)

      #first, we specify the reply queue for the 'real' participant. 

      workitem.reply_queue = workitem.params['reply_queue'] ||= @queue_name+"_FIN"
      workitem.pid = workitem.params['pid']

      msg = encode_workitem(workitem)

      message = @queue.send_message(msg)

      ldebug { "consume() msg sent to queue #{@queue.url} id is #{message.id}" }

      if  workitem.params['reply_by_default'] || workitem.params['reply-anyway'] == true
        reply_to_engine( workitem )
      end
     
      
    end

    protected

      #
      # Turns the workitem into a Hash, pass it through YAML and
      # encode64 the result.
      #
      # Override this method as needed.
      #
      # Something of 'text/plain' flavour should be returned.
      #
      def encode_workitem (wi)

        msg = wi.to_h
        msg = YAML.dump(msg)
        msg = Base64.encode64(msg)
        msg
      end
  end

end
end
