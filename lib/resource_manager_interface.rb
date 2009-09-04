####################################
####
#    David Austin - ITMAT @ UPENN
#    Interfaces to a remote resource manager via SQS messages
#    Messages are encoded with JSON
#

require 'rubygems'
require 'json'
require 'net/http'

META_URL = 'http://169.254.169.254/latest/meta-data/instance-id'


class ResourceManagerInterface 
  
  attr_reader :instance_id

  #calls get
  def initialize(sqs, q)
    
    @sqs = sqs
    @queue = sqs.queue(q)
    @instance_id = get_instance_id
  end

  def get_instance_id
    #fetches instance id from AWS meta services
 #   begin
      # resp = Net::HTTP.get_response(URI.parse(META_URL))
      # data = resp.body

  #  rescue
      data = ALT_INSTANCE_ID
   # end
    return data
    
  end


  def send(string, timeout = nil)
    #puts string and instance ID and timeout and timestamp in a hash
    h = { :instance_id => @instance_id, :status => string, 
      :timestamp => Time.new.strftime("%Y%m%d%H%M%S")}
    h['timeout'] = timeout unless timeout.nil?
    #encode in JSON and send off to queue
    @queue.push(h.to_json)

  end

  
  def method_missing(method)
    if method.to_s.match(/send_(.+)/)
      send($1)
    else
      raise NoMethodError
    end
  end

end
