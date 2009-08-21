# Change this file to be a wrapper around your daemon code.
require 'rubygems'
require 'right_aws'
require 'openwfe'
require 'openwfe/extras/listeners/sqs_gen2_listeners'
require 'openwfe/extras/participants/sqs_gen2_participants'
require 'work_item_helper'

# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  # Trap signals with blocks or procs
  config.trap( 'INT' ) do
  #   # do something clever
  end
  config.trap( 'TERM', Proc.new { puts 'Going down' } )
end

#use right_aws to get a message for a few seconds, then update visibility based on params

sqs = RightAws::SqsGen2.new(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

loop do
  DaemonKit.logger.info "Checking queue #{QUEUE_NAME}..."

  q = sqs.queue(QUEUE_NAME)
  
  #peek at message
  
  m = q.receive(VIS_PEEK)
  
  if m.nil?
    #exit
    DaemonKit.logger.info "No Messages in SQS."
    DaemonKit.logger.info "Sleeping for #{SLEEP_TIME} seconds..."
    sleep SLEEP_TIME
    next
  end
  
  # puts m.to_s #debug
  
  #build WI from message, and then update visibility
  
  wi = WorkItemHelper.decode_message(m.to_s)
  
  puts "DEBUG..."
  puts "Work Item has #{wi.attributes.keys.size} attributes:"
  
  wi.attributes.keys.each do |k|
    
    puts "#{k} => #{wi[k]}"  
    
  end
  
  puts "Adjusting SQS timeout: #{wi.params['sqs_timeout'] ||= VIS_DEFAULT}"
  
  m.visibility = wi.params['sqs_timeout'] ||= VIS_DEFAULT
  
  #now we run the command based on the params, and store it's output as wi.text
  
  cwd = wi.params['cwd'] ||= Dir.getwd
  
  puts "Running Command #{wi.params['command']} in #{cwd}..."
  
  Dir.chdir(cwd) do 
    
    pipe = IO.popen( wi.params['command'] )
    if wi.has_attribute?('text')
      wi.text += pipe.readlines
    else
      wi.text = pipe.readlines
    end
  end
  
  #once method is finished, encode work item and place it in QUEUE_NAME_FIN queue
  
  puts "Finished running command.  WI now has output: "
  puts "#{wi.text}"
  
  q_fin = sqs.queue(wi.reply_queue)
  
  m_fin = WorkItemHelper.encode_workitem(wi)
  
  puts "Pushing WI onto queue: #{q_fin.name}..."
  
  q_fin.push(m_fin)
  
  #clean up and remove message from initial queue
  
  puts "Deleting original message"
  
  m.delete
  
  DaemonKit.logger.info "Sleeping for #{SLEEP_TIME} seconds..."
  sleep SLEEP_TIME
end
