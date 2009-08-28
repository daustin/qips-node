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

# get the interfaces

sqs = RightAws::SqsGen2.new(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
s3 = RightAws::S3.new(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

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
  
  #build WI from message, and then update visibility
  
  wi = WorkItemHelper.decode_message(m.to_s)
  q_fin = sqs.queue(wi.reply_queue ||= "#{QUEUE_NAME}_FIN") 
  #now check validity
  if ! WorkItemHelper.validate_workitem(wi)
    #not a valid workitem! reject and pass along to finished queue
    wi.error = "Not a valid workitem for this node!"
    DaemonKit.logger.info "Not a valid workitem for this node!"
    m_fin = WorkItemHelper.encode_workitem(wi)
    
    puts "Pushing WI onto queue: #{q_fin.name}..."
    
    q_fin.push(m_fin)
    
    #clean up and remove message from initial queue
    
    puts "Deleting original message"
    m.delete
    next
  end

  DaemonKit.logger.info "Adjusting SQS timeout: #{wi.params['sqs_timeout'] ||= VIS_DEFAULT}"
  m.visibility = wi.params['sqs_timeout'] ||= VIS_DEFAULT

  #first lets switch to the directory, clean up, 
  # and then start downloading the input files 

  Dir.chdir(WORK_DIR) do 
    # clean directory
    system "rm -rf *"
    # now download new files form bucket

    inbucket_name = wi.params['input_bucket'] ||= wi.prev_output_bucket
    inbucket = s3.bucket(inbucket_name, true)
    keys = inbucket.keys
    #now we filter if applicable
    filter =  wi.params['input_filter'] ||= wi.input_filter ||= '.'

    # infiles keeps track of filenames so we don't upload stuff that's already there
    infiles = Array.new

    keys.each do |k|
      if k.to_s.match(filter)
        DaemonKit.logger.info "Downloading #{k.to_s}..."
        infiles << File.basename(k.to_s)
        File.open(File.basename(k.to_s), "w+") do |f| 
          f.write(k.data)
        end
      end
    end

    #now we run the command based on the params, and store it's output in a file
    DaemonKit.logger.info "Running Command #{wi.params['command']}..."

    pipe = IO.popen( wi.params['command'] )

    File.open("#{wi.params['pid']}_output.txt", "w+") do |f| 
        f.write(pipe.readlines)
    end

    #now lets put the files back into the output bucket
    outbucket_name = wi.params['output_bucket'] ||= inbucket_name
    Dir.glob("*.*") do |f|
      unless infiles.include?(f)
        DaemonKit.logger.info "Uploading #{f}..."
        key = RightAws::S3::Key.create(outbucket_name, f)
        key.put(File.open(f).readlines)
        
      end

    end

    DaemonKit.logger.info "Finished Uploading..."

  end
  
  
  q_fin = sqs.queue(wi.reply_queue)
  
  m_fin = WorkItemHelper.encode_workitem(wi)
  
  DaemonKit.logger.info "Pushing WI onto queue: #{q_fin.name}..."
  
  q_fin.push(m_fin)
  
  #clean up and remove message from initial queue
  
  DaemonKit.logger.info "Deleting original message"
  
  m.delete
  
end
