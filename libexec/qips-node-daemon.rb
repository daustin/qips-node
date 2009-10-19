#############################################
####
#    David Austin - ITMAT @ UPENN     
#    Main Looping Daemon Code
#
#

require 'rubygems'
require 'right_aws'
require 'openwfe'
require 'sqs_gen2_listeners'
require 'sqs_gen2_participants'
require 'work_item_helper'
require 'resource_manager_interface'
require 's3_helper'

# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  # Trap signals with blocks or procs
  config.trap( 'INT' ) do
    # something creative
  end
  config.trap( 'TERM', Proc.new { puts 'Going down' } )
end

# get the interfaces

sqs = RightAws::SqsGen2.new(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
s3 = RightAws::S3.new(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
rmi = ResourceManagerInterface.new(sqs, STATUS_URL)
s3h = S3Helper.new(s3)

DaemonKit.logger.info "Using instance id: #{rmi.instance_id}"
DaemonKit.logger.info "Sending Status Messages to: #{STATUS_URL}"

loop do
  DaemonKit.logger.info "Checking queue #{QUEUE_NAME}..."

  q = sqs.queue(QUEUE_NAME)
  
  #peek at message
  
  m = q.receive(VIS_PEEK)
  
  if m.nil?
    #exit
    DaemonKit.logger.info "No Messages in SQS."
    DaemonKit.logger.info "Sleeping for #{SLEEP_TIME} seconds..."
    # notify rmgr idle
    rmi.send_idle
    sleep SLEEP_TIME
    next
  end
  
  DaemonKit.logger.info "Found a message!"

  #build WI from message, and then update visibility
  
  wi = WorkItemHelper.decode_message(m.to_s)

  #get reply queue
  q_fin = sqs.queue(wi.reply_queue ||= "#{QUEUE_NAME}_FIN") 
  
  #now check validity
  unless WorkItemHelper.validate_workitem(wi)
    #not a valid workitem! reject and pass along to finished queue
    wi.error = "Not a valid workitem for this node!"
    DaemonKit.logger.info "Not a valid workitem for this node!"
    m_fin = WorkItemHelper.encode_workitem(wi)
   
    DaemonKit.logger.info "Pushing WI onto queue: #{q_fin.name}..."
    
    q_fin.push(m_fin)
    
    #clean up and remove message from initial queue
    
    DaemonKit.logger.info "Deleting invalid workitem..."
    m.delete
    next
  end

  #now that we know that it's valid, lets check for shutdown flag...
  if wi.params['command'].eql?('shutdown') && wi.params['instance-id'].eql?(rmi.instance_id)
    DaemonKit.logger.info "ACK shutdown MESSAGE"
    #send ACK shutdown
    rmi.send_shutdown
    #now shut down!
    m.delete
    break
    
  end

  # notify RMGR busy
  DaemonKit.logger.info "Starting work..."
  rmi.send("busy",  wi.params['sqs-timeout'])

  DaemonKit.logger.info "Adjusting SQS timeout: #{wi.params['sqs-timeout'] ||= VIS_DEFAULT}"
  m.visibility = wi.params['sqs-timeout'] ||= VIS_DEFAULT

  #first lets switch to the directory, clean up, 
  # and then start downloading the input files 

  system "mkdir -p #{WORK_DIR}"

  Dir.chdir(WORK_DIR) do 
    # clean directory
    system "rm -rf *"

    #
    # here we're going to look at a few different ways to get files.
    #    - first we look for an array called input-files, and get them individually
    #    - then we'll look at input_bucket and then filter on input_filter to get other files
    #    - lastly, we'll look for previous output bucket, and get those files using filter
    #

    #First, lets get input files.  they should be in the form: 'mybucket:testdir/sub/file.txt'

    # infile list is an account of files that were downloaded
    infile_list = Array.new
    input_folder = ''
 
    unless wi.params['input-files'].nil?
      # now download each file
      DaemonKit.logger.info "Found Input file list. Downloading..."
      a = wi.params['input-files'].split
      #get folder info
      if a[0].rindex('/').nil?
        input_folder = a[0]
      else
        input_folder = a[0][0..(a[0].rindex('/')-1)]
      end
      a.each do |f|
        infile_list << s3h.download(f)
      end
    end

    #now lets look at the case where an entire folder is specified.  download entire folder, with filter, do the same for previous output
    unless wi.params['input-folder'].nil?
      DaemonKit.logger.info "Found input folder #{wi.params['input-folder']}. Downloading..."
      input_folder = wi.params['input-folder']
      infile_list << s3h.download_folder(wi.params['input-folder'], wi.params['input-filter'])
    end

    # finally lets get previous output folder if all else fails.

    if  wi.params['input-files'].nil? && wi.params['input-folder'].nil? && wi.has_attribute?('previous_output_folder')
      DaemontKit.logger.info "Using previous output folder for inputs. Downloading..."
      input_folder =  wi.previous_output_folder
      infile_list << s3h.download_folder(wi.previous_output_folder, wi.params['input-filter'])
    end

    DaemonKit.logger.info "Downloaded #{infile_list.size} files."

    #now we run the command based on the params, and store it's output in a file
    DaemonKit.logger.info "Running Command #{wi.params['command']}..."

    pipe = IO.popen( wi.params['command'] )

    File.open("#{wi.params['pid']}_output.txt", "w+") do |f| 
        f.write(pipe.readlines)
    end

    #now lets put the files back into the output bucket
    output_folder = wi.previous_output_folder = wi.params['output-folder'] ||= input_folder

    DaemonKit.logger.info "Uploading Output Files..."

    s3h.upload(output_folder, infile_list)

  end
  
  m_fin = WorkItemHelper.encode_workitem(wi)
  
  DaemonKit.logger.info "Pushing WI onto queue: #{q_fin.name}..."
  
  q_fin.push(m_fin)
  
  #clean up and remove message from initial queue
  
  DaemonKit.logger.info "Deleting original message"
  
  m.delete
  
end
