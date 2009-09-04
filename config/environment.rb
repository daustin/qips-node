# Be sure to restart your daemon when you modify this file

# Uncomment below to force your daemon into production mode
#ENV['DAEMON_ENV'] ||= 'production'

# this tells us where to get the messages. 
# don't pull from a queue that we can't accept WI's from
QUEUE_NAME = 'SEQUEST' 

# this is the name of the status queue to alert rmgr:
STATUS_QUEUE = 'NODE_STATUS'

# this is the instance_id to use when not an AWS instance
ALT_INSTANCE_ID = 'daustin_1234'

# working directory. this is where all the work's done on this node
WORK_DIR = '/Users/daustin/scratch'

VIS_PEEK = 0 # AWS peek visibility timeout in seconds
VIS_DEFAULT =  1800 # visibility timeout if not specified
SLEEP_TIME = 30 # sleep time in loop

#get AWS creds
require File.join(File.dirname(__FILE__), 'sqs')

# Boot up
require File.join(File.dirname(__FILE__), 'boot')

DaemonKit::Initializer.run do |config|

  # The name of the daemon as reported by process monitoring tools
  config.daemon_name = 'qips-node'

  # Force the daemon to be killed after X seconds from asking it to
  # config.force_kill_wait = 30

  # Log backraces when a thread/daemon dies (Recommended)
  # config.backtraces = true

  # Configure the safety net (see DaemonKit::Safety)
  # config.safety_net.handler = :mail # (or :hoptoad )
  # config.safety_net.mail.host = 'localhost'
end
