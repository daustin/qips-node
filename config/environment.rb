# Be sure to restart your daemon when you modify this file

# Uncomment below to force your daemon into production mode
#ENV['DAEMON_ENV'] ||= 'production'

AWS_ACCESS_KEY_ID = '11111111111111111111111'
AWS_SECRET_ACCESS_KEY = '22222222222222222222222222'
QUEUE_NAME = 'workqueue0'  # this tells us where to get the message. don't pull from a queue that we can't accept WI's from
VIS_PEEK = 5
VIS_DEFAULT =  30
SLEEP_TIME = 30

# Boot up
require File.join(File.dirname(__FILE__), 'boot')

DaemonKit::Initializer.run do |config|

  # The name of the daemon as reported by process monitoring tools
  config.daemon_name = 'qmgr-node'

  # Force the daemon to be killed after X seconds from asking it to
  # config.force_kill_wait = 30

  # Log backraces when a thread/daemon dies (Recommended)
  # config.backtraces = true

  # Configure the safety net (see DaemonKit::Safety)
  # config.safety_net.handler = :mail # (or :hoptoad )
  # config.safety_net.mail.host = 'localhost'
end
