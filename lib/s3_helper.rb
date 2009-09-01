#################################################
###
##     David Austin - ITMAT @ UPENN
#      S3 Helper  downloads and uploads files 
#
#

####    TODO..

class S3Helper


  def initialize (s3)
    # set s3 instance
    @s3 = s3

  end

  def download (file)
    # downloads a single file onto a local directory, returns basename of file
  end

  def download_files (folder, filter)
    # download all files from folder, applying filter to keys, returns array of basenames



  end

  def upload (exclude_list)
    # upload all files in cwd, except the ones in exclude list



  end




end
