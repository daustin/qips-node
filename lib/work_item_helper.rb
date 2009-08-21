class WorkItemHelper

  def self.decode_message (message)
    
    o = Base64.decode64(message)
    o = YAML.load(o)
    o = OpenWFE::workitem_from_h(o)
    o
    
  end

  def self.encode_workitem (wi)
    
    msg = wi.to_h
    msg = YAML.dump(msg)
    msg = Base64.encode64(msg)
    msg
  end
  
end

