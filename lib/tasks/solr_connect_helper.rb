require 'yaml'
require 'erb'
require 'net/http'
require 'uri'

# Some helpers for rake tasks, for finding
# connecting to Solr via HTTP
module SolrConnectHelper
  # Stupid sub-class of Net::HTTP to get rid of the timeout. 
  class NoTimeoutHTTP < Net::HTTP
    def initialize(*args)
      super(*args)
      @read_timeout = nil
    end
  end

  # make all methods module methods. 
  extend self

  def get_and_print(uri, options = {})
    puts "\nGET #{uri}\n\n"  
    
    response = NoTimeoutHTTP.get_response( URI.parse(uri) )

    puts response.class
    
    if (options[:headers])
      headers = response.to_hash    
      puts headers.keys.collect {|k| "#{k}: #{headers[k]}\n"}
      puts "\n\n"
    end
    
    puts response.body

    return response
  end

  # Root path for  app, that traject 
  # -- used for looking up config files, placing lock files, etc. 
  def app_root
    File.expand_path("../../../", __FILE__)
  end

  def solr_yml_path
    File.join(app_root, "config", "blacklight.yml")
  end

  def rails_env
    ENV["RAILS_ENV"] || "development"
  end

  def solr_url(env = rails_env)    
    solr_config = YAML.load(ERB.new(File.read(solr_yml_path)).result)
    solr_config[env]["url"]
  end

  def replicate_master_url(env = rails_env)    
    solr_config = YAML.load(ERB.new(File.read(solr_yml_path)).result) 
    solr_config[env]["replicate_master_url"]
  end

end
