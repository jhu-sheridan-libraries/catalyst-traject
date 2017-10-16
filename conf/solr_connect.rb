# Load from Rails project config/blacklight.yml to determine
# which Solr to connect to. Set ENV["RAILS_ENV"]
# to choose environment. 

solr_yml_path = File.expand_path("../../../config/blacklight.yml", __FILE__)
solr_config   = YAML::load(File.open(solr_yml_path))

env_name      = ENV["RAILS_ENV"] || "development"
this_env      = solr_config[ env_name ]

# prefer replicate_master_url if available
solr_url = this_env["replicate_master_url"] || this_env["url"]

settings do
  provide "solr.url", solr_url
  provide "solrj_writer.commit_on_close", true
end

# Set up some custom logging -- ordinary logging at INFO
# level will go, as usual, to stderr, where cronmail
# will capture it. 
# Full logging to a file in ./tmp with DEBUG level, including a progress line every 20_000 records. 

settings do   
  provide "log.batch_size", 20_000 
  provide "log.batch_size.severity", "DEBUG"
end

# Create logger that goes nowhere to begin with, we'll add adapters
# to stdout for info and above, and a file in ./log that includes everything. 
self.logger = Yell::Logger.new(:null, :level => "debug", :format => self.logger_format)

# send to stdout so it fits into our cronmail captured transcript
# better, only at info and above level. 
self.logger.adapter :stdout, :level => "info"

# But log full debug level, with progress status,
# to a file in app ./log dir. 
#
# capistrano's multi-directory setup is confusing the symlink option, need
# to disable symlink for datefile log. 
log_file = File.expand_path("../../../log/traject-#{env_name}.log", __FILE__)
logger.adapter :datefile, log_file, :date_pattern => "%Y-week-%V", :keep => 2, :symlink => false
