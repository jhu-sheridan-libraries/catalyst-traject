require 'net/http'
require 'cgi'
require "rexml/document"
require "time"
require 'dotenv'
Dotenv.load
require 'dotenv/tasks'

require_relative "solr_connect_helper"

namespace :horizon do


  desc "Export from Horizon to file"
  task export: :dotenv do      
    command_line = traject_command_line(:mode => "marcout", :output_file => ENV['OUTPUT'])
    puts "Executing:\n#{command_line}"
    puts
    success = system(command_line)

    # look for errors in the log

    unless success
      puts
      puts "Errors! exiting in failure mode"      
      exit(1)
    end
  end  

  desc "exporter directly from horizon, map to Solr"
  task export_to_index: :dotenv do
    command_line = traject_command_line(:mode => :solr)

    puts "Executing:\n#{command_line}"
    puts
    success = system(command_line)

    # look for errors in the log

    unless success
      puts
      puts "Errors! exiting in failure mode"      
      exit(1)
    end
  end

  
  desc "Mass index from Horizon, with replication"
  task mass_index: :dotenv do  
    solr_master = SolrConnectHelper.replicate_master_url

    if solr_master.nil? 
      one_solr_url = SolrConnectHelper.solr_url
      $stderr.puts
      $stderr.puts "WARNING: NO replicate_master_url set in blacklight.yml / #{SolrConnectHelper.rails_env}!!!"
      $stderr.puts "We will be indexing directly to #{one_solr_url}"
      $stderr.puts

      unless ENV['MASS_INDEX_SINGLE_SOLR']=="1"
        $stderr.puts "For safety, won't do this unless you set env MASS_INDEX_SINGLE_SOLR=1."
        $stderr.puts "Exiting."
        $stderr.puts
        $stderr.puts "If you're sure, run again as for example:"
        $stderr.puts "$ MASS_INDEX_SINGLE_SOLR=1 rake #{ARGV.join(' ')}"
        exit 1
      end      
    end

    mass_index_solr_url = solr_master || one_solr_url
    
    
    # Make a lock file. This task will run a long time, we don't want to
    # run it twice overlapping. We also want to capture the start time for
    # later deleting records older than start time, in a safe way, so we
    # have it for manual recovery even if we crash. 
    #
    # important we keep lockfile in tmp/pids, a directory that capistrano
    # keeps in shared -- if it's just in a capistrano single app releases
    # directory, it won't be noticed for collisions! 
    lockfile = File.join(SolrConnectHelper.app_root, "tmp", "pids", "horizon_mass_index.pid")
    if File.exist?(lockfile)
      puts "Lock file exists, is another process running? Manually delete if you know what you're doing: #{lockfile}"
      exit 1
    end    
    start_time = Time.now
    
    puts "Registered start time: #{start_time}  (#{start_time.utc.iso8601})"
    
    
    File.open(lockfile, "w") do |f|
      f.write( {'pid' => Process.pid, 'start_time' => start_time}.to_yaml )
    end
    
    #Delete the lockfile even if we die weirdly
    at_exit do
      if File.exists?( lockfile )
        File.delete(lockfile)
        puts "Lock file still existed at exit, exit abnormal? Removed #{lockfile}"
      end
    end
    
    # Mass index to replication master 
    puts
    puts "Running horizon:export_to_index to #{mass_index_solr_url}"    
    Rake::Task["horizon:export_to_index"].invoke    
    puts "Done importing all horizon to master #{Time.now}"
    
    # Now delete any records OLDER than when we started from
    # source=horizon, cause if the record wasn't replaced with a newer
    # one, that means it's been deleted from horizon.
    puts
    puts "Deleting old records prior to our current import"
    dq = "<delete><query>source:horizon AND timestamp:[* TO #{start_time.utc.iso8601}]</query></delete>"
    SolrConnectHelper.get_and_print("#{mass_index_solr_url}/update?stream.body=#{CGI.escape(dq)}"  )
    SolrConnectHelper.get_and_print(mass_index_solr_url + "/update?stream.body=%3Ccommit/%3E")
    puts "Done deleting old records at #{Time.now}"
    

    # And optimize the master guy please
    SolrConnectHelper.get_and_print( mass_index_solr_url + "/update?stream.body=%3Coptimize/%3E" )
    puts "Done optimizing #{mass_index_solr_url} at #{Time.now}"
    
    
    # Sanity check, if master doens't have at least ENV["SANITY_CHECK_COUNT"]
    # records, abort abort abort! (default two million)
    if solr_master.nil?    
      puts "No master/slave defined, skipping replication"
    else
      puts   
      sanity_check_count = (ENV["SANITY_CHECK_COUNT"] || 2000000).to_i
      puts "Sanity check, won't replicate unless at least #{sanity_check_count} records in master..."
      # q=*:* encoded
      response = SolrConnectHelper.get_and_print("#{solr_master}/select?defType=lucene&q=%2A%3A%2A&rows=0&facet=false")    
      xml = REXML::Document.new response.body if response.kind_of?(Net::HTTPOK)
      if xml && (count = xml.elements["//result/@numFound"].to_s.to_i) && count >= sanity_check_count
        puts "...Passed!"
      else
        puts "....FAILED!! #{count}"
        exit(1)
      end    


      # And replicate it to slave!
      puts
      puts "Replicating master to slave"
      Rake::Task["solr:replicate"].invoke
      puts "Done sending replicate command at #{Time.now}. (Replication itself may still be ongoing)"
    end
    
    # Delete lockfile, and we're done
    File.delete(lockfile)
    
    puts "Done at #{Time.now}"
  end    
end

# args:
# [:timestamp]  pass in a timestamp to use for creating filenames.
#               Can be useful for ensuring consistent timestamps with files created elsewhere.
# [:output_file]  string file path to write marc to when in 'marcout' mode. Leave blank to write to stdout. 
# [:mode]       default 'marcout', can be 'marcout' to output marc, or 'solr' to index to solr
#
# ENV variables used:
#
# [ONLY]
# [FIRST]
# [LAST]
# [RAILS_ENV]
def traject_command_line(args = {})
  # Horizon and Solr connection details are looked
  # up by the actual traject config files that we invoke, based on RAILS_ENV.
  # We dont' need to look them up here or include them in command line. 

  str = ""
  if `ruby -v` =~ /jruby/
    puts "current ruby is jruby, executing with current ruby..."
  elsif system("chruby-exec jruby -- echo")
    str << "chruby-exec jruby -- "
  else
    warn "No jruby detected, and `chruby-exec jruby` not available either. We need jruby to run traject. Try installing chruby with jruby?"
    exit 1
  end
  
  str << " BUNDLE_GEMFILE='./Gemfile' bundle exec "
  
  str << "traject -c conf/horizon_source.rb -c conf/horizon_index.rb  "

  if args[:mode].to_s == "solr"
    str << " -c conf/solr_connect.rb "
  else # args[:mode] == marcout
    str << " -x marcout"

    output_file = args[:output_file] || auto_marcout_filename
    str << " -o \"#{output_file}\" "
  end

  # Only it's a normal kind of indexing, do the
  # shelfbrowse side channel index too
  unless ENV['ONLY'] || ENV['FIRST'] || ENV['LAST'] || (args[:mode] != :solr)
    str = "SHELFBROWSE_TMP_TABLE_SWAP=1 #{str}"
    str << " -c conf/shelf_browse_index.rb"
  end


  str << " -s horizon.only_bib=#{ENV['ONLY']} " if ENV['ONLY']
  str << " -s horizon.first_bib=#{ENV['FIRST']} " if ENV['FIRST']
  str << " -s horizon.last_bib=#{ENV['LAST']} "   if ENV['LAST']
  
  
  return str
end

def now_timestamp
  Time.now.strftime("%d%b%Y-%H%M")
end

def auto_marcout_filename
  name = ""
  if ENV['ONLY']
    name << "bib#{ENV['ONLY']}"
  elsif ENV['FIRST'] || ENV['LAST']
    name << "bibs-" <<
      ("#{ENV['FIRST']}" if ENV['FIRST']) << "-" <<
      ("#{ENV['LAST']}" if ENV['LAST'])
  else
    name << "marcout"
  end

  name << "-#{now_timestamp}.marc"
end





