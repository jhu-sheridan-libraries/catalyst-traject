require 'fileutils'
require 'net/http'
require 'cgi'

require_relative "solr_connect_helper"



namespace :solr do

  desc "Run optimize on current Solr"
  task :optimize do
    SolrConnectHelper.get_and_print( SolrConnectHelper.solr_url + "/update?stream.body=%3Coptimize/%3E" )    
  end

  desc "Delete ALL records and commit in current Solr"
  task :delete_all do
    if SolrConnectHelper.rails_env == "production"
      puts "Delete_all in PRODUCTION, you sure? (y|n) [n]"
      confirmation = STDIN.gets.chomp.downcase
      unless confirmation[0,1].downcase == 'y'
        puts('Quitting.')
        exit
      end    
    end

    SolrConnectHelper.get_and_print(SolrConnectHelper.solr_url + "/update?stream.body=%3Cdelete%3E%3Cquery%3Eid:%5B*TO%20*%5D%3C/query%3E%3C/delete%3E")
    
    SolrConnectHelper.get_and_print(SolrConnectHelper.solr_url + "/update?stream.body=%3Ccommit/%3E")
  end

  desc "Replicate in current environment from :replicate_master_url"
  task :replicate do
    unless SolrConnectHelper.replicate_master_url
      puts "No replicate_master_url found in #{File.join("../config", "blacklight.yml")} / #{SolrConnectHelper.rails_env}"
      exit(1)
    end
    SolrConnectHelper.get_and_print(SolrConnectHelper.solr_url + "/replication?masterUrl=#{CGI.escape(SolrConnectHelper.replicate_master_url + "/replication")}&command=fetchIndex")
  end

end


