# Gemfile just for our uses of the traject indexer
source "https://rubygems.org"

gem "dotenv"

# Add rake so when we execute bundle exec rake in context of this
# gem file, it'll work. 
gem "rake"
gem 'ruby-debug'
gem "traject"
gem "traject_horizon"
# for experimental umich format classification
gem "traject_umich_format"
gem "traject-marc4j_reader"

gem "lcsort" #, :path => "../../lcsort"
gem "traject_sequel_writer" #, :path => "../../traject_sequel_writer" #:github => "traject/traject_sequel_writer"
gem "jdbc-mysql"
# traject dependencies which require updated for jruby1.9

# fixes warning: Object#timeout is deprecated, use Timeout.timeout instead
gem "httpclient"

# fixes warning: `-' after local variable or literal is interpreted as binary operator
gem "concurrent-ruby"
