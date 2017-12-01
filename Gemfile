# Gemfile just for our uses of the traject indexer
source "https://rubygems.org"

gem "dotenv"

# Add rake so when we execute bundle exec rake in context of this
# gem file, it'll work. 
gem "rake"
gem 'ruby-debug'
gem "traject", "~> 2.0" #:github => "traject-project/traject"
gem "traject_horizon", "~> 1.2", ">= 1.2.4"

# for experimental umich format classification
gem "traject_umich_format"

gem "lcsort" #, :path => "../../lcsort"
gem "traject_sequel_writer" #, :path => "../../traject_sequel_writer" #:github => "traject/traject_sequel_writer"
gem "jdbc-mysql"
# traject dependencies which require updated for jruby1.9

# fixes warning: Object#timeout is deprecated, use Timeout.timeout instead
gem "httpclient", "~>2.8", ">= 2.8.2"

# fixes warning: `-' after local variable or literal is interpreted as binary operator
gem "concurrent-ruby", "~>0.9", ">= 0.9.1"
