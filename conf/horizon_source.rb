# traject configuration for talking to horizon

# Reads horizon connection details from containing Rails apps config/horizon.yml
# Set ENV["RAILS_ENV"] to determine rails environment key used for horizon
# connect detail defaults -- can still be overridden on command line.

require 'traject_horizon'
require 'dotenv'
Dotenv.load

horizon_yml_path = File.expand_path("../../config/horizon.yml", __FILE__)
all_config       = YAML.load(ERB.new(File.read(horizon_yml_path)).result)
conf             = all_config[  ENV['FROM_ENV'] || ENV['RAILS_ENV'] || "development"   ]

# development:
#   host: horizonbu.mse.jhu.edu
#   port: 2025
#   db_name: horizon_test
#   login: esys
#   password: ****
#   jtds_type: sybase

settings do
  store "reader_class_name", "Traject::HorizonReader"

  # All of these, looked up from our yaml config file,
  # can be over-ridden on command line too. 
  provide "horizon.jtds_type",  conf['jtds_type']
  provide "horizon.host",       conf['host']
  provide "horizon.port",       conf['port']
  provide "horizon.database",   conf['db_name']
  provide "horizon.user",       conf['login']

  provide "horizon.password",   conf['password']

  # For records with copies, include copies but not (subsidiary) items,
  # for records without copies, include items: 'direct' mode. 
  provide "horizon.include_holdings", "direct"

  # Configuration for what item/copy columns to include (and how to join
  # them in via sql), and how to map them to marc fields:

  provide "horizon.item_tag",      "991"
  # Crazy isnull() in the call_type join to join to call_type directly on item
  # if specified otherwise calltype on collection. Phew!
  provide "horizon.item_join_clause", "LEFT OUTER JOIN collection ON item.collection = collection.collection LEFT OUTER JOIN call_type ON isnull(item.call_type, collection.call_type) = call_type.call_type"
  provide "horizon.item_map", {
    "item.call_reconstructed"   => "a",
    "call_type.processor"       => "f",
    "call_type.call_type"       => "b",
    "item.copy_reconstructed"   => "c",
    "item.staff_only"           => "q",
    "item.item#"                => "i",
    "item.collection"           => "l",
    "item.notes"                => "n",
    "item.location"             => "m",
    "item.n_ckos"               => "z" # num checkouts
  }

  provide "horizon.copy_tag",         "937"
  # Crazy isnull() in the call_type join to join to call_type directly on item
  # if specified otherwise calltype on collection. Phew!
  provide "horizon.copy_join_clause", "LEFT OUTER JOIN collection ON copy.collection = collection.collection LEFT OUTER JOIN call_type ON isnull(copy.call_type, collection.call_type) = call_type.call_type"
  provide "horizon.copy_map", {
    "copy.copy#"           => "8",
    "copy.call"            => "a",
    "copy.copy_number"     => "c",
    "call_type.processor"  => "f",
    "call_type.call_type"  => "b",
    "copy.staff_only"      => "q",
    "copy.location"        => "m",
    "copy.collection"      => "l",
    "copy.pac_note"        => "n"
  }


  # verbose local tags we don't need, and which may also
  # conflict with the tag we're going to use to add holdings, get
  # them out of the way.
  provide "horizon.exclude_tags", "998,999,991,937"
end

# Skip records with leader byte 5 'd', those are records marked
# for deletion but not yet purged from horizon.
each_record do |record, context|
  if record.leader[5] == 'd'
    context.skip!("Leader byte 5 is 'd', record marked for deletion in Horizon")
  end
end
