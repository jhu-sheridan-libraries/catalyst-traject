require 'traject/sequel_writer'
require 'lcsort'
require 'traject/null_writer'
require 'time'

#
# A pretty hacky traject config to index call numbers to the stackview_call_numbers
# table in the app rdbms (MySQL). To support our shelf browse feature, based
# on rails_stackview. 
#
# These are indexed in a 'side channel', in an each_record block, usually
# along side normal indexing to the Solr. 
#
# By default this writes directly to the stackview_call_numbers, which
# if the table already had data in it, may create duplicate entries, and
# leave old should-be-deleted entries. 
#
# Set ENV SHELFBROWSE_TMP_TABLE_SWAP=1 to, instead, index to a temporary table
# first, then swap that table in as stackview_call_numbers when indexing is
# complete. This can be used in a mass index to completely replace table
# with new indexed contents. 
#
# The actual logic for mapping from MARC to rows in stackview_call_numbers
# is in the JhShelfBrowseExtractor class, which is presently defined at the bottom
# of this very file. It does get a bit hacky. 
#
# We are using the lcsort gem for LC call number normalization. 
# https://github.com/pulibrary/lcsort
#
# And the traject_sequel_writer gem to write to our rdbms
# https://github.com/traject/traject_sequel_writer


# Get the database to write to from the app's database.yml a couple dirs up
database_yml_path = File.expand_path("../../../config/database.yml", __FILE__)
all_db_config       = YAML::load(File.read(database_yml_path))
db_conf             = all_db_config[  ENV['FROM_ENV'] || ENV['RAILS_ENV'] || "development"   ]


sequel_db           = Sequel.connect("jdbc:mysql://#{db_conf['host']}:#{db_conf['port']}/#{db_conf['database']}?characterEncoding=utf8&user=#{db_conf['username']}&password=#{db_conf['password']}",
  # Not sure why we're getting pool timeouts, try increasing from 5 seconds to 10
  # and increasing connections
    :pool_timeout => 10,
    :max_connections => 6
  )

if ENV["SHELFBROWSE_TMP_TABLE_SWAP"]
  logger.info("SHELFBROWSE: indexing to tmp table, will swap tables after processing")

  # Create a stackview_call_numbers_tmp with the same schema as stackview_call_numbers
  sequel_db.run("DROP TABLE IF EXISTS stackview_call_numbers_TMP")
  # This is MySQL-only syntax, sorry. 
  sequel_db.run("CREATE TABLE stackview_call_numbers_TMP LIKE stackview_call_numbers")

  index_to_table_name = 'stackview_call_numbers_TMP'
else
  index_to_table_name = 'stackview_call_numbers'
end

safe_uri = sequel_db.uri.gsub(/([\?\&]password=)[^\&\;]*/, '\1[SUPRESSED]')
logger.info("SHELFBROWSE: Writing to #{safe_uri}, table #{index_to_table_name}")

call_writer = Traject::SequelWriter.new(
  # JDBC connection string for our MySQL app db. 
  # NOTE: characterEncoding param is neccesary to avoid corrupting UTF8
  "sequel_writer.database"      => sequel_db,
  "sequel_writer.table_name"    => index_to_table_name,
  # Give it our current traject logger, so it can log. 
  "logger"                      => logger
)


each_record do |record, context|
  # If it's been marked to skip, do nothing with it! 
  next if context.skip? 

  JhShelfBrowseExtractor.new(context).stackview_output_contexts.each do |s_context|
    call_writer.put s_context
  end
end

after_processing do
  # Make sure we wait for SequelWriter to finish anything it's got queued. 
  logger.info("SHELFBROWSE: Closing writer...")
  call_writer.close

  if ENV["SHELFBROWSE_TMP_TABLE_SWAP"]
    logger.info("SHELFBROWSE: Swapping stackview_call_numbers_TMP for stackview_call_numbers")

    # Swap our TMP table in for the real table. This may be MySQL-only syntax. 
    sequel_db.transaction do
      sequel_db.run("RENAME TABLE stackview_call_numbers TO stackview_call_numbers_old, stackview_call_numbers_TMP To stackview_call_numbers")
      sequel_db.run("DROP TABLE stackview_call_numbers_old")
    end
  end
  logger.info("SHELFBROWSE complete")
end

# Try to clean up a bit our logic for creating stackview_call_numbers rows for shelf browse
# in this class. It's still a bit of a mess. 
class JhShelfBrowseExtractor
  # Max widths to truncate db values to
  @max_widths = {
    'title'   => 95,
    'creator' => 95,    
  }
  def self.max_widths
    @max_widths
  end

  # Local collection codes that are used for "Blue Label P's" --
  # confusingly, the only LC call numbers starting with P we want to KEEP
  # are the blue label P's. 
  @blue_label_hash = {}
  blue_label_collections = ['ccolubl', 'cdntnbl', 'cmtgrfb', 'cmtgybl', 'cmtgybn', 'codtnbl', 'cvsplbl', 'cwa&sbl', 'cwa&srb', 'cwashbl', 'eanalbl', 'eanblnc', 'eartbl', 'eavdvdb', 'eavrefb', 'ecirlab', 'ecl87bl', 'eclarfb', 'eclasbl', 'ecrlbnc', 'egerfbl', 'egerfwb', 'ehomebl', 'emainbb', 'emaindb', 'emblue', 'emblunc', 'enearbl', 'eoblu', 'eoblunc', 'eoc87bl', 'eocirlb', 'eoclabl', 'eocrmrb', 'eofdtbl', 'eofhseb', 'eofmdbl', 'eofprbl', 'eofspbl', 'eofsptb', 'eofzioc', 'eolblue', 'eolmedb', 'eolprob', 'eolserb', 'eoschbl', 'ercobl', 'escagbl', 'escrfbl', 'esforbl', 'esfwlbl', 'esg2fbl', 'esgrfbl', 'esgribl', 'esgrrbl', 'esgrtbl', 'esmedbl', 'espugib', 'estudbl']  
  blue_label_collections.each {|col| @blue_label_hash[col] = true}
  def self.blue_label_hash
    @blue_label_hash
  end

  # We get the whole traject context, becuase we need the MARC source_record
  # to extract some things from, AND the current output_hash which we want
  # to copy certain things from
  def initialize(context)
    @context = context
  end

  def source_record
    @source_record ||= @context.source_record
  end

  def solr_output
    @solr_output ||= @context.output_hash
  end

  # Returns a hash of columns to values for stackview_call_numbers table,
  # NOT including actual call numbers yet, just values related to the bib
  # itself. We'll later supplement it with call number info, possibly
  # multiple call numbers per bib. 
  def output_bib_values
    self.truncate_values({
      "system_id"     => solr_output["id"].first,
      "title"         => self.title,
      "creator"       => self.creator,
      "pub_date"      => self.pub_date,
      "format"        => self.stacklife_format,
      "shelfrank"     => self.shelfrank,

      "measurement_page_numeric"    => self.num_pages,
      "measurement_height_numeric"  => self.height_cm
    })
  end
    

  # Returns an array of Traject::Contexts representing prepared
  # rows to add to the stackview_call_numbers table. 
  def stackview_output_contexts
    # Get the call-number-independent output values for this record
    bib_value_hash = self.output_bib_values

    # Get all the call numbers
    lc_calls = self.local_lc_calls + self.bib_lc_calls

    # For each call number, prepare an output Context -- but we collapse
    # the call numbers on unique LC class letter/whole number per 
    # bib, which we can do by looking at the first 8 bytes of the normalized
    # call number. 

    seen_call_base = {}

    contexts = []

    lc_calls.each do |call|
      # Strip some of the local prefixes we use before call numbers
      call.gsub!(/\A *(OVERSIZE)? ?(CAGE)? */i, '') if call

      next if call.empty?

      # Add bib ID on the end before normalization, to ensure unique
      # call nums when identical accross two bibs. Makes things work less
      # confusingly. Should not disturb the parser if we separate with a space. 
      normalized = Lcsort.normalize(call, :append_suffix => bib_value_hash['system_id'])
      
      # If we couldn't normalize, but it looks basically like a call number, log it. 
      #if (!normalized) && call !~ /\:/ && call =~ /\s*([A-Z]{1,3})\s*(\d+(\s*?\.\s*?(\d+)))/
      #  puts "Bad call number #{call} at https://catalyst.library.jhu.edu/catalog/#{orig["id"].first}" 
      #end

      next unless normalized

      call_base = normalized.slice(0,7)
      next if seen_call_base.has_key?(call_base)
      
      seen_call_base[call_base] = true

      # Make a new hash for this call number specifically. 
      o = bib_value_hash.dup
      o["sort_key_type"]    = "lc"
      o["sort_key_display"] = call
      o["sort_key"]         = normalized
      o["created_at"]       = Time.now

      ocontext = Traject::Indexer::Context.new(:output_hash => o, :source_record => self.source_record)
      contexts << ocontext
    end

    return contexts
  end



  # Gets a displayable title out of marc
  def title
    default = "[TITLE UNKNOWN]"
    m245 = source_record['245']

    return default unless m245

    title = m245['a'] || m245['k']

    return default unless title

    title = title.sub(/\s*[[:punct:]]+\s*$/, '')

    subtitle = [m245['b'], m245['n'], m245['p'], m245['s']].compact.join('')
    subtitle = nil if subtitle.empty?
    subtitle = subtitle.sub(/\s*[[:punct:]]+\s*$/, '') if subtitle

    return [title, subtitle].compact.join(": ")
  end

  # Gets a displayble creator out of MARC, may return nil
  def creator
    ( (source_record['100'] && source_record['100']['a']) || 
      (source_record['110'] && source_record['110']['a']) || 
      (source_record['111'] && source_record['111']['a']) || 
      (source_record['245'] && source_record['245']['c'])
    )
  end

  # Copies a publication date already extracted for solr output, if present.
  # May be nil. 
  def pub_date
    solr_output["pub_date"] && solr_output["pub_date"].first
  end

  # Extracts num pages from MARC. May be nil. 
  def num_pages    
    if source_record['300'] && (source_record['300']['a'] =~ /(\d{1,6}) ?pp?(?:[\. ]|\Z)(?!.*\d+ pp?(?:[\. ]|\Z))/)
      return $1
    end

    return nil
  end

  # Extracts height in cm from MARC record, may be nil. 
  def height_cm
    return nil unless source_record['300']

    if source_record['300']['c'] =~ /(\d{1,4}) ?cm(?:[\. ]|\Z)/
      return $1
    elsif source_record['300']['c'] =~ /(\d{1,4}) ?in\./
      # inches? why not convert to cm
      return $1.to_i * 2.5
    end

    return nil
  end

  # Translates from our already existing local format vocabular, present in the previously
  # calculated Solr output -- to a format string for stacklife. 
  #
  # Includes custom 'plain' format, sometimes with "plain:extra format info" tag. 
  def stacklife_format
    return "plain" unless solr_output["format"]

    if (solr_output["format"] & ["Book", "Dissertation/Thesis", "Musical Score"]).count > 0
      return 'book'
    elsif solr_output["format"].include?("DVD")
      return 'Video/Film'
    elsif solr_output["format"].include?("CD")
      return 'Sound Recording'
    elsif solr_output["format"].include?("Journal/Newspaper")
      return 'Serial'
    else
      formats = solr_output["format"].dup

      # Never show 'Print', not useful here
      formats.delete("Print")

      # We don't need "Video/Film" if it's marked VHS, or "Musical Recording" if it's marked LP
      formats.delete("Video/Film") if formats.include?("VHS")
      if formats.include?("LP")
        formats.delete("Musical Recording") 
        formats.delete("Non-musical Recording")
      end

      # Include format descr after 'plain:', our stackview plain
      # type template will display it. 
      return "plain:#{formats.join(', ')}"
    end
  end

  # Calculates a shelfrank for stackview, by:
  #
  # Add up all checkouts on any items, call that shelfrank. We really ought
  # to probably normalize num checkouts per time item has been in collection, and
  # normalize on a scale of 1-100. But I bet this will work well enough anyway. 
  def shelfrank
    return Traject::MarcExtractor.cached("991z").extract(source_record).collect {|i| i.to_i}.inject(:+)
  end

  # Gets call numbers that we think are probably LC or NLM out of the 991 and 937
  # MARC fields we use for listing our local holdings. 
  #
  # Ignores things we think are white label P's, which are not standard
  # LC classifications, leaves them out. 
  #
  # Could be expanded in the future to just 'local_calls' returning
  # a hash of call-type and array, if we expand to handling more types
  # in separate runs. 
  def local_lc_calls
    return Traject::MarcExtractor.cached("991:937").collect_matching_lines(source_record) do |field, spec, extractor|
      # we output call type 'processor' type in subfield 'f' of our holdings
      # fields, that sort of maybe tells us if it's an LCC field.
      # When the data is right, which it often isn't, but we use it to eliminate certain ones. 
      
      call_num = field['a'] if ['lc', 'nlm'].include?(field['f'])
      
      # Strip some of the local prefixes we use before call numbers
      call_num.gsub!(/\A *(OVERSIZE)? ?(CAGE)? */i, '') if call_num

      # ignore it if it's a white label P, not standard LC classification -- which is 
      # hard to figure out. 
      # If it's collection code begins with 'e' but is NOT in the 'blue label' list, 
      # then it's probably a white label P. 
      if call_num && call_num[0] == "P" && field['l'][0] == 'e' && !( self.class.blue_label_hash.has_key?(field['l']))
        call_num = nil 
      end

      call_num
    end.compact
  end

  # Extract cataloger suggested LC call-numbers from Bib itself. 
  # May also include LC class numbers which serve as
  # the basis for call numbers and are good enough. And we include NLM calls too,
  # becuase they sort the same. 
  #
  # Looks in bib 050, 055, 060, 090. 
  # Makes sure contiguous $a and $b are kept together. 
  def bib_lc_calls
    return_val = []

    Traject::MarcExtractor.cached("050:055:060:090").each_matching_line(source_record) do |line|
      separated_calls = line.inject([]) do |results, subfield|
        if subfield.code == 'a'
          results << []
        end

        if ['a', 'b'].include? subfield.code
          results.last && (results.last << subfield.value)
        end

        results
      end

      return_val.concat separated_calls.collect {|arr| arr.join(" ")}
    end

    return return_val
  end

  # just a little utility method for simply truncating str to byte width
  # Kind of tricky to do it UTF-8 aware, oh well. 
  def truncate(str, width)
    return str if str.bytesize <= width

    ellipses = '...'
    truncated_width = width - 3 # leave room for elipses

    # Not handling UTF-8 at the moment
    str.slice(0, truncated_width) + ellipses
  end

  def truncate_values(hash)
    self.class.max_widths.each do |key, max|
      hash[key] = truncate(hash[key], max) if hash[key]
    end

    return hash
  end

end