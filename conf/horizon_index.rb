require 'traject/macros/marc21_semantics'
extend  Traject::Macros::Marc21Semantics

require 'traject/macros/marc_format_classifier'
extend Traject::Macros::MarcFormats

# add our ../lib to LOAD_PATH, including ../lib/translation_maps
$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../lib'))

settings do
  # 3 cpu's on catsolrmaster, normally would default to 2 procesing threads,
  # let's try 3 to see if it speeds things with for our parallel shelfbrowse indexing
  # going on. 
  provide "processing_thread_pool", 3
end


to_field "id", extract_marc("001", :first => true) do |marc_record, accumulator, context|
  # prefix each Horizon bib id with "bib_" to make a Solr unique id --
  # namespacing to avoid collision when we add records from other sources.
  accumulator.collect! {|s| "bib_#{s}"}
end

to_field "source",              literal("horizon")

to_field "marc_display",        serialized_marc(:format => "binary", :binary_escape => false, :allow_oversized => true)

to_field "text",                  extract_all_marc_values
to_field "text_extra_boost_t",    extract_marc("505art")
to_field "publisher_t",           extract_marc("260abef:261abef:262ab:264ab")
to_field "language_facet",        marc_languages

to_field "format"                 do |record, accumulator|
  # PIG wants no 'Other' category for unclassified elements, so we :default => nil. 
  accumulator.concat Traject::Macros::MarcFormatClassifier.new(record).formats( :default => nil  )
end

# add in DVD/CD etc carrier types courtesy of umich gem
# https://github.com/billdueber/traject_umich_format
#
# Note that PIG wants just eg "CD" instead of "Audio CD", even
# though the logic used from umich really is trying to target only Audio CDs. 
# 19 March 2013. 
# We'll see if that causes confusion and needs to be changed one way or another. 
require 'traject/umich_format'
umich_format_map = Traject::TranslationMap.new('umich/format').merge(
  "RC" => "CD",
  "RL" => "LP",
  "VB" => "Blu-ray",
  "VD" => "DVD",
  "VH" => "VHS",
)

to_field "format" do |record, accumulator|
  types = Traject::UMichFormat.new(record).types
  # only keep the ones we want
  # (previously tried more that didn't work with our catalog)
  types = types & %w{RC RL VB VD VH}
  # translate to human with translation map
  accumulator.concat umich_format_map.translate_array(types)
end

to_field "isbn_t",                extract_marc("020a:773z:776z:534z:556z")
to_field "lccn",                  extract_marc("010a") do |record, accumulator|
  accumulator.each {|s| s.strip!}
end
to_field "material_type_display", extract_marc("300a", :separator => nil, :trim_punctuation => true)
to_field "title_t",               extract_marc("245ak")
to_field "title1_t",              extract_marc("245abk")
to_field "title2_t",              extract_marc("245nps:130:240abcdefgklmnopqrs:210ab:222ab:242abcehnp:243abcdefgklmnopqrs:246abcdefgnp:247abcdefgnp")
to_field "title3_t",              extract_marc("700gklmnoprst:710fgklmnopqrst:711fgklnpst:730abdefgklmnopqrst:740anp:780abcrst:785abcrst:773abrst")

to_field "title3_t" do |record, accumulator|
  # also add in 505$t only if the 505 has an $r -- we consider this likely to be
  # a titleish string, if there's a 505$r
  record.each_by_tag('505') do |field|
    if field['r']
      accumulator.concat field.subfields.collect {|sf| sf.value if sf.code == 't'}.compact
    end
  end
  accumulator.uniq!
end

# An 'exact title' exactMatch field, from 245ak and
# 245ab. Also deals with non-filing chars.
# Can return multiple strings in some cases, such as non-filing chars,
# or both $b and $k present.
#
# Doesn't worry about punctuation, as that should be normalized by
# exactMatch field this is destined for.
to_field "title_exactmatch" do |record, accumulator|
  field = record["245"]
  if field
    # Straight 245$a is used -- or if no $a, then first 245$k. 
    base_title        = field['a'] || field['k']    
    accumulator << base_title if base_title

    # Also, add that base_title with non-filing chars skipped
    filing_base_title = nil
    non_filing_count  = field.indicator2.to_i
    if base_title && non_filing_count > 0
      filing_base_title = base_title.slice(non_filing_count, base_title.length)
      accumulator << filing_base_title
    end

    # Also, add both base and non-filing base, with subtitle added on.
    subtitle = field['b']
    if subtitle
      accumulator << [base_title, subtitle].join(" ") if base_title
      accumulator << [filing_base_title, subtitle].join(" ") if filing_base_title
    end
  end
  accumulator.uniq!
end

to_field "title_display",       extract_marc("245abk", :trim_punctuation => true, :first => true)
to_field "title_sort",          marc_sortable_title

to_field "title_series_t",      extract_marc("440a:490a:800abcdt:400abcd:810abcdt:410abcd:811acdeft:411acdef:830adfgklmnoprst:760ast:762ast")
to_field "series_facet",        marc_series_facet

to_field "author_unstem",       extract_marc("100abcdgqu:110abcdgnu:111acdegjnqu")
to_field "author2_unstem",      extract_marc("700abcdegqu:710abcdegnu:711acdegjnqu:720a:505r:245c:191abcdegqu")
to_field "author_display",      extract_marc("100abcdq:110:111")
to_field "author_sort",         marc_sortable_author


#to_field "author_facet",        extract_marc("100abcdq:110abcdgnu:111acdenqu:700abcdq:710abcdgnu:711acdenqu", :trim_punctuation => true)
# Split author into author and organization, per PIG decision march 2014. PROBLEMS, working on it. 
to_field "author_facet",        extract_marc("100abcdq:700abcdq", :trim_punctuation => true)
to_field "organization_facet",  extract_marc("110abcdgnu:111acdenqu:710abcdgnu:711acdenqu", :trim_punctuation => true)


to_field "subject_t",           extract_marc("600:610:611:630:650:651avxyz:653aa:654abcvyz:655abcvxyz:690abcdxyz:691abxyz:692abxyz:693abxyz:656akvxyz:657avxyz:652axyz:658abcd")
to_field "subject_topic_facet"  do |record, accumulator|
  # some where we need to seperate subfields, some where we need to keep them together
  accumulator.concat  MarcExtractor.cached("610x:611x:630x:648a:648x:650x:651a:651x:691a:691x:690a:690x", :separator => nil).extract(record)
  accumulator.concat  MarcExtractor.cached("600abcdtq:610abt:611abt:630aa:650aa:653aa:654ab:656aa").extract(record)

  # trim
  accumulator.collect! {|v| Traject::Macros::Marc21.trim_punctuation v}

  #upcase first letter if needed, in MeSH sometimes inconsistently downcased
  accumulator.collect! do |value|
    value.gsub(/\A[a-z]/) do |m|
      m.upcase
    end
  end

  # No default wanted by PI. 
  #accumulator << "Unspecified" if accumulator.empty?
end

to_field "subject_geo_facet",   marc_geo_facet
to_field "subject_era_facet",   marc_era_facet

# not doing this at present, this wouldn't be quite right, need custom
# logic for where to insert '--', not just between any subfield. 
#to_field "subject_facet",     extract_marc("600:610:611:630:650:651:655:690", :seperator => "--")

to_field "published_display", extract_marc("260a", :trim_punctuation => true)
to_field "pub_date",          marc_publication_date

# LCC to broad class, start with built-in from marc record, but then do our own for local
# call numbers.
#
# Discpline facet requested ELIMINATED by PIG, March 2014. 
# lcc_map             = Traject::TranslationMap.new("lcc_top_level")
# to_field "discipline_facet",  marc_lcc_to_broad_category(:default => nil) do |record, accumulator|
#   # add in our local call numbers
#   accumulator.concat(
#     Traject::MarcExtractor.cached("991:937").collect_matching_lines(record) do |field, spec, extractor|
#         # we output call type 'processor' in subfield 'f' of our holdings
#         # fields, that sort of maybe tells us if it's an LCC field.
#         # When the data is right, which it often isn't.
#       call_type = field['f']
#       if call_type == "sudoc"
#         # we choose to call it:
#         "Government Publication"
#       elsif call_type.nil? || call_type == "lc" || field['a'] =~ Traject::Macros::Marc21Semantics::LCC_REGEX
#         # run it through the map
#         s = field['a']
#         s = s.slice(0, 1) if s
#         lcc_map[s]
#       else
#         nil
#       end
#     end.compact
#   )
#
#   # If it's got an 086, we'll put it in "Government Publication", to be
#   # consistent with when we do that from a local SuDoc call #.
#   if Traject::MarcExtractor.cached("086a", :separator =>nil).extract(record).length > 0
#     accumulator << "Government Publication"
#   end
#
#   accumulator.uniq!
#
#   if accumulator.empty?
#     accumulator << "Unknown"
#   end
# end

# Extract our local call numbers from holdings
# JHU call number searchable. Horizon is exporting call numbers in 991$a, with
# the "copy" portion in 991$c. We won't include copy numbers right now, as we
# don't have an auto-left-anchored search yet. This is a 'naive' first try
# at call number search.
to_field "local_call_number_t" do |record, accumulator|
  MarcExtractor.cached("991:937").each_matching_line(record) do |field|
    # Subfield q being ascii '1' means that this item is
    # marked staff only, do not include it's call numbers.
    # Note we still may unhappily be including some items which,
    # while not themselves staff-only, are attached to copies that
    # are.
    if field['q'] != '1'
      call_num = field['a'] # we keep call number in 'a'
      accumulator << call_num if call_num
    end
  end
  accumulator.uniq!
end

# Override "violencello" with colloqial "cello"
instrumentation_map_hash = Traject::TranslationMap.new("marc_instruments").merge("sc" => "Cello").to_hash
to_field "instrumentation_facet",       marc_instrumentation_humanized("048ab", :translation_map => instrumentation_map_hash)
to_field "instrumentation_code_unstem", marc_instrument_codes_normalized

to_field "issn",                extract_marc("022a:022l:022y:773x:774x:776x", :separator => nil)
to_field "issn_related",        extract_marc("490x:440x:800x:400x:410x:411x:810x:811x:830x:700x:710x:711x:730x:780x:785x:777x:543x:760x:762x:765x:767x:770x:772x:775x:786x:787x", :separator => nil)

to_field "oclcnum_t",           oclcnum

# add hathi to traject
to_field "hathi_url" do |record, accumulator|
  accumulator << record['url']
  accumulator.uniq!
end

to_field "hathi_access" do |record, accumulator|
  accumulator << record['access']
  accumulator.uniq!
end

to_field "other_number_unstem", extract_marc("024a:028a")

to_field "location_facet" do |record, accumulator|
  # downcase the collection/location codes we get from Horizon,
  # horizon seems to like to be case insensitive and mix case.
  location_codes   = MarcExtractor.cached("991m:937m", :separator => nil).extract(record).collect {|c| c.downcase}
  collection_codes = MarcExtractor.cached("991l:937l", :separator => nil).extract(record).collect {|c| c.downcase}

  accumulator.concat Traject::TranslationMap.new("jh_locations").translate_array(location_codes)
  accumulator.concat Traject::TranslationMap.new("jh_collections").translate_array(collection_codes)

  # Map to empty string to mean 'no facet posting', make it so. 
  accumulator.delete_if {|a| a.nil? || a.empty?}

  accumulator.uniq!

  # PI wants no 'Unknown' https://wiki.library.jhu.edu/display/HILT/March+4+2014+Agenda
  #accumulator << "Unknown" if accumulator.empty?
end

# Okay, anything that's been classified as format "Online", we want to index as in
# EVERY location facet, per request of PIG March 2014.
all_location_values = (Traject::TranslationMap.new("jh_locations").to_hash.values.flatten + 
  Traject::TranslationMap.new("jh_collections").to_hash.values.flatten).uniq
all_location_values.delete_if {|a| a.nil? || a.empty?}
each_record do |record, context|
  if (context.output_hash["format"] || []).include? "Online"
      context.output_hash["location_facet"] ||= []
      context.output_hash["location_facet"].concat all_location_values
      context.output_hash["location_facet"].uniq!
  end
end
