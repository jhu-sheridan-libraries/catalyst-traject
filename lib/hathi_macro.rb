module HathiMacro
  Marc21 = Traject::Macros::Marc21
  OCLC_CLEAN = /^\(OCoLC\)[^0-9A-Za-z]*([0-9A-Za-z]*)[^0-9A-Za-z]*$/
  @@conn = ""
  @@isConnected = false

  def hathi_access
    lambda do |record, accumulator, _context|

      local_id = Marc21.extract_marc_from(record, '001', first: true).first

      # - lookup access level in database table
      hathi_access = lookup_hathi(local_id, 'access')
      # - add to Solr record if present - skip nils/empty-strings
      accumulator << hathi_access if hathi_access

    end
  end

  def hathi_url
    lambda do |record, accumulator, _context|

      local_id = Marc21.extract_marc_from(record, '001', first: true).first

      # - lookup access level in database table
      hathi_url = lookup_hathi(local_id, 'url')
      # - add to Solr record if present - skip nils/empty-strings
      accumulator << hathi_url if hathi_url

    end
  end
  def open_connection!
    #logger.debug("HorizonReader: Opening JDBC Connection at #{jdbc_url(false)} ...")

    conn =  java.sql.DriverManager.getConnection( jdbc_url(true) )
    # If autocommit on, fetchSize later has no effect, and JDBC slurps
    # the whole result set into memory, which we can not handle.
    conn.setAutoCommit false
    @@isConnected = true
    logger.debug("HorizonReader: Opened JDBC Connection.")
    return conn
  end

  # Looks up JDBC url from settings, either 'horizon.jdbc_url' if present,
  # or individual settings. Will include password from `horizon.password`
  # only if given a `true` arg -- leave false for output to logs, to keep
  # password out of logs.
  def jdbc_url(include_password=false)
    url = if settings.has_key? "horizon.jdbc_url"
      settings["horizon.jdbc_url"]
    else
      jtds_type = settings['horizon.jtds_type'] || 'sybase'
      database  = settings['horizon.database']  || 'horizon'
      host      = settings['horizon.host']      or raise ArgumentError.new("Need horizon.host setting, or horizon.jdbc_url")
      port      = settings['horizon.port']      || '2025'
      user      = settings['horizon.user']      or raise ArgumentError.new("Need horizon.user setting, or horizon.jdbc_url")

      "jdbc:jtds:#{jtds_type}://#{host}:#{port}/#{database};user=#{user}"
    end
    # Not sure if useCursors makes a difference, but just in case.
    url += ";useCursors=true"

    if timeout = settings['horizon.timeout']
      url += ";socketTimeout=#{timeout};loginTimeout=#{timeout}"
    end
    if include_password
      password  = settings['horizon.password'] or raise ArgumentError.new("Need horizon.password setting")
      url += ";password=#{password}"
    end
    return url
  end


  def lookup_hathi(local_id, type)
    begin
      if @@isConnected == false
        conn = open_connection!
        @@conn = conn
      end
      local_id = local_id.to_s
      sql = "select * from jhu_hathi_exception where bib# = #{local_id}"
      stmt = @@con.createStatement()
      rs = stmt.executeQuery(sql)
      # Search all returned records for highest level of access (allow)
      # If not found, return whatever else we got
      hathi_value = 'none'
      while (rs.next)
        #logger.info(rs.getString(type))
        hathi_value = rs.getString(type)
      end
    #ensure
    #  conn.close
    end
    return hathi_value
  end
end
