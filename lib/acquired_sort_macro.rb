module AcquiredSortMacro
  Marc21 = Traject::Macros::Marc21
  @@test = "init"

  def acquired_date
    lambda do |record, accumulator, _context|

      local_id = Marc21.extract_marc_from(record, '001', first: true).first

      # - lookup acquired date in database table
      date = lookup_acquired_date(local_id)
      # - add to Solr record if present - skip nils/empty-strings
      accumulator << date if date

    end
  end

  def open_connection!
    #logger.debug("HorizonReader: Opening JDBC Connection at #{jdbc_url(false)} ...")

    conn =  java.sql.DriverManager.getConnection( jdbc_url(true) )
    # If autocommit on, fetchSize later has no effect, and JDBC slurps
    # the whole result set into memory, which we can not handle.
    conn.setAutoCommit false
    #logger.debug("HorizonReader: Opened JDBC Connection.")
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


  def lookup_acquired_date(local_id)
    begin
      if @@test == "init"
        conn = open_connection!
        stmt = conn.createStatement()
        @@test = conn
      else
        stmt = @@test.createStatement()
      end
      local_id = local_id.to_s
      sql = "select top 1 convert(CHAR(20), dateadd(DAY, creation_date, '1970-01-01'), 23) AS acquired_date from item where bib# = #{local_id} order by creation_date desc"
      rs = stmt.executeQuery(sql)
      date = nil
      while (rs.next)
        date = rs.getString('acquired_date').rstrip+'Z'
      end
      #ensure
      #  conn.close
    end
    return date
  end
end
