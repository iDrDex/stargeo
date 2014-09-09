import time, glob, re


def search_form(self, url):
    form = FORM(INPUT(_name='keywords',
                      _value=request.get_vars.keywords,
                      # _style='width:200px;',
                      _id='keywords'),
                INPUT(_name='filter', _type='checkbox', _value="on", _checked=request.get_vars.filter),
                INPUT(_type='submit', _value=T('Search')),
                INPUT(_type='submit', _value=T('Clear'),
                      _onclick="jQuery('#keywords').val('');"),
                _method="GET", _action=url)
    return form


def setFilter(series_fts_query):
    Series_Filter.truncate()
    sqlQuery = """
        INSERT INTO series_filter
        SELECT id
        FROM series_view_fts
        WHERE to_tsquery('english', '%s') @@ doc
    """ % series_fts_query
    # print sqlQuery
    start_time = time.time()
    db.executesql(sqlQuery)
    print "\t--- %s seconds --- to FTS series_view" % (time.time() - start_time)
    db.commit()

# def getSeriesFieldsSerially(series_fts_query):
#     if not session.all_series_field_names:
#         session.all_series_field_names = sorted([row.attribute_name
#                                           for row in db().select(Series_Attribute.attribute_name, distinct=True)])
#
#     series_fts_query = "|" \
#         .join(set(series_fts_query \
#                   .replace("&", " ") \
#                   .replace("|", " ") \
#                   .split()))
#
#     sql = """
#         SELECT *
#         from series_attribute
#         join series_filter using (series_id)
#         WHERE
#           attribute_name = '%s'
#           AND
#           to_tsquery('english', '%s') @@
#           (to_tsvector('english', attribute_name) ||
#           to_tsvector('english', attribute_value))
#         LIMIT 1;"""
#     start_time = time.time()
#     # print sql % (session.all_series_field_names[0], series_fts_query)
#     series_field_names = [field_name for field_name in session.all_series_field_names
#                    if db.executesql(sql % (field_name, series_fts_query))]
#
#     print "\t--- %s seconds --- to filter FTS series_attribute" % (time.time() - start_time)
#     return series_field_names


def getFields(series_fts_query):
    series_fts_query = "|" \
        .join(set(series_fts_query \
                  .replace("&", " ") \
                  .replace("|", " ") \
                  .split()))

    filename = "%s.fields.txt"%series_fts_query
    start_time = time.time()
    if not glob.glob(filename):
        sqlQuery = """
        SELECT distinct attribute_name
        from series_attribute
        join series_filter using (series_id)
        WHERE
          to_tsquery('english', '%s') @@
          (to_tsvector('english', attribute_name) ||
          to_tsvector('english', attribute_value))
        ORDER BY attribute_name;""" % series_fts_query
        series_field_names = [row[0] for row in db.executesql(sqlQuery)]
        open(filename, "w").write("\n".join(series_field_names))
    series_field_names = open(filename).read().split()
    print "\t--- %s seconds --- to FTS series_attribute" % (time.time() - start_time)
    return series_field_names


def searchable(sfields=None, series_fts_query=""):
    if not series_fts_query:
        return Series_View.id > 0
    return Series_View.id == Series_Filter.id

def get_series_fts_query(search_text):
    search_text = search_text or ""
    series_fts_query = re.sub(r'\s+',
                       r' ',
                       search_text)

    series_fts_query = re.sub(r'\s?\|\s? ',
                       r'|',
                       series_fts_query)

    series_fts_query = re.sub(r' ',
                       r'&',
                       series_fts_query)
    return series_fts_query

def index():
    # return dict(grid=SQLFORM.grid(Series_View.id > 0, search_widget=None))
    # return dict(grid=SQLFORM.grid(Series_View,left=Series_Filter.on(Series_View.id == Series_Filter.id), search_widget=None))


    print "***", request.vars.keywords, "***"
    start_time = time.time()
    series_fts_query = get_series_fts_query(request.vars.keywords)
    flash = False
    if series_fts_query <> session.series_fts_query:
        setFilter(series_fts_query)
        session.series_fts_query = series_fts_query
        session.series_field_names = None
        flash = True

    query = searchable(series_fts_query=series_fts_query)

    fields = None
    if request.vars.filter:
        session.series_field_names = session.series_field_names or getFields(series_fts_query)
        fields = [Series_View[field_name] for field_name in session.series_field_names]
    # fields = sorted(set(Series_View[field]
    # for field_list in [row.attribute_names
    # for row in db().select(Series_Filter.attribute_names, distinct=True)]
    # for field in field_list))    fields = None
    dashboard = SQLFORM.grid(query=query,
                             # deletable=False,
                             # editable=False,
                             # create=False,
                             field_id=Series_View.id,
                             search_widget=search_form,
                             searchable=searchable,
                             fields=fields,
                             buttons_placement='left')
    if flash:
        response.flash = T("Found %s series in %.2f seconds" % (db(query).count(), time.time() - start_time))

    return dict(dashboard=dashboard)
    # return response.render(d)

