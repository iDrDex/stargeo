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


def setFilter(sample_fts_query):
    Sample_Filter.truncate()
    sqlQuery = """
        INSERT INTO sample_filter
        SELECT id
        FROM sample_view_fts
        WHERE to_tsquery('english', '%s') @@ doc
    """ % sample_fts_query
    # print sqlQuery
    start_time = time.time()
    db.executesql(sqlQuery)
    print "\t--- %s seconds --- to FTS sample_view" % (time.time() - start_time)
    db.commit()


# def getSampleFieldsSerially(sample_fts_query):
# if not session.all_sample_field_names:
# session.all_sample_field_names = sorted([row.attribute_name
# for row in db().select(Sample_Attribute.attribute_name, distinct=True)])
#
#     sample_fts_query = "|" \
#         .join(set(sample_fts_query \
#                   .replace("&", " ") \
#                   .replace("|", " ") \
#                   .split()))
#
#     sql = """
#         SELECT *
#         from sample_attribute
#         join sample_filter using (sample_id)
#         WHERE
#           attribute_name = '%s'
#           AND
#           to_tsquery('english', '%s') @@
#           (to_tsvector('english', attribute_name) ||
#           to_tsvector('english', attribute_value))
#         LIMIT 1;"""
#     start_time = time.time()
#     # print sql % (session.all_sample_field_names[0], sample_fts_query)
#     sample_field_names = [field_name for field_name in session.all_sample_field_names
#                    if db.executesql(sql % (field_name, sample_fts_query))]
#
#     print "\t--- %s seconds --- to filter FTS sample_attribute" % (time.time() - start_time)
#     return sample_field_names


def getFields(sample_fts_query):
    sample_fts_query = "|" \
        .join(set(sample_fts_query \
                  .replace("&", " ") \
                  .replace("|", " ") \
                  .split()))

    filename = "%s.fields.txt" % sample_fts_query
    start_time = time.time()
    if not glob.glob(filename):
        sqlQuery = """
        SELECT distinct attribute_name
        from sample_attribute
        join sample_filter using (sample_id)
        WHERE
          to_tsquery('english', '%s') @@
          (to_tsvector('english', attribute_name) ||
          to_tsvector('english', attribute_value))
        ORDER BY attribute_name;""" % sample_fts_query
        sample_field_names = [row[0] for row in db.executesql(sqlQuery)]
        open(filename, "w").write("\n".join(sample_field_names))
    sample_field_names = open(filename).read().split()
    print "\t--- %s seconds --- to FTS sample_attribute" % (time.time() - start_time)
    return sample_field_names


def searchable(sfields=None, sample_fts_query=""):
    if not sample_fts_query:
        return Sample_View.id > 0
    return Sample_View.id == Sample_Filter.id


def get_sample_fts_query(search_text):
    search_text = search_text or ""
    sample_fts_query = re.sub(r'\s+',
                              r' ',
                              search_text)

    sample_fts_query = re.sub(r'\s?\|\s? ',
                              r'|',
                              sample_fts_query)

    sample_fts_query = re.sub(r' ',
                              r'&',
                              sample_fts_query)
    return sample_fts_query


def index():
    print "***", request.vars.keywords, "***"
    start_time = time.time()
    sample_fts_query = get_sample_fts_query(request.vars.keywords)
    flash = False
    if sample_fts_query <> session.sample_fts_query:
        setFilter(sample_fts_query)
        session.sample_fts_query = sample_fts_query
        session.sample_field_names = None
        flash = True

    query = searchable(sample_fts_query=sample_fts_query)

    fields = None
    if request.vars.filter:
        session.sample_field_names = session.sample_field_names or getFields(sample_fts_query)
        fields = [Sample_View[field_name] for field_name in session.sample_field_names]
    # fields = sorted(set(Sample_View[field]
    # for field_list in [row.attribute_names
    # for row in db().select(Sample_Filter.attribute_names, distinct=True)]
    # for field in field_list))    fields = None
    dashboard = SQLFORM.grid(query=query,
                             # deletable=False,
                             # editable=False,
                             # create=False,
                             field_id=Sample_View.id,
                             search_widget=search_form,
                             searchable=searchable,
                             fields=fields,
                             buttons_placement='left',
                             links=[lambda row: A(BUTTON('Tag'), _href=URL("tag", "index",
                                                                                # args=row.series_view.series_id,
                                                                                vars=dict(
                                                                                    series_id=row.sample_view.series_id
                                                                                    if 'sample_view' in row
                                                                                    else row.series_id
                                                                                )))]
    )
    if flash:
        response.flash = T("Found %s samples in %.2f seconds" % (db(query).count(), time.time() - start_time))

    return dict(dashboard=dashboard)
    # return response.render(d)

