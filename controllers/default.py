# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # This is a sample controller
# # - index is the default action of any application
# # - user is required for authentication and authorization
# # - download is for downloading files uploaded in the db (does streaming)
# # - call exposes all registered services (none by default)
# ########################################################################

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


def setSampleFilter(fts_query):
    Sample_Filter.truncate()
    sqlQuery = """
        INSERT INTO sample_filter
        SELECT id, sample_id
        FROM sample_view_fts
        JOIN sample_view using (id)
        WHERE to_tsquery('english', '%s') @@ doc
    """ % fts_query
    # print sqlQuery
    start_time = time.time()
    db.executesql(sqlQuery)
    print "\t--- %s seconds --- to FTS sample_view" % (time.time() - start_time)
    db.commit()

def getSampleFieldsSerially(fts_query):
    if not session.all_field_names:
        session.all_field_names = sorted([row.attribute_name
                                          for row in db().select(Sample_Attribute.attribute_name, distinct=True)])

    fts_query = "|" \
        .join(set(fts_query \
                  .replace("&", " ") \
                  .replace("|", " ") \
                  .split()))

    sql = """
        SELECT *
        from sample_attribute
        join sample_filter using (sample_id)
        WHERE
          attribute_name = '%s'
          AND
          to_tsquery('english', '%s') @@
          (to_tsvector('english', attribute_name) ||
          to_tsvector('english', attribute_value))
        LIMIT 1;"""
    start_time = time.time()
    # print sql % (session.all_field_names[0], fts_query)
    field_names = [field_name for field_name in session.all_field_names
                   if db.executesql(sql % (field_name, fts_query))]

    print "\t--- %s seconds --- to filter FTS sample_attribute" % (time.time() - start_time)
    return field_names


def getSampleFields(fts_query):
    fts_query = "|" \
        .join(set(fts_query \
                  .replace("&", " ") \
                  .replace("|", " ") \
                  .split()))

    filename = "%s.fields.txt"%fts_query
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
        ORDER BY attribute_name;""" % fts_query
        field_names = [row[0] for row in db.executesql(sqlQuery)]
        open(filename, "w").write("\n".join(field_names))
    field_names = open(filename).read().split()
    print "\t--- %s seconds --- to FTS sample_attribute" % (time.time() - start_time)
    return field_names


def searchable(sfields=None, fts_query=""):
    if not fts_query:
        return Sample_View.id > 0
    query = Sample_View.id == Sample_Filter.id
    # query = Sample_View.id.belongs(session.sample_ids)
    return query

def get_fts_query(search_text):
    fts_query = re.sub(r'\s+',
                       r' ',
                       search_text)

    fts_query = re.sub(r'\s?\|\s? ',
                       r'|',
                       fts_query)

    fts_query = re.sub(r' ',
                       r'&',
                       fts_query)
    return fts_query

def search():
    print "***", request.vars.keywords, "***"
    start_time = time.time()
    fts_query = get_fts_query(request.vars.keywords)
    flash = False
    if fts_query <> session.fts_query:
        setSampleFilter(fts_query)
        session.fts_query = fts_query
        session.field_names = None
        flash = True

    query = searchable(fts_query=fts_query)

    fields = None
    if request.vars.filter:
        session.field_names = session.field_names or getSampleFields(fts_query)
        fields = [Sample_View[field_name] for field_name in session.field_names]
    # fields = sorted(set(Sample_View[field]
    # for field_list in [row.attribute_names
    # for row in db().select(Sample_Filter.attribute_names, distinct=True)]
    # for field in field_list))    fields = None
    dashboard = SQLFORM.grid(query=query,
                             # deletable=False,
                             # editable=False,
                             # create=False,
                             search_widget=search_form,
                             searchable=searchable,
                             fields=fields,
                             buttons_placement='left')
    if flash:
        response.flash = T("Found %s samples in %.2f seconds" % (db(query).count(), time.time() - start_time))

    return dict(dashboard=dashboard)
    # return response.render(d)


def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    query = Sample_View
    Sample_View.series_id.requires = \
        Sample_View.platform_id.requires = \
        Sample_View.sample_id.requires = None  # Speed
    return dict(grid=SQLFORM.grid(query,
                                  editable=False,
                                  deletable=False))


def user():
    """
    exposes:
    http://..../[app]/default/user/login
    http://..../[app]/default/user/logout
    http://..../[app]/default/user/register
    http://..../[app]/default/user/profile
    http://..../[app]/default/user/retrieve_password
    http://..../[app]/default/user/change_password
    http://..../[app]/default/user/manage_users (requires membership in
    use @auth.requires_login()
        @auth.requires_membership('group name')
        @auth.requires_permission('read','table name',record_id)
    to decorate functions that need access control
    """
    return dict(form=auth())


@cache.action()
def download():
    """
    allows downloading of uploaded files
    http://..../[app]/default/download/[filename]
    """
    return response.download(request, db)


def call():
    """
    exposes services. for example:
    http://..../[app]/default/call/jsonrpc
    decorate with @services.jsonrpc the functions to expose
    supports xml, json, xmlrpc, jsonrpc, amfrpc, rss, csv
    """
    return service()


@auth.requires_signature()
def data():
    """
    http://..../[app]/default/data/tables
    http://..../[app]/default/data/create/[table]
    http://..../[app]/default/data/read/[table]/[id]
    http://..../[app]/default/data/update/[table]/[id]
    http://..../[app]/default/data/delete/[table]/[id]
    http://..../[app]/default/data/select/[table]
    http://..../[app]/default/data/search/[table]
    but URLs must be signed, i.e. linked with
      A('table',_href=URL('data/tables',user_signature=True))
    or with the signed load operator
      LOAD('default','data.load',args='tables',ajax=True,user_signature=True)
    """
    return dict(form=crud())
