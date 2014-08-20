# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # This is a sample controller
# # - index is the default action of any application
# # - user is required for authentication and authorization
# # - download is for downloading files uploaded in the db (does streaming)
# # - call exposes all registered services (none by default)
# ########################################################################



#
# def search_form(self, url):
# form = FORM('',
#
# INPUT(_name='keywords', _value=request.get_vars.keywords,
#                       _style='width:200px;',
#                       _id='keywords'),
#                 INPUT(_type='submit', _value=T('Search')),
#                 INPUT(_type='submit', _value=T('Clear'),
#                       _onclick="jQuery('#keywords').val('');"),
#                 _method="GET", _action=url)
#
#     return form


import pandas as pd, re


def test():
    fields = [Field('gsm_name'), Field('gse_name')]
    temp_db = DAL('sqlite:memory').define_table('mytable', *fields)._db
    temp_db.mytable.truncate()
    temp_db.mytable.bulk_insert([
        dict(gsm_name="aaa", gse_name="bbb"),
        dict(gsm_name="ccc", gse_name="ddd"),
    ])
    query = temp_db.mytable.id > 0
    return dict(grid=SQLFORM.grid(query))


def getSearchDf(words):
    sql = """
            SELECT gse_name, gpl_name, gsm_name, name, value
            from sample_attribute_search
            WHERE
              doc @@ to_tsquery('simple', '%s');"""

    wordsDf = pd.DataFrame()
    for word in words:
        sqlQuery = sql % word
        rows = db.executesql(sqlQuery, as_dict=True)
        df = pd.DataFrame.from_records(rows, columns=["gse_name gpl_name gsm_name name value".split()])
        df['word'] = word
        df.to_csv("%s.csv" % word)
        wordsDf = pd.concat([df, wordsDf])
    return wordsDf


def getQuery(keywords):
    p = re.compile('[\W_]+', re.UNICODE)
    keywords = set(keywords.split() if keywords else [])
    print "querying db"
    df = getSearchDf(keywords)
    query = Sample_Attribute.id < 0
    if not df.empty:
        print "unstacking %s records" % len(df.index)
        df['name'] = df['name'] \
            .astype(str) \
            .apply(lambda x: p.sub("_", x)) \
            .str.lower()
        unstacked = df.groupby('gsm_name').filter(lambda row: len(pd.Series.unique(row['word'])) == len(keywords)) \
            .drop_duplicates(subset=['gsm_name', 'name']) \
            .set_index(['gsm_name', 'gse_name', 'gpl_name', 'name'])[['value']] \
            .unstack() \
            .reset_index()
        unstacked.columns = list(unstacked.columns.get_level_values(0)[:3]) \
                            + list(unstacked.columns.get_level_values(1)[3:])
                            # + ["col%s"%i for i in range(len(unstacked.columns[3:]))]
        # unstacked = unstacked[list(set(unstacked.columns))]
        unstacked = unstacked.T.drop_duplicates().T
        colOrder = sorted(unstacked.columns.tolist(), key = lambda x: len(unstacked[x].dropna()), reverse=True)
        rowOrder = sorted(unstacked.T.columns.tolist(), key = lambda x: len(unstacked.T[x].dropna()), reverse=True)
        unstacked = unstacked.T[rowOrder].T[colOrder]
        unstacked = unstacked.fillna("")
        unstacked.to_csv('unstacked.csv')
        recs = unstacked.T.to_dict().values()

        print "inserting %s records" % len(recs)
        levels = unstacked.columns.get_level_values(1)
        # Fields to extract from the summary
        fields = [Field('gsm_name'), Field('gse_name')] \
                 + [Field('%s' % level) for level in levels]

        temp_db = DAL('sqlite:memory').define_table('mytable', *fields)._db
        temp_db.mytable.truncate()
        temp_db.mytable.bulk_insert(recs)
        query = temp_db.mytable.id > 0
        print "done"
    return query


QUERY = None


def searchable(sfields=None, keywords=""):
    return QUERY


def search():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    # response.flash = T("Welcome to web2py!")
    # return dict(message=T('Hello World'))

    keywords = request.get_vars.keywords
    QUERY = getQuery(keywords)

    dashboard = SQLFORM.grid(query=QUERY,
                             # deletable=False,
                             # editable=False,
                             # create=False,
                             searchable=searchable,
                             # field_id = 'mytable.id'
    )
    return (dict(dashboard=dashboard))


def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    # response.flash = T("Welcome to web2py!")
    # return dict(message=T('Hello World'))

    grid = SQLFORM.grid(Sample_Attribute.id < 0)
    return dict(grid=grid)


@request.restful()
def api():
    response.view = 'generic.' + request.extension

    def GET(*args, **vars):
        patterns = 'auto'
        parser = db.parse_as_rest(patterns, args, vars)
        if parser.status == 200:
            return dict(content=parser.response)
        else:
            raise HTTP(parser.status, parser.error)

    def POST(table_name, **vars):
        return db[table_name].validate_and_insert(**vars)

    return locals()


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
