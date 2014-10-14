# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # This is a sample controller
## - index is the default action of any application
## - user is required for authentication and authorization
## - download is for downloading files uploaded in the db (does streaming)
## - api is an example of Hypermedia API support and access control
#########################################################################

import locale

def index():
    #cache counts
    session.platform_count = session.platform_count or locale.format("%d", db(Platform).count(), grouping=True)
    session.tag_count = session.tag_count or locale.format("%d", db(Tag).count(), grouping=True)
    session.sample_count = session.sample_count or locale.format("%d", db(Sample).count(), grouping=True)
    session.sample_attribute_count = session.sample_attribute_count or locale.format("%d", db(Sample_Attribute).count(),
                                                                                     grouping=True)
    session.sample_tag_count = session.sample_tag_count or locale.format("%d", db(Sample_Tag).count(), grouping=True)
    session.series_count = session.series_count or locale.format("%d", db(Series).count(), grouping=True)
    session.series_attribute_count = session.series_attribute_count or locale.format("%d", db(Series_Attribute).count(),
                                                                                     grouping=True)
    session.series_tag_count = session.series_tag_count or locale.format("%d", db(Series_Tag).count(), grouping=True)

    Series_Tag.tag_id.represent = lambda name, row: DIV(row.tag_id.tag_name, _class = "badge")
    tags = SQLFORM.grid(Series_Tag,
                        searchable=False,
                        csv=False,
                        orderby=~Series_Tag.id,
                        paginate=7,
                        maxtextlength=100,
                        ondelete=update_sample_cross_tab)

    search_form = search_widget(url=URL('series', 'index', vars=dict(keywords=request.vars.keywords)))

    return dict(search_form=search_form,
                tags=tags)


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


@auth.requires_login()
def api():
    """
    this is example of API with access control
    WEB2PY provides Hypermedia API (Collection+JSON) Experimental
    """
    from gluon.contrib.hypermedia import Collection

    rules = {
        '<tablename>': {'GET': {}, 'POST': {}, 'PUT': {}, 'DELETE': {}},
    }
    return Collection(db).process(request, response, rules)
