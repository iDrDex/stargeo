# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # This is a sample controller
# # - index is the default action of any application
# # - user is required for authentication and authorization
# # - download is for downloading files uploaded in the db (does streaming)
# # - call exposes all registered services (none by default)
# ########################################################################



import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """

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

    form = search_form(None, None)
    # form.attributes['_method'] = "POST" #Hack needed to make it form.process.accept work

    if 'keywords' in request.vars:
        redirect(URL('series', 'index',
                     vars=dict(keywords=request.vars.keywords)))


    return dict(form=form)


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
