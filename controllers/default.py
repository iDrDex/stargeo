# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # This is a sample controller
# # - index is the default action of any application
# # - user is required for authentication and authorization
# # - download is for downloading files uploaded in the db (does streaming)
# # - call exposes all registered services (none by default)
# ########################################################################



def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    session.platform_count = session.platform_count or db(Platform).count()
    session.tag_count = session.tag_count or db(Tag).count()

    session.sample_count = session.sample_count or db(Sample).count()
    session.sample_attribute_count = session.sample_attribute_count or db(Sample_Attribute).count()
    session.sample_tag_count = session.sample_tag_count or db(Sample_Tag).count()
    import locale

    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    session.series_count = session.series_count or db(Series).count()
    session.series_attribute_count = session.series_attribute_count or db(Series_Attribute).count()
    session.series_tag_count = session.series_tag_count or db(Series_Tag).count()
    stats = UL(LI(A(locale.format_string("%d tags", session.tag_count, grouping=True))),
               LI(A(locale.format_string("%d samples", session.sample_count, grouping=True))),
               LI(A(locale.format_string("%d attributes", session.sample_attribute_count, grouping=True),
                    _style="""padding-left: 1em""")),
               LI(A(locale.format_string("%d tag annotations", session.sample_tag_count, grouping=True),
                    _style="""padding-left: 1em""")),
               LI(A(locale.format_string("%d series", session.series_count, grouping=True))),
               LI(A(locale.format_string("%d attributes", session.series_attribute_count, grouping=True),
                    _style="""padding-left: 1em""")),
               LI(A(locale.format_string("%d tag annotations", session.series_tag_count, grouping=True),
                  _style="""padding-left: 1em""")),
               LI(A(locale.format_string("%d platforms", session.platform_count, grouping=True))),
                _class = "nav"
            )

    fields = Tag.id, Sample_Tag.id.count(), Series_Tag.id.count()
    query = Series_Tag
    tags = SQLFORM.grid(query, searchable=False, csv=False, orderby=~Series_Tag.id, paginate=10)
    tags.element('.web2py_counter', replace=None)

    response.files += ["https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css",
    "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css",
    "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"]

    return dict(stats=stats,
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
