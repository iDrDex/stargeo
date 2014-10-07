__author__ = 'dex'
response.generic_patterns = ['.html']
response.files += ["https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"]
response.files += ["//maxcdn.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.min.css"]


def search_widget(fields=None, url=None):
    search_form = \
        FORM(  # formname='search_form',
               DIV(  # _class='row'
                     DIV(  # _class="col-lg-6",
                           DIV(  # _class="input-group input-group-lg"
                                 SPAN(INPUT(_name="invariant",
                                            _type='checkbox',
                                            _checked=request.vars.invariant,
                                            _onclick="""
                                                $('#search_form').submit();
                                            """),
                                      _class="input-group-addon"),
                                 INPUT(_class="form-control",
                                       _name="keywords",
                                       _type="text",
                                       _id="keywords",
                                       _value=request.vars.keywords or "",
                                       _placeholder="Search...", ),
                                 SPAN(BUTTON(I(_class="fa fa-search"),
                                             _class="btn btn-default",
                                             _type="submit"),
                                      A(I(_class="fa fa-times-circle"),
                                        _class="btn btn-default",
                                        _href=URL(), ),
                                      _class="input-group-btn"),
                                 _class="input-group input-group-lg"),
                           _class="col-lg-10 col-lg-offset-1", ),
                     _class="row"
               ),
               formname='search_form',
               _id='search_form',  # needed for invariant onclickw
               _method="GET",
               _action=url or URL(),
        )

    return search_form


def get_fts_query(search_text):
    import re

    search_text = search_text or ""
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


def searchable(fields, keywords):
    fts_query = get_fts_query(keywords)
    query = "%s_view.doc @@ to_tsquery('english', '%s')" % (request.controller, fts_query) \
        if keywords else db["%s_view" % request.controller]
    return query


paginate = 20


def get_grid():
    import time

    session.start_time = time.time()  # sets the counter

    query = searchable(fields=None, keywords=request.vars.keywords),

    fields = get_variant_fields(query=query,
                                paginate=paginate,
                                view=db["%s_view" % request.controller]) \
        if not request.vars.invariant else None

    grid = SQLFORM.grid(query,
                        fields=fields,
                        search_widget=None,
                        searchable=searchable,
                        paginate=paginate,
                        create=False,
                        editable=False,
                        deletable=False,
                        user_signature=None,
                        buttons_placement='left',
                        links=[lambda row: get_tags(row),
                               lambda row: get_tag_button(row)])

    response.flash = T("That took about %.2f seconds!" % (time.time() - session.start_time))


    return grid


def get_variant_fields(query, paginate, view):
    import pandas as pd

    limitby = None
    if paginate:
        try:
            page = int(request.vars.page or 1) - 1
        except ValueError:
            page = 0
        limitby = (paginate * page, paginate * (page + 1))

    rows = db(query).select(limitby=limitby)
    columns = [re.sub(r".+_view.", "", col) for col in rows.colnames]
    df = pd.DataFrame(rows.as_list()) \
        [columns] \
        .set_index('id')  # id column
    df.to_csv('df.csv')
    variant_fields = [view[field] for field in df if df[field].nunique() > 1]

    return variant_fields


def get_tags(row):
    series_id = row.series_id if 'series_id' in row else row.sample_id.series_id
    tags = [row.tag_name
            if '%s_view' % request.controller in row
            else row.tag_name
            for row in db((Series_Tag.series_id == series_id) &
                          # (Series_Tag.platform_id == row.platform_id) &
                          (Series_Tag.tag_id == Tag.id)).select(Tag.tag_name,
                                                                distinct=Series_Tag.tag_id)]
    return DIV([DIV(tag, _class="badge") for tag in tags])


def get_tag_button(row):
    vars = None
    if 'page' in request.get_vars:
        del (request.get_vars.page)
    return FORM(BUTTON('Tag'),
                hidden={'series_id': row.series_id if 'series_id' in row else row.sample_id.series_id},
                _action=URL("tag", "index", vars=request.get_vars))

