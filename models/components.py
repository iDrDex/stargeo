__author__ = 'dex'
response.generic_patterns = ['.html']
response.files += ["https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"]
response.files += ["//maxcdn.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.min.css"]
response.files += [URL('static', 'css/my.css')]


def search_widget(fields=None, url=None):
    search_form = \
        FORM(  # formname='search_form',
               DIV(  # _class='row'
                     DIV(  # _class="col-lg-6",
                           DIV(  # _class="input-group input-group-lg"
                                 SPAN(INPUT(_name="invariant",
                                            _type='checkbox',
                                            _checked=request.vars.invariant,
                                            _disabled=True if request.controller == "series_tag" \
                                                              or request.controller == 'sample_tag' \
                                                              or request.controller == 'default' \
                                                else False,
                                            _onclick="""
                                                     $('#search_form').submit();
                                                     """,
                                            _data_toggle="tooltip",
                                            _title="Toggle invariant fields"),
                                      _class="input-group-addon"),
                                 INPUT(_class="form-control",
                                       _name="keywords",
                                       _type="text",
                                       _id="keywords",
                                       _value=request.vars.keywords or "",
                                       _placeholder="Search...", ),
                                 SPAN(BUTTON(I(_class="fa fa-search"),
                                             _class="btn btn-primary",
                                             _type="submit",
                                             _data_toggle="tooltip",
                                             _title="Go!"),
                                      A(I(_class="fa fa-times-circle"),
                                        _class="btn btn-default",
                                        _href=URL(), ),
                                      _class="input-group-btn",
                                      _data_toggle="tooltip",
                                      _title="Clear"),
                                 _class="input-group input-group-lg"),
                           _class="col-lg-10 col-lg-offset-1", ),
                     _class="row"
               ),
               formname='search_form',
               _id='search_form',  # needed for invariant onclick
               _method="GET",
               _action=url or URL(),
        )

    return search_form


def get_fts_query(search_text):
    import re

    search_text = search_text \
        .strip() \
        .replace("&", " ") \
        .lower() \
        if search_text \
        else ""

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
    view = db["%s_view" % request.controller]
    view_id = "%s_view_id" % request.controller
    if not fts_query:
        query = view.id > 0
    else:
        search = db(Search.fts_query == fts_query).select().first() \
                 or Search(Search.insert(fts_query=fts_query))
        results = db["%s_view_results" % request.controller]
        query = (results.search_id == search.id) & (results[view_id] == view.id)
        if db(query).isempty():
            sql = """
                INSERT INTO %s_view_results
                SELECT
                NEXTVAL('%s_view_results_id_seq'),
                id,
                %s
                FROM %s_view
                WHERE %s_view.doc @@ to_tsquery('english', '%s');
            """ % (request.controller, request.controller, search.id, request.controller, request.controller, fts_query)


            # select_sql  = """NEXTVAL('%s_view_results_id_seq'),
            # id,
            # %s
            # INTO %s_view_results"""%\
            # (request.controller, search.id,request.controller)
            # from_sql  = "%s_view" % request.controller
            # where_sql  = "%s_view.doc @@ to_tsquery('english', '%s')" % (request.controller, fts_query)
            # sql = """SELECT %s
            # FROM %s
            # WHERE %s;"""%(select_sql, from_sql, where_sql)
            print sql
            # 1 / 0
            db.executesql(sql)
        User_Search.insert(keywords=keywords,
                           fts_query=fts_query)
        db.commit()
    return query


def get_fields(view, query, paginate):
    index_field_names = ['series_id', 'platform_id', 'sample_id']
    index_fields = [view[name] for name in index_field_names if name in view.fields]
    remove_field_names = ['series_geo_accession', 'sample_geo_accession']
    exclude_field_names = set(index_field_names + remove_field_names)
    variant_fields = get_variant_fields(query, paginate, view)
    filtered_variant_fields = [field for field in variant_fields
                               if field.name not in exclude_field_names]
    fields = index_fields + filtered_variant_fields
    return fields


def get_tag_headers(view, query):
    """Given a query returns all the tags annotated for that field"""
    tagQuery = query \
               & (view['series_id'] == Series_Tag.series_id) \
               & (view['platform_id'] == Series_Tag.platform_id)
    rows = db(tagQuery).select(distinct=Series_Tag.tag_id)
    tag_fields = [view[row.series_tag.tag_id.tag_name] for row in rows]

    index_field_names = ['series_id', 'platform_id', 'sample_id']
    index_fields = [view[name] for name in index_field_names if name in view.fields]
    fields = index_fields + tag_fields
    return fields


paginate = 10


def get_grid():
    import time

    view = db["%s_view" % request.controller]
    session.start_time = time.time()  # sets the counter
    query = searchable(fields=view, keywords=request.vars.keywords)
    fields = None
    if request.controller.endswith("_tag"):
        fields = get_tag_headers(view, query)
    elif not request.vars.invariant:
        fields = get_fields(view, query, paginate)

    grid = SQLFORM.grid(query,
                        field_id=view.id,
                        fields=fields,
                        search_widget=None,
                        searchable=searchable,
                        paginate=paginate,
                        maxtextlength=50,
                        create=False,
                        editable=False,
                        deletable=False,
                        user_signature=None,
                        buttons_placement='left',
                        links=[lambda row: get_tags(row),
                               lambda row: get_tag_button(row)])
    response.flash = T("That took about %.2f seconds!" % (time.time() - session.start_time))
    return grid


# import pandas as pd
#
#
# def get_nunique(query, view, paginate):
# limitby = None
# if paginate:
# try:
# page = int(request.vars.page or 1) - 1
# except ValueError:
# page = 0
#         limitby = (paginate * page, paginate * (page + 1))
#
#     rows = db(query).select(view.ALL, limitby=limitby)
#     columns = [re.sub(r".+_view.", "", col) for col in rows.colnames]
#     df = pd.DataFrame(rows.as_list())
#     df.to_csv('df.csv')
#     nunique = pd.DataFrame(rows.as_list()) \
#         [columns] \
#         .set_index('id') \
#         .fillna("") \
#         .count()
#     return nunique


def get_variant_fields(query, paginate, view):
    limitby = None
    if paginate:
        try:
            page = int(request.vars.page or 1) - 1
        except ValueError:
            page = 0
        limitby = (paginate * page, paginate * (page + 1))

    field2set = {}
    rows = db(query).select(view.ALL, limitby=limitby).as_list()
    for row in rows:
        for field in row:
            if field not in field2set.keys():
                field2set[field] = set()
            field2set[field].add(row[field])

    variant_fields = []
    min_count = 1 if len(rows) > 1 else 0

    if field2set:
        variant_fields = [view[field] for field in sorted(field2set.keys()) if len(field2set[field]) > min_count]
    return variant_fields


def get_series_id(row):
    controller = request.controller
    view = "%s_view" % controller

    series_id = \
        'series_id' in row and row.series_id or \
        'sample_id' in row and row.sample_id.series_id or \
        view in row and 'series_id' in row[view] and row[view].series_id or \
        view in row and 'sample_id' in row[view] and row[view].sample_id or \
        redirect('default', 'index')  #must have series_id

    return series_id


def get_platform_id(row):
    controller = request.controller
    view = "%s_view" % controller

    platform_id = \
        'platform_id' in row and row.platform_id or \
        'sample_id' in row and row.sample_id.platform_id or \
        view in row and 'platform_id' in row[view] and row[view].platform_id or \
        view in row and 'sample_id' in row[view] and row[view].sample_id or \
        None  #may not have platform_id for series views

    return platform_id


def get_tags(row):
    series_id = get_series_id(row)
    # series_id = row.series_id if 'series_id' in row else row.sample_id.series_id
    query = (Series_Tag.series_id == series_id) & (Series_Tag.tag_id == Tag.id)
    platform_id = get_platform_id(row)
    if platform_id:
        query &= (Series_Tag.platform_id == platform_id)

    tags = [row.tag_name
            if '%s_view' % request.controller in row
            else row.tag_name
            for row in db(query).select(Tag.tag_name,
                                        distinct=Series_Tag.tag_id)]
    return DIV([DIV(tag, _class="badge") for tag in tags])


def get_tag_button(row):
    series_id = get_series_id(row)
    if 'page' in request.get_vars:
        del (request.get_vars.page)
    return FORM(BUTTON(I(_class="fa fa-tag"),
                       'Tag',
                       _class="button btn btn-default",
                       _type="submit"),
                hidden=dict(series_id=series_id),
                _action=URL("tag", "index", vars=request.get_vars),
                _data_toggle="tooltip",
                _title="Tag this GSE")

